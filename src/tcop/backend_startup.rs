//! src/backend/tcop/backend_startup.c
//!
//! backend_startup.c
//!   Backend startup code
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/tcop/backend_startup.c

use crate::prelude::*;
use crate::pg_config_manual::NAMEDATALEN;

use std::ffi::{c_char, c_int, c_void};

use crate::c::{int32, uint32, Size};

// List handling (pg_list.h).
use crate::nodes::pg_list::{lappend, lfirst, linitial, list_free, list_length, List, NIL};
// foreach!/current_cell! are #[macro_export] macros living at the crate root.
use crate::{current_cell, foreach};
// StringInfo (stringinfo.h). appendStringInfo is kept as a local stub below
// because the translated call sites still use C printf-style format strings.
use crate::lib::stringinfo::{appendStringInfoString, initStringInfo, StringInfoData};
// Timestamp type (timestamp.h).
use crate::utils::init::globals::TimestampTz;
// Extra error-reporting level not re-exported by the prelude.
use crate::utils::elog::COMMERROR;

// ============================================================================
// backend_startup.h
// ============================================================================

/*
 * CAC_state is passed from postmaster to the backend process, to indicate
 * whether the connection should be accepted, or if the process should just
 * send an error to the client and close the connection.  Note that the
 * connection can fail for various reasons even if postmaster passed CAC_OK.
 */
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum CAC_state {
    CAC_OK,
    CAC_STARTUP,
    CAC_SHUTDOWN,
    CAC_RECOVERY,
    CAC_NOTHOTSTANDBY,
    CAC_TOOMANY,
}
pub use CAC_state::*;

/* Information passed from postmaster to backend process in 'startup_data' */
#[repr(C)]
pub struct BackendStartupData {
    pub canAcceptConnections: CAC_state,

    /*
     * Time at which the connection client socket is created. Only used for
     * client and wal sender connections.
     */
    pub socket_created: TimestampTz,

    /*
     * Time at which the postmaster initiates process creation -- either
     * through fork or otherwise. Only used for client and wal sender
     * connections.
     */
    pub fork_started: TimestampTz,
}

/*
 * Granular control over which messages to log for the log_connections GUC.
 *
 * RECEIPT, AUTHENTICATION, AUTHORIZATION, and SETUP_DURATIONS are different
 * aspects of connection establishment and backend setup for which we may emit
 * a log message.
 *
 * ALL is a convenience alias equivalent to all of the above aspects.
 *
 * ON is backwards compatibility alias for the connection aspects that were
 * logged in Postgres versions < 18.
 */
pub const LOG_CONNECTION_RECEIPT: uint32 = 1 << 0;
pub const LOG_CONNECTION_AUTHENTICATION: uint32 = 1 << 1;
pub const LOG_CONNECTION_AUTHORIZATION: uint32 = 1 << 2;
pub const LOG_CONNECTION_SETUP_DURATIONS: uint32 = 1 << 3;
pub const LOG_CONNECTION_ON: uint32 =
    LOG_CONNECTION_RECEIPT | LOG_CONNECTION_AUTHENTICATION | LOG_CONNECTION_AUTHORIZATION;
pub const LOG_CONNECTION_ALL: uint32 = LOG_CONNECTION_RECEIPT
    | LOG_CONNECTION_AUTHENTICATION
    | LOG_CONNECTION_AUTHORIZATION
    | LOG_CONNECTION_SETUP_DURATIONS;
pub type LogConnectionOption = uint32;

/*
 * A collection of timings of various stages of connection establishment and
 * setup for client backends and WAL senders.
 *
 * Used to emit the setup_durations log message for the log_connections GUC.
 */
#[repr(C)]
pub struct ConnectionTiming {
    /*
     * The time at which the client socket is created and the time at which
     * the connection is fully set up and first ready for query. Together
     * these represent the total connection establishment and setup time.
     */
    pub socket_create: TimestampTz,
    pub ready_for_use: TimestampTz,

    /* Time at which process creation was initiated */
    pub fork_start: TimestampTz,

    /* Time at which process creation was completed */
    pub fork_end: TimestampTz,

    /* Time at which authentication started */
    pub auth_start: TimestampTz,

    /* Time at which authentication was finished */
    pub auth_end: TimestampTz,
}

// ============================================================================
// backend_startup.c
// ============================================================================

/* GUCs */
#[no_mangle]
pub static mut Trace_connection_negotiation: bool = false;
#[no_mangle]
pub static mut log_connections: uint32 = 0;
#[no_mangle]
pub static mut log_connections_string: *mut c_char = std::ptr::null_mut();

/* Other globals */

/*
 * ConnectionTiming stores timestamps of various points in connection
 * establishment and setup.
 * ready_for_use is initialized to a special value here so we can check if
 * we've already set it before doing so in PostgresMain().
 */
#[no_mangle]
pub static mut conn_timing: ConnectionTiming = ConnectionTiming {
    socket_create: 0,
    ready_for_use: TIMESTAMP_MINUS_INFINITY,
    fork_start: 0,
    fork_end: 0,
    auth_start: 0,
    auth_end: 0,
};

/*
 * Entry point for a new backend process.
 *
 * Initialize the connection, read the startup packet, authenticate the
 * client, and start the main processing loop.
 */
#[no_mangle]
pub unsafe extern "C" fn BackendMain(startup_data: *const c_void, startup_data_len: Size) {
    let bsdata = startup_data as *const BackendStartupData;

    Assert(startup_data_len == std::mem::size_of::<BackendStartupData>());
    Assert(!MyClientSocket.is_null());

    // #ifdef EXEC_BACKEND  -- not compiled

    /* Perform additional initialization and collect startup packet */
    BackendInitialize(MyClientSocket, (*bsdata).canAcceptConnections);

    /*
     * Create a per-backend PGPROC struct in shared memory.  We must do this
     * before we can use LWLocks or access any shared memory.
     */
    InitProcess();

    /*
     * Make sure we aren't in PostmasterContext anymore.  (We can't delete it
     * just yet, though, because InitPostgres will need the HBA data.)
     */
    MemoryContextSwitchTo(TopMemoryContext);

    PostgresMain((*MyProcPort).database_name, (*MyProcPort).user_name);
}

/*
 * BackendInitialize -- initialize an interactive (postmaster-child)
 *				backend process, and collect the client's startup packet.
 *
 * returns: nothing.  Will not return at all if there's any failure.
 *
 * Note: this code does not depend on having any access to shared memory.
 * Indeed, our approach to SIGTERM/timeout handling *requires* that
 * shared memory not have been touched yet; see comments within.
 * In the EXEC_BACKEND case, we are physically attached to shared memory
 * but have not yet set up most of our local pointers to shmem structures.
 */
unsafe fn BackendInitialize(client_sock: *mut ClientSocket, cac: CAC_state) {
    let status: c_int;
    let ret: c_int;
    let port: *mut Port;
    let mut remote_host: [c_char; NI_MAXHOST as usize] = [0; NI_MAXHOST as usize];
    let mut remote_port: [c_char; NI_MAXSERV as usize] = [0; NI_MAXSERV as usize];
    let mut ps_data: StringInfoData = std::mem::zeroed();
    let oldcontext: MemoryContext;

    /* Tell fd.c about the long-lived FD associated with the client_sock */
    ReserveExternalFD();

    /*
     * PreAuthDelay is a debugging aid for investigating problems in the
     * authentication cycle: it can be set in postgresql.conf to allow time to
     * attach to the newly-forked backend with a debugger.  (See also
     * PostAuthDelay, which we allow clients to pass through PGOPTIONS, but it
     * is not honored until after authentication.)
     */
    if PreAuthDelay > 0 {
        pg_usleep(PreAuthDelay as i64 * 1000000);
    }

    /* This flag will remain set until InitPostgres finishes authentication */
    ClientAuthInProgress = true; /* limit visibility of log messages */

    /*
     * Initialize libpq and enable reporting of ereport errors to the client.
     * Must do this now because authentication uses libpq to send messages.
     *
     * The Port structure and all data structures attached to it are allocated
     * in TopMemoryContext, so that they survive into PostgresMain execution.
     * We need not worry about leaking this storage on failure, since we
     * aren't in the postmaster process anymore.
     */
    oldcontext = MemoryContextSwitchTo(TopMemoryContext);
    MyProcPort = pq_init(client_sock);
    port = MyProcPort;
    MemoryContextSwitchTo(oldcontext);

    whereToSendOutput = DestRemote; /* now safe to ereport to client */

    /* set these to empty in case they are needed before we set them up */
    (*port).remote_host = c"".as_ptr() as *mut c_char;
    (*port).remote_port = c"".as_ptr() as *mut c_char;

    /*
     * We arrange to do _exit(1) if we receive SIGTERM or timeout while trying
     * to collect the startup packet; while SIGQUIT results in _exit(2).
     * Otherwise the postmaster cannot shutdown the database FAST or IMMED
     * cleanly if a buggy client fails to send the packet promptly.
     *
     * Exiting with _exit(1) is only possible because we have not yet touched
     * shared memory; therefore no outside-the-process state needs to get
     * cleaned up.
     */
    pqsignal(SIGTERM, process_startup_packet_die as usize);
    /* SIGQUIT handler was already set up by InitPostmasterChild */
    InitializeTimeouts(); /* establishes SIGALRM handler */
    sigprocmask(SIG_SETMASK, &raw const StartupBlockSig, std::ptr::null_mut());

    /*
     * Get the remote host name and port for logging and status display.
     */
    remote_host[0] = b'\0' as c_char;
    remote_port[0] = b'\0' as c_char;
    ret = pg_getnameinfo_all(
        &raw mut (*port).raddr.addr,
        (*port).raddr.salen,
        remote_host.as_mut_ptr(),
        std::mem::size_of_val(&remote_host) as Size,
        remote_port.as_mut_ptr(),
        std::mem::size_of_val(&remote_port) as Size,
        (if log_hostname { 0 } else { NI_NUMERICHOST }) | NI_NUMERICSERV,
    );
    if ret != 0 {
        elog!(
            WARNING,
            "pg_getnameinfo_all() failed: {}",
            CStr_from(gai_strerror(ret))
        );
    }

    /*
     * Save remote_host and remote_port in port structure (after this, they
     * will appear in log_line_prefix data for log messages).
     */
    (*port).remote_host = MemoryContextStrdup(TopMemoryContext, remote_host.as_ptr());
    (*port).remote_port = MemoryContextStrdup(TopMemoryContext, remote_port.as_ptr());

    /* And now we can log that the connection was received, if enabled */
    if log_connections & LOG_CONNECTION_RECEIPT != 0 {
        if remote_port[0] != 0 {
            elog!(
                LOG,
                "connection received: host={} port={}",
                CStr_from(remote_host.as_ptr()),
                CStr_from(remote_port.as_ptr())
            );
        } else {
            elog!(
                LOG,
                "connection received: host={}",
                CStr_from(remote_host.as_ptr())
            );
        }
    }

    /* For testing client error handling */
    // #ifdef USE_INJECTION_POINTS -- not compiled

    /*
     * If we did a reverse lookup to name, we might as well save the results
     * rather than possibly repeating the lookup during authentication.
     *
     * Note that we don't want to specify NI_NAMEREQD above, because then we'd
     * get nothing useful for a client without an rDNS entry.  Therefore, we
     * must check whether we got a numeric IPv4 or IPv6 address, and not save
     * it into remote_hostname if so.  (This test is conservative and might
     * sometimes classify a hostname as numeric, but an error in that
     * direction is safe; it only results in a possible extra lookup.)
     */
    if log_hostname
        && ret == 0
        && strspn(remote_host.as_ptr(), c"0123456789.".as_ptr()) < strlen(remote_host.as_ptr())
        && strspn(remote_host.as_ptr(), c"0123456789ABCDEFabcdef:".as_ptr())
            < strlen(remote_host.as_ptr())
    {
        (*port).remote_hostname = MemoryContextStrdup(TopMemoryContext, remote_host.as_ptr());
    }

    /*
     * Ready to begin client interaction.  We will give up and _exit(1) after
     * a time delay, so that a broken client can't hog a connection
     * indefinitely.  PreAuthDelay and any DNS interactions above don't count
     * against the time limit.
     *
     * Note: AuthenticationTimeout is applied here while waiting for the
     * startup packet, and then again in InitPostgres for the duration of any
     * authentication operations.  So a hostile client could tie up the
     * process for nearly twice AuthenticationTimeout before we kick him off.
     *
     * Note: because PostgresMain will call InitializeTimeouts again, the
     * registration of STARTUP_PACKET_TIMEOUT will be lost.  This is okay
     * since we never use it again after this function.
     */
    RegisterTimeout(STARTUP_PACKET_TIMEOUT, StartupPacketTimeoutHandler);
    enable_timeout_after(STARTUP_PACKET_TIMEOUT, (AuthenticationTimeout * 1000) as i64);

    /* Handle direct SSL handshake */
    let mut status = ProcessSSLStartup(port);

    /*
     * Receive the startup packet (which might turn out to be a cancel request
     * packet).
     */
    if status == STATUS_OK {
        status = ProcessStartupPacket(port, false, false);
    }

    /*
     * If we're going to reject the connection due to database state, say so
     * now instead of wasting cycles on an authentication exchange. (This also
     * allows a pg_ping utility to be written.)
     */
    if status == STATUS_OK {
        match cac {
            CAC_STARTUP => {
                elog!(FATAL, "the database system is starting up");
            }
            CAC_NOTHOTSTANDBY => {
                if !EnableHotStandby {
                    elog!(
                        FATAL,
                        "the database system is not accepting connections"
                    );
                } else if reachedConsistency {
                    elog!(
                        FATAL,
                        "the database system is not yet accepting connections"
                    );
                } else {
                    elog!(
                        FATAL,
                        "the database system is not yet accepting connections"
                    );
                }
            }
            CAC_SHUTDOWN => {
                elog!(FATAL, "the database system is shutting down");
            }
            CAC_RECOVERY => {
                elog!(FATAL, "the database system is in recovery mode");
            }
            CAC_TOOMANY => {
                elog!(FATAL, "sorry, too many clients already");
            }
            CAC_OK => {}
        }
    }

    /*
     * Disable the timeout, and prevent SIGTERM again.
     */
    disable_timeout(STARTUP_PACKET_TIMEOUT, false);
    sigprocmask(SIG_SETMASK, &raw const BlockSig, std::ptr::null_mut());

    /*
     * As a safety check that nothing in startup has yet performed
     * shared-memory modifications that would need to be undone if we had
     * exited through SIGTERM or timeout above, check that no on_shmem_exit
     * handlers have been registered yet.  (This isn't terribly bulletproof,
     * since someone might misuse an on_proc_exit handler for shmem cleanup,
     * but it's a cheap and helpful check.  We cannot disallow on_proc_exit
     * handlers unfortunately, since pq_init() already registered one.)
     */
    check_on_shmem_exit_lists_are_empty();

    /*
     * Stop here if it was bad or a cancel packet.  ProcessStartupPacket
     * already did any appropriate error reporting.
     */
    if status != STATUS_OK {
        proc_exit(0);
    }

    /*
     * Now that we have the user and database name, we can set the process
     * title for ps.  It's good to do this as early as possible in startup.
     */
    initStringInfo(&raw mut ps_data);
    if am_walsender {
        appendStringInfo(
            &raw mut ps_data,
            c"%s ".as_ptr(),
            GetBackendTypeDesc(B_WAL_SENDER),
        );
    }
    appendStringInfo(&raw mut ps_data, c"%s ".as_ptr(), (*port).user_name);
    if *(*port).database_name != b'\0' as c_char {
        appendStringInfo(&raw mut ps_data, c"%s ".as_ptr(), (*port).database_name);
    }
    appendStringInfoString(&raw mut ps_data, (*port).remote_host);
    if *(*port).remote_port != b'\0' as c_char {
        appendStringInfo(&raw mut ps_data, c"(%s)".as_ptr(), (*port).remote_port);
    }

    init_ps_display(ps_data.data);
    pfree(ps_data.data as *mut c_void);

    set_ps_display(c"initializing".as_ptr());

    let _ = (status, ret, oldcontext);
}

/*
 * Check for a direct SSL connection.
 *
 * This happens before the startup packet so we are careful not to actually
 * read any bytes from the stream if it's not a direct SSL connection.
 */
unsafe fn ProcessSSLStartup(port: *mut Port) -> c_int {
    let firstbyte: c_int;

    Assert(!(*port).ssl_in_use);

    pq_startmsgread();
    firstbyte = pq_peekbyte();
    pq_endmsgread();
    if firstbyte == EOF {
        /*
         * Like in ProcessStartupPacket, if we get no data at all, don't
         * clutter the log with a complaint.
         */
        return STATUS_ERROR;
    }

    if firstbyte != 0x16 {
        /* Not an SSL handshake message */
        return STATUS_OK;
    }

    /*
     * First byte indicates standard SSL handshake message
     *
     * (It can't be a Postgres startup length because in network byte order
     * that would be a startup packet hundreds of megabytes long)
     */

    // #ifdef USE_SSL -- not compiled; SSL not supported by this build
    /* SSL not supported by this build */
    // goto reject;

    // reject:
    if Trace_connection_negotiation {
        elog!(LOG, "direct SSL connection rejected");
    }
    STATUS_ERROR
}

/*
 * Read a client's startup packet and do something according to it.
 *
 * Returns STATUS_OK or STATUS_ERROR, or might call ereport(FATAL) and
 * not return at all.
 *
 * (Note that ereport(FATAL) stuff is sent to the client, so only use it
 * if that's what you want.  Return STATUS_ERROR if you don't want to
 * send anything to the client, which would typically be appropriate
 * if we detect a communications failure.)
 *
 * Set ssl_done and/or gss_done when negotiation of an encrypted layer
 * (currently, TLS or GSSAPI) is completed. A successful negotiation of either
 * encryption layer sets both flags, but a rejected negotiation sets only the
 * flag for that layer, since the client may wish to try the other one. We
 * should make no assumption here about the order in which the client may make
 * requests.
 */
unsafe fn ProcessStartupPacket(port: *mut Port, ssl_done: bool, gss_done: bool) -> c_int {
    let mut len: int32;
    let buf: *mut c_char;
    let proto: ProtocolVersion;
    let oldcontext: MemoryContext;

    pq_startmsgread();

    /*
     * Grab the first byte of the length word separately, so that we can tell
     * whether we have no data at all or an incomplete packet.  (This might
     * sound inefficient, but it's not really, because of buffering in
     * pqcomm.c.)
     */
    len = 0;
    if pq_getbytes(&raw mut len as *mut c_char, 1) == EOF {
        /*
         * If we get no data at all, don't clutter the log with a complaint;
         * such cases often occur for legitimate reasons.  An example is that
         * we might be here after responding to NEGOTIATE_SSL_CODE, and if the
         * client didn't like our response, it'll probably just drop the
         * connection.  Service-monitoring software also often just opens and
         * closes a connection without sending anything.  (So do port
         * scanners, which may be less benign, but it's not really our job to
         * notice those.)
         */
        return STATUS_ERROR;
    }

    if pq_getbytes((&raw mut len as *mut c_char).add(1), 3) == EOF {
        /* Got a partial length word, so bleat about that */
        if !ssl_done && !gss_done {
            ereport!(COMMERROR, "incomplete startup packet");
        }
        return STATUS_ERROR;
    }

    len = pg_ntoh32(len as uint32) as int32;
    len -= 4;

    if len < std::mem::size_of::<ProtocolVersion>() as int32 || len > MAX_STARTUP_PACKET_LENGTH {
        ereport!(COMMERROR, "invalid length of startup packet");
        return STATUS_ERROR;
    }

    /*
     * Allocate space to hold the startup packet, plus one extra byte that's
     * initialized to be zero.  This ensures we will have null termination of
     * all strings inside the packet.
     */
    buf = palloc((len + 1) as Size) as *mut c_char;
    *buf.add(len as usize) = b'\0' as c_char;

    if pq_getbytes(buf, len as Size) == EOF {
        ereport!(COMMERROR, "incomplete startup packet");
        return STATUS_ERROR;
    }
    pq_endmsgread();

    /*
     * The first field is either a protocol version number or a special
     * request code.
     */
    proto = pg_ntoh32(*(buf as *const ProtocolVersion));
    (*port).proto = proto;

    if proto == CANCEL_REQUEST_CODE {
        ProcessCancelRequestPacket(port, buf as *mut c_void, len);
        /* Not really an error, but we don't want to proceed further */
        return STATUS_ERROR;
    }

    if proto == NEGOTIATE_SSL_CODE && !ssl_done {
        let SSLok: c_char;

        // #ifdef USE_SSL -- not compiled
        SSLok = b'N' as c_char; /* No support for SSL */

        if Trace_connection_negotiation {
            if SSLok == b'S' as c_char {
                elog!(LOG, "SSLRequest accepted");
            } else {
                elog!(LOG, "SSLRequest rejected");
            }
        }

        let SSLok_local = SSLok;
        while secure_write(port, &raw const SSLok_local as *mut c_void, 1) != 1 {
            if errno() == EINTR {
                continue; /* if interrupted, just retry */
            }
            elog!(COMMERROR, "failed to send SSL negotiation response: %m");
            return STATUS_ERROR; /* close the connection */
        }

        // #ifdef USE_SSL -- not compiled

        /*
         * At this point we should have no data already buffered.  If we do,
         * it was received before we performed the SSL handshake, so it wasn't
         * encrypted and indeed may have been injected by a man-in-the-middle.
         * We report this case to the client.
         */
        if pq_buffer_remaining_data() > 0 {
            ereport!(FATAL, "received unencrypted data after SSL request");
            unreachable!();
        }

        /*
         * regular startup packet, cancel, etc packet should follow, but not
         * another SSL negotiation request, and a GSS request should only
         * follow if SSL was rejected (client may negotiate in either order)
         */
        return ProcessStartupPacket(port, true, SSLok == b'S' as c_char);
    } else if proto == NEGOTIATE_GSS_CODE && !gss_done {
        let GSSok: c_char = b'N' as c_char;

        // #ifdef ENABLE_GSS -- not compiled

        if Trace_connection_negotiation {
            if GSSok == b'G' as c_char {
                elog!(LOG, "GSSENCRequest accepted");
            } else {
                elog!(LOG, "GSSENCRequest rejected");
            }
        }

        let GSSok_local = GSSok;
        while secure_write(port, &raw const GSSok_local as *mut c_void, 1) != 1 {
            if errno() == EINTR {
                continue;
            }
            elog!(COMMERROR, "failed to send GSSAPI negotiation response: %m");
            return STATUS_ERROR; /* close the connection */
        }

        // #ifdef ENABLE_GSS -- not compiled

        /*
         * At this point we should have no data already buffered.  If we do,
         * it was received before we performed the GSS handshake, so it wasn't
         * encrypted and indeed may have been injected by a man-in-the-middle.
         * We report this case to the client.
         */
        if pq_buffer_remaining_data() > 0 {
            ereport!(
                FATAL,
                "received unencrypted data after GSSAPI encryption request"
            );
            unreachable!();
        }

        /*
         * regular startup packet, cancel, etc packet should follow, but not
         * another GSS negotiation request, and an SSL request should only
         * follow if GSS was rejected (client may negotiate in either order)
         */
        return ProcessStartupPacket(port, GSSok == b'G' as c_char, true);
    }

    /* Could add additional special packet types here */

    /*
     * Set FrontendProtocol now so that ereport() knows what format to send if
     * we fail during startup. We use the protocol version requested by the
     * client unless it's higher than the latest version we support. It's
     * possible that error message fields might look different in newer
     * protocol versions, but that's something those new clients should be
     * able to deal with.
     */
    FrontendProtocol = Min(proto, PG_PROTOCOL_LATEST);

    /* Check that the major protocol version is in range. */
    if PG_PROTOCOL_MAJOR(proto) < PG_PROTOCOL_MAJOR(PG_PROTOCOL_EARLIEST)
        || PG_PROTOCOL_MAJOR(proto) > PG_PROTOCOL_MAJOR(PG_PROTOCOL_LATEST)
    {
        elog!(
            FATAL,
            "unsupported frontend protocol {}.{}: server supports {}.0 to {}.{}",
            PG_PROTOCOL_MAJOR(proto),
            PG_PROTOCOL_MINOR(proto),
            PG_PROTOCOL_MAJOR(PG_PROTOCOL_EARLIEST),
            PG_PROTOCOL_MAJOR(PG_PROTOCOL_LATEST),
            PG_PROTOCOL_MINOR(PG_PROTOCOL_LATEST)
        );
        unreachable!();
    }

    /*
     * Now fetch parameters out of startup packet and save them into the Port
     * structure.
     */
    oldcontext = MemoryContextSwitchTo(TopMemoryContext);

    /* Handle protocol version 3 startup packet */
    {
        let mut offset: int32 = std::mem::size_of::<ProtocolVersion>() as int32;
        let mut unrecognized_protocol_options: *mut List = NIL;

        /*
         * Scan packet body for name/option pairs.  We can assume any string
         * beginning within the packet body is null-terminated, thanks to
         * zeroing extra byte above.
         */
        (*port).guc_options = NIL;

        while offset < len {
            let nameptr: *mut c_char = buf.add(offset as usize);
            let valoffset: int32;
            let valptr: *mut c_char;

            if *nameptr == b'\0' as c_char {
                break; /* found packet terminator */
            }
            valoffset = offset + strlen(nameptr) as int32 + 1;
            if valoffset >= len {
                break; /* missing value, will complain below */
            }
            valptr = buf.add(valoffset as usize);

            if strcmp(nameptr, c"database".as_ptr()) == 0 {
                (*port).database_name = pstrdup(valptr);
            } else if strcmp(nameptr, c"user".as_ptr()) == 0 {
                (*port).user_name = pstrdup(valptr);
            } else if strcmp(nameptr, c"options".as_ptr()) == 0 {
                (*port).cmdline_options = pstrdup(valptr);
            } else if strcmp(nameptr, c"replication".as_ptr()) == 0 {
                /*
                 * Due to backward compatibility concerns the replication
                 * parameter is a hybrid beast which allows the value to be
                 * either boolean or the string 'database'. The latter
                 * connects to a specific database which is e.g. required for
                 * logical decoding while.
                 */
                if strcmp(valptr, c"database".as_ptr()) == 0 {
                    am_walsender = true;
                    am_db_walsender = true;
                } else if !parse_bool(valptr, &raw mut am_walsender) {
                    elog!(
                        FATAL,
                        "invalid value for parameter \"{}\": \"{}\"",
                        "replication",
                        CStr_from(valptr)
                    );
                    unreachable!();
                }
            } else if strncmp(nameptr, c"_pq_.".as_ptr(), 5) == 0 {
                /*
                 * Any option beginning with _pq_. is reserved for use as a
                 * protocol-level option, but at present no such options are
                 * defined.
                 */
                unrecognized_protocol_options =
                    lappend(unrecognized_protocol_options, pstrdup(nameptr) as *mut c_void);
            } else {
                /* Assume it's a generic GUC option */
                (*port).guc_options =
                    lappend((*port).guc_options, pstrdup(nameptr) as *mut c_void);
                (*port).guc_options =
                    lappend((*port).guc_options, pstrdup(valptr) as *mut c_void);

                /*
                 * Copy application_name to port if we come across it.  This
                 * is done so we can log the application_name in the
                 * connection authorization message.  Note that the GUC would
                 * be used but we haven't gone through GUC setup yet.
                 */
                if strcmp(nameptr, c"application_name".as_ptr()) == 0 {
                    (*port).application_name = pg_clean_ascii(valptr, 0);
                }
            }
            offset = valoffset + strlen(valptr) as int32 + 1;
        }

        /*
         * If we didn't find a packet terminator exactly at the end of the
         * given packet length, complain.
         */
        if offset != len - 1 {
            ereport!(
                FATAL,
                "invalid startup packet layout: expected terminator as last byte"
            );
            unreachable!();
        }

        /*
         * If the client requested a newer protocol version or if the client
         * requested any protocol options we didn't recognize, let them know
         * the newest minor protocol version we do support and the names of
         * any unrecognized options.
         */
        if PG_PROTOCOL_MINOR(proto) > PG_PROTOCOL_MINOR(PG_PROTOCOL_LATEST)
            || unrecognized_protocol_options != NIL
        {
            SendNegotiateProtocolVersion(unrecognized_protocol_options);
        }
    }

    /* Check a user name was given. */
    if (*port).user_name.is_null() || *(*port).user_name == b'\0' as c_char {
        ereport!(
            FATAL,
            "no PostgreSQL user name specified in startup packet"
        );
        unreachable!();
    }

    /* The database defaults to the user name. */
    if (*port).database_name.is_null() || *(*port).database_name == b'\0' as c_char {
        (*port).database_name = pstrdup((*port).user_name);
    }

    /*
     * Truncate given database and user names to length of a Postgres name.
     * This avoids lookup failures when overlength names are given.
     */
    if strlen((*port).database_name) >= NAMEDATALEN as Size {
        *(*port).database_name.add(NAMEDATALEN as usize - 1) = b'\0' as c_char;
    }
    if strlen((*port).user_name) >= NAMEDATALEN as Size {
        *(*port).user_name.add(NAMEDATALEN as usize - 1) = b'\0' as c_char;
    }

    if am_walsender {
        MyBackendType = B_WAL_SENDER;
    } else {
        MyBackendType = B_BACKEND;
    }

    /*
     * Normal walsender backends, e.g. for streaming replication, are not
     * connected to a particular database. But walsenders used for logical
     * replication need to connect to a specific database. We allow streaming
     * replication commands to be issued even if connected to a database as it
     * can make sense to first make a basebackup and then stream changes
     * starting from that.
     */
    if am_walsender && !am_db_walsender {
        *(*port).database_name = b'\0' as c_char;
    }

    /*
     * Done filling the Port structure
     */
    MemoryContextSwitchTo(oldcontext);

    STATUS_OK
}

/*
 * The client has sent a cancel request packet, not a normal
 * start-a-new-connection packet.  Perform the necessary processing.  Nothing
 * is sent back to the client.
 */
unsafe fn ProcessCancelRequestPacket(_port: *mut Port, pkt: *mut c_void, pktlen: c_int) {
    let canc: *mut CancelRequestPacket;
    let len: c_int;

    if (pktlen as usize) < core::mem::offset_of!(CancelRequestPacket, cancelAuthCode) {
        ereport!(COMMERROR, "invalid length of cancel request packet");
        return;
    }
    len = pktlen - core::mem::offset_of!(CancelRequestPacket, cancelAuthCode) as c_int;
    if len == 0 || len > 256 {
        ereport!(
            COMMERROR,
            "invalid length of cancel key in cancel request packet"
        );
        return;
    }

    canc = pkt as *mut CancelRequestPacket;
    SendCancelRequest(
        pg_ntoh32((*canc).backendPID as uint32) as c_int,
        (*canc).cancelAuthCode.as_ptr(),
        len,
    );
}

/*
 * Send a NegotiateProtocolVersion to the client.  This lets the client know
 * that they have either requested a newer minor protocol version than we are
 * able to speak, or at least one protocol option that we don't understand, or
 * possibly both. FrontendProtocol has already been set to the version
 * requested by the client or the highest version we know how to speak,
 * whichever is older. If the highest version that we know how to speak is too
 * old for the client, it can abandon the connection.
 *
 * We also include in the response a list of protocol options we didn't
 * understand.  This allows clients to include optional parameters that might
 * be present either in newer protocol versions or third-party protocol
 * extensions without fear of having to reconnect if those options are not
 * understood, while at the same time making certain that the client is aware
 * of which options were actually accepted.
 */
unsafe fn SendNegotiateProtocolVersion(unrecognized_protocol_options: *mut List) {
    let mut buf: StringInfoData = std::mem::zeroed();

    pq_beginmessage(&raw mut buf, PqMsg_NegotiateProtocolVersion as c_char);
    pq_sendint32(&raw mut buf, FrontendProtocol as uint32);
    pq_sendint32(
        &raw mut buf,
        list_length(unrecognized_protocol_options) as uint32,
    );
    foreach!(lc, unrecognized_protocol_options, {
        pq_sendstring(&raw mut buf, lfirst(current_cell!(lc)) as *const c_char);
    });
    pq_endmessage(&raw mut buf);

    /* no need to flush, some other message will follow */
}

/*
 * SIGTERM while processing startup packet.
 *
 * Running proc_exit() from a signal handler would be quite unsafe.
 * However, since we have not yet touched shared memory, we can just
 * pull the plug and exit without running any atexit handlers.
 *
 * One might be tempted to try to send a message, or log one, indicating
 * why we are disconnecting.  However, that would be quite unsafe in itself.
 * Also, it seems undesirable to provide clues about the database's state
 * to a client that has not yet completed authentication, or even sent us
 * a startup packet.
 */
unsafe extern "C" fn process_startup_packet_die(_postgres_signal_arg: c_int) {
    _exit(1);
}

/*
 * Timeout while processing startup packet.
 * As for process_startup_packet_die(), we exit via _exit(1).
 */
unsafe extern "C" fn StartupPacketTimeoutHandler() {
    _exit(1);
}

/*
 * Helper for the log_connections GUC check hook.
 *
 * `elemlist` is a listified version of the string input passed to the
 * log_connections GUC check hook, check_log_connections().
 * check_log_connections() is responsible for cleaning up `elemlist`.
 *
 * validate_log_connections_options() returns false if an error was
 * encountered and the GUC input could not be validated and true otherwise.
 *
 * `flags` returns the flags that should be stored in the log_connections GUC
 * by its assign hook.
 */
unsafe fn validate_log_connections_options(elemlist: *mut List, flags: *mut uint32) -> bool {
    let mut item: *mut c_char;

    /*
     * For backwards compatibility, we accept these tokens by themselves.
     *
     * Prior to PostgreSQL 18, log_connections was a boolean GUC that accepted
     * any unambiguous substring of 'true', 'false', 'yes', 'no', 'on', and
     * 'off'. Since log_connections became a list of strings in 18, we only
     * accept complete option strings.
     */
    static compat_options: [config_enum_entry; 8] = [
        config_enum_entry { name: c"off".as_ptr(), val: 0, hidden: false },
        config_enum_entry { name: c"false".as_ptr(), val: 0, hidden: false },
        config_enum_entry { name: c"no".as_ptr(), val: 0, hidden: false },
        config_enum_entry { name: c"0".as_ptr(), val: 0, hidden: false },
        config_enum_entry { name: c"on".as_ptr(), val: LOG_CONNECTION_ON as c_int, hidden: false },
        config_enum_entry { name: c"true".as_ptr(), val: LOG_CONNECTION_ON as c_int, hidden: false },
        config_enum_entry { name: c"yes".as_ptr(), val: LOG_CONNECTION_ON as c_int, hidden: false },
        config_enum_entry { name: c"1".as_ptr(), val: LOG_CONNECTION_ON as c_int, hidden: false },
    ];

    *flags = 0;

    /* If an empty string was passed, we're done */
    if list_length(elemlist) == 0 {
        return true;
    }

    /*
     * Now check for the backwards compatibility options. They must always be
     * specified on their own, so we error out if the first option is a
     * backwards compatibility option and other options are also specified.
     */
    item = linitial(elemlist) as *mut c_char;

    for i in 0..compat_options.len() {
        let option = &compat_options[i];

        if pg_strcasecmp(item, option.name) != 0 {
            continue;
        }

        if list_length(elemlist) > 1 {
            GUC_check_errdetail!(
                "Cannot specify log_connections option \"{}\" in a list with other options.",
                CStr_from(item)
            );
            return false;
        }

        *flags = option.val as uint32;
        return true;
    }

    /* Now check the aspect options. The empty string was already handled */
    foreach!(l, elemlist, {
        static options: [config_enum_entry; 5] = [
            config_enum_entry { name: c"receipt".as_ptr(), val: LOG_CONNECTION_RECEIPT as c_int, hidden: false },
            config_enum_entry { name: c"authentication".as_ptr(), val: LOG_CONNECTION_AUTHENTICATION as c_int, hidden: false },
            config_enum_entry { name: c"authorization".as_ptr(), val: LOG_CONNECTION_AUTHORIZATION as c_int, hidden: false },
            config_enum_entry { name: c"setup_durations".as_ptr(), val: LOG_CONNECTION_SETUP_DURATIONS as c_int, hidden: false },
            config_enum_entry { name: c"all".as_ptr(), val: LOG_CONNECTION_ALL as c_int, hidden: false },
        ];

        item = lfirst(current_cell!(l)) as *mut c_char;
        let mut found = false;
        for i in 0..options.len() {
            let option = &options[i];

            if pg_strcasecmp(item, option.name) == 0 {
                *flags |= option.val as uint32;
                found = true;
                break; /* goto next */
            }
        }

        if !found {
            GUC_check_errdetail!("Invalid option \"{}\".", CStr_from(item));
            return false;
        }

        // next: ;
    });

    true
}

/*
 * GUC check hook for log_connections
 */
#[no_mangle]
pub unsafe extern "C" fn check_log_connections(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    _source: GucSource,
) -> bool {
    let mut flags: uint32 = 0;
    let rawstring: *mut c_char;
    let mut elemlist: *mut List = std::ptr::null_mut();
    let success: bool;

    /* Need a modifiable copy of string */
    rawstring = pstrdup(*newval);

    if !SplitIdentifierString(rawstring, b',' as c_char, &raw mut elemlist) {
        GUC_check_errdetail!(
            "Invalid list syntax in parameter \"{}\".",
            "log_connections"
        );
        pfree(rawstring as *mut c_void);
        list_free(elemlist);
        return false;
    }

    /* Validation logic is all in the helper */
    success = validate_log_connections_options(elemlist, &raw mut flags);

    /* Time for cleanup */
    pfree(rawstring as *mut c_void);
    list_free(elemlist);

    if !success {
        return false;
    }

    /*
     * We succeeded, so allocate `extra` and save the flags there for use by
     * assign_log_connections().
     */
    *extra = guc_malloc(LOG, std::mem::size_of::<c_int>() as Size);
    if (*extra).is_null() {
        return false;
    }
    *(*extra as *mut c_int) = flags as c_int;

    true
}

/*
 * GUC assign hook for log_connections
 */
#[no_mangle]
pub unsafe extern "C" fn assign_log_connections(_newval: *const c_char, extra: *mut c_void) {
    log_connections = *(extra as *const c_int) as uint32;
}

// ============================================================================
// Local stubs for unported dependencies
// ============================================================================

#[allow(non_camel_case_types)]
pub type ProtocolVersion = uint32;
#[allow(non_camel_case_types)]
pub type GucSource = c_int;

#[repr(C)]
pub struct config_enum_entry {
    pub name: *const c_char,
    pub val: c_int,
    pub hidden: bool,
}
unsafe impl Sync for config_enum_entry {}

unsafe fn CStr_from<'a>(p: *const c_char) -> &'a str {
    if p.is_null() {
        return "";
    }
    std::ffi::CStr::from_ptr(p).to_str().unwrap_or("")
}

unsafe fn errno() -> c_int {
    unimplemented!() // TODO: backend_startup.c
}

extern "C" {
    fn strlen(s: *const c_char) -> Size;
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn strncmp(a: *const c_char, b: *const c_char, n: Size) -> c_int;
    fn strspn(s: *const c_char, accept: *const c_char) -> Size;
    fn _exit(status: c_int) -> !;
    fn gai_strerror(errcode: c_int) -> *const c_char;
}

// --- stubbed externs / consts that other modules will provide ---

pub const STATUS_OK: c_int = 0;
pub const STATUS_ERROR: c_int = -1;
pub const EOF: c_int = -1;
pub const EINTR: c_int = 4;
pub const SIG_SETMASK: c_int = 2;
pub const SIGTERM: c_int = 15;
pub const NI_MAXHOST: c_int = 1025;
pub const NI_MAXSERV: c_int = 32;
pub const NI_NUMERICHOST: c_int = 1;
pub const NI_NUMERICSERV: c_int = 2;

pub const MAX_STARTUP_PACKET_LENGTH: int32 = 10000;
pub const NEGOTIATE_SSL_CODE: ProtocolVersion = (1234 << 16) | 5679;
pub const NEGOTIATE_GSS_CODE: ProtocolVersion = (1234 << 16) | 5680;
pub const CANCEL_REQUEST_CODE: ProtocolVersion = (1234 << 16) | 5678;
pub const PG_PROTOCOL_LATEST: ProtocolVersion = (3 << 16) | 0;
pub const PG_PROTOCOL_EARLIEST: ProtocolVersion = (3 << 16) | 0;

#[inline]
pub fn PG_PROTOCOL_MAJOR(v: ProtocolVersion) -> uint32 {
    v >> 16
}
#[inline]
pub fn PG_PROTOCOL_MINOR(v: ProtocolVersion) -> uint32 {
    v & 0xffff
}
#[inline]
pub fn Min(a: ProtocolVersion, b: ProtocolVersion) -> ProtocolVersion {
    if a < b { a } else { b }
}

pub const STARTUP_PACKET_TIMEOUT: c_int = 0;
pub const TIMESTAMP_MINUS_INFINITY: TimestampTz = i64::MIN;

pub const PqMsg_NegotiateProtocolVersion: c_int = b'v' as c_int;

// Backend type tags
pub const B_WAL_SENDER: c_int = 0;
pub const B_BACKEND: c_int = 0;

// whereToSendOutput value
pub const DestRemote: c_int = 0;

#[repr(C)]
pub struct ClientSocket {
    _private: [u8; 0],
}

#[repr(C)]
pub struct sockaddr_storage_wrap {
    pub addr: SockAddr,
    pub salen: Size,
}
#[repr(C)]
pub struct SockAddr {
    pub ss_family: c_int,
}

#[repr(C)]
pub struct Port {
    pub remote_host: *mut c_char,
    pub remote_port: *mut c_char,
    pub remote_hostname: *mut c_char,
    pub raddr: SockAddrStorage,
    pub laddr: SockAddrStorage,
    pub proto: ProtocolVersion,
    pub database_name: *mut c_char,
    pub user_name: *mut c_char,
    pub cmdline_options: *mut c_char,
    pub application_name: *mut c_char,
    pub guc_options: *mut List,
    pub ssl_in_use: bool,
}

#[repr(C)]
pub struct SockAddrStorage {
    pub addr: SockAddr,
    pub salen: Size,
}

#[repr(C)]
pub struct CancelRequestPacket {
    pub cancelRequestCode: ProtocolVersion,
    pub backendPID: int32,
    pub cancelAuthCode: [c_char; 256],
}

// extern statics defined elsewhere
extern "C" {
    pub static mut MyClientSocket: *mut ClientSocket;
    pub static mut MyProcPort: *mut Port;
    pub static mut whereToSendOutput: c_int;
    pub static mut ClientAuthInProgress: bool;
    pub static mut FrontendProtocol: ProtocolVersion;
    pub static mut MyBackendType: c_int;
    pub static mut am_walsender: bool;
    pub static mut am_db_walsender: bool;
    pub static mut log_hostname: bool;
    pub static mut PreAuthDelay: c_int;
    pub static mut AuthenticationTimeout: c_int;
    pub static mut EnableHotStandby: bool;
    pub static mut reachedConsistency: bool;
    pub static mut StartupBlockSig: SigSet;
    pub static mut BlockSig: SigSet;
    // TopMemoryContext is provided by crate::prelude (utils::palloc).
}

#[repr(C)]
pub struct SigSet {
    _private: [u8; 128],
}

// MemoryContext is provided by crate::prelude (utils::palloc).

// --- stubbed functions provided by other modules ---

// Assert is a #[macro_export] macro in the prelude; this fn-form stub keeps the
// `Assert(cond)` call sites compiling (macros and fns live in separate namespaces).
unsafe fn Assert(_cond: bool) {}

unsafe fn ReserveExternalFD() {
    unimplemented!() // TODO: storage/fd.c
}
unsafe fn pg_usleep(_micros: i64) {
    unimplemented!() // TODO: port/pgsleep.c
}
unsafe fn pq_init(_client_sock: *mut ClientSocket) -> *mut Port {
    unimplemented!() // TODO: libpq/pqcomm.c
}
unsafe fn pqsignal(_signo: c_int, _func: usize) {
    unimplemented!() // TODO: libpq/pqsignal.c
}
unsafe fn InitializeTimeouts() {
    unimplemented!() // TODO: utils/misc/timeout.c
}
unsafe fn sigprocmask(_how: c_int, _set: *const SigSet, _oldset: *mut SigSet) -> c_int {
    unimplemented!() // TODO: libc
}
unsafe fn pg_getnameinfo_all(
    _addr: *mut SockAddr,
    _salen: Size,
    _node: *mut c_char,
    _nodelen: Size,
    _service: *mut c_char,
    _servicelen: Size,
    _flags: c_int,
) -> c_int {
    unimplemented!() // TODO: common/ip.c
}
unsafe fn MemoryContextStrdup(_ctx: MemoryContext, _s: *const c_char) -> *mut c_char {
    unimplemented!() // TODO: utils/mmgr/mcxt.c
}
unsafe fn RegisterTimeout(_id: c_int, _handler: unsafe extern "C" fn()) -> c_int {
    unimplemented!() // TODO: utils/misc/timeout.c
}
unsafe fn enable_timeout_after(_id: c_int, _delay_ms: i64) {
    unimplemented!() // TODO: utils/misc/timeout.c
}
unsafe fn disable_timeout(_id: c_int, _keep_indicator: bool) {
    unimplemented!() // TODO: utils/misc/timeout.c
}
unsafe fn check_on_shmem_exit_lists_are_empty() {
    unimplemented!() // TODO: storage/ipc/ipc.c
}
unsafe fn proc_exit(_code: c_int) -> ! {
    unimplemented!() // TODO: storage/ipc/ipc.c
}
unsafe fn InitProcess() {
    unimplemented!() // TODO: storage/lmgr/proc.c
}
unsafe fn PostgresMain(_dbname: *const c_char, _username: *const c_char) {
    unimplemented!() // TODO: tcop/postgres.c
}
unsafe fn GetBackendTypeDesc(_backend_type: c_int) -> *const c_char {
    unimplemented!() // TODO: utils/init/miscinit.c
}
unsafe fn init_ps_display(_fixed_part: *const c_char) {
    unimplemented!() // TODO: utils/misc/ps_status.c
}
unsafe fn set_ps_display(_activity: *const c_char) {
    unimplemented!() // TODO: utils/misc/ps_status.c
}
unsafe fn pq_startmsgread() {
    unimplemented!() // TODO: libpq/pqcomm.c
}
unsafe fn pq_endmsgread() {
    unimplemented!() // TODO: libpq/pqcomm.c
}
unsafe fn pq_peekbyte() -> c_int {
    unimplemented!() // TODO: libpq/pqcomm.c
}
unsafe fn pq_getbytes(_s: *mut c_char, _len: Size) -> c_int {
    unimplemented!() // TODO: libpq/pqcomm.c
}
unsafe fn pq_buffer_remaining_data() -> Size {
    unimplemented!() // TODO: libpq/pqcomm.c
}
unsafe fn secure_write(_port: *mut Port, _ptr: *mut c_void, _len: Size) -> isize {
    unimplemented!() // TODO: libpq/be-secure.c
}
unsafe fn pg_ntoh32(_x: uint32) -> uint32 {
    unimplemented!() // TODO: port/pg_bswap.h
}
unsafe fn parse_bool(_value: *const c_char, _result: *mut bool) -> bool {
    unimplemented!() // TODO: utils/adt/bool.c
}
unsafe fn pg_clean_ascii(_str: *const c_char, _alloc_len: c_int) -> *mut c_char {
    unimplemented!() // TODO: common/string.c
}
unsafe fn SendCancelRequest(_backendPID: c_int, _cancelAuthCode: *const c_char, _len: c_int) {
    unimplemented!() // TODO: postmaster/postmaster.c
}
// appendStringInfo is a printf-style variadic macro in C; the translated call
// sites pass a C format string and a single value, so a local stub fn preserves
// those call sites without forcing a Rust `format!` string-literal.
unsafe fn appendStringInfo<T>(_str: *mut StringInfoData, _fmt: *const c_char, _arg: T) {
    unimplemented!() // TODO: lib/stringinfo.c (printf-style append)
}
unsafe fn pq_beginmessage(_buf: *mut StringInfoData, _msgtype: c_char) {
    unimplemented!() // TODO: libpq/pqformat.c
}
unsafe fn pq_sendint32(_buf: *mut StringInfoData, _i: uint32) {
    unimplemented!() // TODO: libpq/pqformat.c
}
unsafe fn pq_sendstring(_buf: *mut StringInfoData, _str: *const c_char) {
    unimplemented!() // TODO: libpq/pqformat.c
}
unsafe fn pq_endmessage(_buf: *mut StringInfoData) {
    unimplemented!() // TODO: libpq/pqformat.c
}
unsafe fn SplitIdentifierString(
    _rawstring: *mut c_char,
    _separator: c_char,
    _namelist: *mut *mut List,
) -> bool {
    unimplemented!() // TODO: utils/adt/varlena.c
}
unsafe fn pg_strcasecmp(_a: *const c_char, _b: *const c_char) -> c_int {
    unimplemented!() // TODO: port/pgstrcasecmp.c
}
unsafe fn guc_malloc(_elevel: c_int, _size: Size) -> *mut c_void {
    unimplemented!() // TODO: utils/misc/guc.c
}

// GUC_check_errdetail is a macro in C; provide a minimal stub macro here.
macro_rules! GUC_check_errdetail {
    ($($arg:tt)*) => {{
        let _ = format!($($arg)*);
    }};
}
use GUC_check_errdetail;
