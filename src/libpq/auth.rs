//! libpq/auth.c - Routines to handle network authentication.

use crate::prelude::*;

use crate::appendStringInfo;
use crate::lib::stringinfo::{
    appendBinaryStringInfo, appendStringInfoChar, appendStringInfoString,
    initStringInfo, StringInfo, StringInfoData,
};
use crate::nodes::pg_list::List;
use crate::libpq::crypt::{
    get_password_type, get_role_password, md5_crypt_verify, plain_crypt_verify, PasswordType,
    PASSWORD_TYPE_MD5,
};
use crate::libpq::libpq::{
    pq_flush, pq_getbyte, pq_getmessage, pq_startmsgread, secure_loaded_verify_locations,
};
use crate::libpq::libpq_be::{
    ClientConnectionInfo, Port, MyClientConnectionInfo,
};
use crate::libpq::oauth::pg_be_oauth_mech;
use crate::libpq::pqformat::{pq_beginmessage, pq_endmessage, pq_sendbytes, pq_sendint32};
use crate::libpq::protocol::{
    AUTH_REQ_GSS, AUTH_REQ_GSS_CONT, AUTH_REQ_MD5, AUTH_REQ_OK, AUTH_REQ_PASSWORD,
    AUTH_REQ_SASL_FIN, AUTH_REQ_SSPI, PqMsg_AuthenticationRequest, PqMsg_GSSResponse,
    PqMsg_PasswordMessage,
};
use crate::libpq::sasl::{pg_be_sasl_mech, CheckSASLAuth};
use crate::libpq::scram::pg_be_scram_mech;
use crate::tcop::backend_startup::{am_db_walsender, am_walsender};

use core::ffi::CStr;

// AuthRequest is the wire-level int passed to sendAuthRequest.
type AuthRequest = c_int;

// C `EOF` from <stdio.h>; not defined as a Rust constant in the port.
const EOF: c_int = -1;

// PG_MAX_AUTH_TOKEN_LENGTH from libpq/auth.h.
const PG_MAX_AUTH_TOKEN_LENGTH: c_int = 65535;

// ---------------------------------------------------------------------------
// errcodes (utils/errcodes.h, not yet ported). Folded into ereport! comments.
// TODO(pg-port): import from generated errcodes once available.
// ---------------------------------------------------------------------------
#[allow(non_upper_case_globals)]
const ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION: c_int = 0;
#[allow(non_upper_case_globals)]
const ERRCODE_INVALID_PASSWORD: c_int = 0;
#[allow(non_upper_case_globals)]
const ERRCODE_CONFIG_FILE_ERROR: c_int = 0;
#[allow(non_upper_case_globals)]
const ERRCODE_PROTOCOL_VIOLATION: c_int = 0;
#[allow(non_upper_case_globals)]
const ERRCODE_OUT_OF_MEMORY: c_int = 0;
#[allow(non_upper_case_globals)]
const ERRCODE_FEATURE_NOT_SUPPORTED: c_int = 0;
#[allow(non_upper_case_globals)]
const ERRCODE_INVALID_ROLE_SPECIFICATION: c_int = 0;

// ---------------------------------------------------------------------------
// log_connections flags (utils/misc/guc / backend_startup.h, not yet ported).
// TODO(pg-port): real log_connections GUC + LOG_CONNECTION_* bits.
// ---------------------------------------------------------------------------
#[allow(non_upper_case_globals)]
const LOG_CONNECTION_AUTHENTICATION: c_int = 1;
extern "C" {
    static mut log_connections: c_int;
}

// ---------------------------------------------------------------------------
// UserAuth enum (libpq/hba.h, not yet ported).
// TODO(pg-port): dedup with hba.h UserAuth enum.
// ---------------------------------------------------------------------------
pub type UserAuth = c_int;
pub const uaReject: UserAuth = 0;
pub const uaImplicitReject: UserAuth = 1;
pub const uaTrust: UserAuth = 2;
pub const uaIdent: UserAuth = 3;
pub const uaPassword: UserAuth = 4;
pub const uaMD5: UserAuth = 5;
pub const uaSCRAM: UserAuth = 6;
pub const uaGSS: UserAuth = 7;
pub const uaSSPI: UserAuth = 8;
pub const uaPAM: UserAuth = 9;
pub const uaBSD: UserAuth = 10;
pub const uaLDAP: UserAuth = 11;
pub const uaCert: UserAuth = 12;
pub const uaRADIUS: UserAuth = 13;
pub const uaPeer: UserAuth = 14;
pub const uaOAuth: UserAuth = 15;

// ClientCertMode (libpq/hba.h).
pub type ClientCertMode = c_int;
pub const clientCertOff: ClientCertMode = 0;
pub const clientCertCA: ClientCertMode = 1;
pub const clientCertFull: ClientCertMode = 2;

// ClientCertName (libpq/hba.h).
pub type ClientCertName = c_int;
pub const clientCertCN: ClientCertName = 0;
pub const clientCertDN: ClientCertName = 1;

// ConnType (libpq/hba.h).
pub type ConnType = c_int;
pub const ctLocal: ConnType = 0;

// ---------------------------------------------------------------------------
// HbaLine: canonical def lives in libpq::hba (hba.h home). Was a partial local copy here.
pub use crate::libpq::hba::{HbaLine, SockAddr};

// ---------------------------------------------------------------------------
// Kerberos and GSSAPI GUCs (defined here in C, kept as module statics).
// ---------------------------------------------------------------------------
#[no_mangle]
pub static mut pg_krb_server_keyfile: *mut c_char = null_mut();
#[no_mangle]
pub static mut pg_krb_caseins_users: bool = false;
#[no_mangle]
pub static mut pg_gss_accept_delegation: bool = false;

/*
 * This hook allows plugins to get control following client authentication,
 * but before the user has been informed about the results.  It could be used
 * to record login events, insert a delay after failed authentication, etc.
 */
pub type ClientAuthentication_hook_type = Option<unsafe extern "C" fn(*mut Port, c_int)>;
#[no_mangle]
pub static mut ClientAuthentication_hook: ClientAuthentication_hook_type = None;

// Max size of username ident server can return (per RFC 1413).
const IDENT_USERNAME_MAX: usize = 512;

// Standard TCP port number for Ident service.  Assigned by IANA.
const IDENT_PORT: c_int = 113;

// PGSQL_PAM_SERVICE - service name passed to PAM.
const PGSQL_PAM_SERVICE: &CStr = c"postgresql";

// ---------------------------------------------------------------------------
// Dependencies living in OTHER .c files, stubbed as TODO(pg-port).
// ---------------------------------------------------------------------------

// libpq/hba.c
unsafe fn hba_getauthmethod(_port: *mut Port) {
    crate::libpq::hba::hba_getauthmethod(_port as _)
}
unsafe fn hba_authname(auth_method: UserAuth) -> *const c_char { crate::libpq::hba::hba_authname(auth_method) }
unsafe fn check_usermap(
    usermap_name: *const c_char,
    pg_user: *const c_char,
    system_user: *const c_char,
    case_insensitive: bool,
) -> c_int { crate::libpq::hba::check_usermap(usermap_name as _, pg_user as _, system_user as _, case_insensitive) }

// common/ip.c
unsafe fn pg_getnameinfo_all(
    addr: *const SockAddrStorage,
    salen: c_int,
    node: *mut c_char,
    nodelen: c_int,
    service: *mut c_char,
    servicelen: c_int,
    flags: c_int,
) -> c_int { crate::common::ip::pg_getnameinfo_all(addr as _, salen as _, node as _, nodelen as _, service as _, servicelen as _, flags as _) }
unsafe fn pg_getaddrinfo_all(
    hostname: *const c_char,
    servname: *const c_char,
    hintp: *const addrinfo,
    result: *mut *mut addrinfo,
) -> c_int { crate::common::ip::pg_getaddrinfo_all(hostname as _, servname as _, hintp as _, result as _) }
unsafe fn pg_freeaddrinfo_all(hint_ai_family: c_int, ai: *mut addrinfo) { crate::common::ip::pg_freeaddrinfo_all(hint_ai_family as _, ai as _) }

// storage/ipc/ipc.c
unsafe fn proc_exit(code: c_int) -> ! { crate::storage::ipc::ipc::proc_exit(code as _) }

// utils/mb / utils/error - miscadmin.h CHECK_FOR_INTERRUPTS.
unsafe fn CHECK_FOR_INTERRUPTS() {
    // TODO(pg-port): miscadmin.h CHECK_FOR_INTERRUPTS
}

// utils/error/elog.c - psprintf / errdetail_log / gettext helpers.
unsafe fn psprintf(_fmt: *const c_char) -> *mut c_char {
    unimplemented!() // TODO(pg-port): utils/error/elog.c (variadic psprintf)
}
unsafe fn gettext_noop(s: *const c_char) -> *const c_char {
    s
}

// utils/error - port/secure random.
unsafe fn pg_strong_random(buf: *mut c_void, len: Size) -> bool { crate::port::pg_strong_random::pg_strong_random(buf as _, len as _) }

// common/md5.c
unsafe fn pg_md5_binary(
    buff: *const c_void,
    len: Size,
    hexsum: *mut c_void,
    errstr: *mut *const c_char,
) -> bool { crate::common::md5_common::pg_md5_binary(buff as _, len, hexsum as _, errstr as _) }

// libc / port routines used verbatim from the C source.
extern "C" {
    fn strlen(s: *const c_char) -> Size;
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn strncmp(s1: *const c_char, s2: *const c_char, n: Size) -> c_int;
    fn strcspn(s: *const c_char, reject: *const c_char) -> Size;
    fn strchr(s: *const c_char, c: c_int) -> *mut c_char;
    fn strcpy(dst: *mut c_char, src: *const c_char) -> *mut c_char;
    fn memcpy(dst: *mut c_void, src: *const c_void, n: Size) -> *mut c_void;
    fn memcmp(s1: *const c_void, s2: *const c_void, n: Size) -> c_int;
    fn memset(s: *mut c_void, c: c_int, n: Size) -> *mut c_void;
    fn snprintf(s: *mut c_char, n: Size, fmt: *const c_char, ...) -> c_int;
    fn atoi(s: *const c_char) -> c_int;
    fn socket(domain: c_int, ty: c_int, protocol: c_int) -> c_int;
    fn bind(sockfd: c_int, addr: *const c_void, addrlen: c_uint) -> c_int;
    fn connect(sockfd: c_int, addr: *const c_void, addrlen: c_uint) -> c_int;
    fn send(sockfd: c_int, buf: *const c_void, len: Size, flags: c_int) -> isize;
    fn recv(sockfd: c_int, buf: *mut c_void, len: Size, flags: c_int) -> isize;
    fn sendto(
        sockfd: c_int,
        buf: *const c_void,
        len: Size,
        flags: c_int,
        dest_addr: *const c_void,
        addrlen: c_uint,
    ) -> isize;
    fn recvfrom(
        sockfd: c_int,
        buf: *mut c_void,
        len: Size,
        flags: c_int,
        src_addr: *mut c_void,
        addrlen: *mut c_uint,
    ) -> isize;
    fn closesocket(fd: c_int) -> c_int;
    fn getpeereid(sockfd: c_int, euid: *mut uid_t, egid: *mut gid_t) -> c_int;
    fn getpwuid_r(
        uid: uid_t,
        pwd: *mut passwd,
        buf: *mut c_char,
        buflen: Size,
        result: *mut *mut passwd,
    ) -> c_int;
    fn pg_isblank(c: c_char) -> bool;
    fn pg_strcasecmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn gai_strerror(errcode: c_int) -> *const c_char;
    fn select(
        nfds: c_int,
        readfds: *mut fd_set,
        writefds: *mut fd_set,
        exceptfds: *mut fd_set,
        timeout: *mut timeval,
    ) -> c_int;
    fn gettimeofday(tv: *mut timeval, tz: *mut c_void) -> c_int;
    fn pg_hton16(x: uint16) -> uint16;
    fn pg_hton32(x: uint32) -> uint32;
    fn pg_ntoh16(x: uint16) -> uint16;
    // Darwin errno location.
    fn __error() -> *mut c_int;
}

// SockAddrStorage matches libpq-be.h SockAddr (struct sockaddr_storage + salen).
#[repr(C)]
pub struct SockAddrStorage {
    pub addr: SockAddrData,
    pub salen: c_int,
}
#[repr(C)]
pub struct SockAddrData {
    pub ss_family: c_ushort,
    _pad: [u8; 126],
}
type c_ushort = u16;

#[repr(C)]
pub struct addrinfo {
    pub ai_flags: c_int,
    pub ai_family: c_int,
    pub ai_socktype: c_int,
    pub ai_protocol: c_int,
    pub ai_addrlen: c_uint,
    pub ai_canonname: *mut c_char,
    pub ai_addr: *mut c_void,
    pub ai_next: *mut addrinfo,
}

type uid_t = c_uint;
type gid_t = c_uint;

#[repr(C)]
pub struct passwd {
    pub pw_name: *mut c_char,
    _opaque: [u8; 64],
}

#[repr(C)]
pub struct fd_set {
    _opaque: [u8; 128],
}

#[repr(C)]
pub struct timeval {
    pub tv_sec: c_long,
    pub tv_usec: c_long,
}

// AI_NUMERICHOST, NI_*, SOCK_STREAM, SOCK_DGRAM, AF_*, EINTR, ENOSYS from
// <netdb.h>/<sys/socket.h>/<errno.h>.
const AI_NUMERICHOST: c_int = 4;
const NI_NUMERICHOST: c_int = 2;
const NI_NUMERICSERV: c_int = 8;
const NI_MAXHOST: usize = 1025;
const NI_MAXSERV: usize = 32;
const SOCK_STREAM: c_int = 1;
const SOCK_DGRAM: c_int = 2;
const AF_UNSPEC: c_int = 0;
const AF_INET6: c_int = 30;
const EINTR: c_int = 4;
const ENOSYS: c_int = 78;
const PGINVALID_SOCKET: c_int = -1;

// errno accessor for Darwin.
unsafe fn errno() -> c_int {
    *__error()
}
unsafe fn set_errno(v: c_int) {
    *__error() = v;
}

/*
 * Tell the user the authentication failed, but not (much about) why.
 *
 * There is a tradeoff here between security concerns and making life
 * unnecessarily difficult for legitimate users.  We would not, for example,
 * want to report the password we were expecting to receive...
 * But it seems useful to report the username and authorization method
 * in use, and these are items that must be presumed known to an attacker
 * anyway.
 * Note that many sorts of failure report additional information in the
 * postmaster log, which we hope is only readable by good guys.  In
 * particular, if logdetail isn't NULL, we send that string to the log.
 */
unsafe fn auth_failed(port: *mut Port, status: c_int, mut logdetail: *const c_char) {
    let errstr: *const c_char;
    let cdetail: *mut c_char;
    let mut errcode_return: c_int = ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION;

    /*
     * If we failed due to EOF from client, just quit; there's no point in
     * trying to send a message to the client, and not much point in logging
     * the failure in the postmaster log.  (Logging the failure might be
     * desirable, were it not for the fact that libpq closes the connection
     * unceremoniously if challenged for a password when it hasn't got one to
     * send.  We'll get a useless log entry for every psql connection under
     * password auth, even if it's perfectly successful, if we log STATUS_EOF
     * events.)
     */
    if status == STATUS_EOF {
        proc_exit(0);
    }

    match (*(*port).hba).auth_method {
        uaReject | uaImplicitReject => {
            errstr = gettext_noop(
                c"authentication failed for user \"%s\": host rejected".as_ptr(),
            );
        }
        uaTrust => {
            errstr = gettext_noop(c"\"trust\" authentication failed for user \"%s\"".as_ptr());
        }
        uaIdent => {
            errstr = gettext_noop(c"Ident authentication failed for user \"%s\"".as_ptr());
        }
        uaPeer => {
            errstr = gettext_noop(c"Peer authentication failed for user \"%s\"".as_ptr());
        }
        uaPassword | uaMD5 | uaSCRAM => {
            errstr =
                gettext_noop(c"password authentication failed for user \"%s\"".as_ptr());
            /* We use it to indicate if a .pgpass password failed. */
            errcode_return = ERRCODE_INVALID_PASSWORD;
        }
        uaGSS => {
            errstr = gettext_noop(c"GSSAPI authentication failed for user \"%s\"".as_ptr());
        }
        uaSSPI => {
            errstr = gettext_noop(c"SSPI authentication failed for user \"%s\"".as_ptr());
        }
        uaPAM => {
            errstr = gettext_noop(c"PAM authentication failed for user \"%s\"".as_ptr());
        }
        uaBSD => {
            errstr = gettext_noop(c"BSD authentication failed for user \"%s\"".as_ptr());
        }
        uaLDAP => {
            errstr = gettext_noop(c"LDAP authentication failed for user \"%s\"".as_ptr());
        }
        uaCert => {
            errstr =
                gettext_noop(c"certificate authentication failed for user \"%s\"".as_ptr());
        }
        uaRADIUS => {
            errstr = gettext_noop(c"RADIUS authentication failed for user \"%s\"".as_ptr());
        }
        uaOAuth => {
            errstr =
                gettext_noop(c"OAuth bearer authentication failed for user \"%s\"".as_ptr());
        }
        _ => {
            errstr = gettext_noop(
                c"authentication failed for user \"%s\": invalid authentication method"
                    .as_ptr(),
            );
        }
    }

    // C also: psprintf(_("Connection matched file \"%s\" line %d: \"%s\""),
    //   port->hba->sourcefile, port->hba->linenumber, port->hba->rawline);
    cdetail = psprintf(c"Connection matched file".as_ptr());
    if !logdetail.is_null() {
        // C also: logdetail = psprintf("%s\n%s", logdetail, cdetail);
        logdetail = psprintf(logdetail);
    } else {
        logdetail = cdetail;
    }

    let _ = errcode(errcode_return);
    // C also: errmsg(errstr, port->user_name);
    //   logdetail ? errdetail_log("%s", logdetail) : 0
    ereport!(
        FATAL,
        errmsg!(
            "{}",
            CStr::from_ptr(errstr).to_string_lossy()
        )
    );

    /* doesn't return */
}

/*
 * Sets the authenticated identity for the current user.  The provided string
 * will be stored into MyClientConnectionInfo, alongside the current HBA
 * method in use.  The ID will be logged if log_connections has the
 * 'authentication' option specified.
 *
 * Auth methods should call this routine exactly once, as soon as the user is
 * successfully authenticated, even if they have reasons to know that
 * authorization will fail later.
 *
 * The provided string will be copied into TopMemoryContext, to match the
 * lifetime of MyClientConnectionInfo, so it is safe to pass a string that is
 * managed by an external library.
 */
pub unsafe fn set_authn_id(port: *mut Port, id: *const c_char) {
    Assert!(!id.is_null());

    if !MyClientConnectionInfo.authn_id.is_null() {
        /*
         * An existing authn_id should never be overwritten; that means two
         * authentication providers are fighting (or one is fighting itself).
         * Don't leak any authn details to the client, but don't let the
         * connection continue, either.
         */
        // C also: errdetail_log("previous identifier: \"%s\"; new identifier: \"%s\"",
        //   MyClientConnectionInfo.authn_id, id)
        ereport!(FATAL, errmsg!("authentication identifier set more than once"));
    }

    MyClientConnectionInfo.authn_id = MemoryContextStrdup(TopMemoryContext, id);
    MyClientConnectionInfo.auth_method = (*(*port).hba).auth_method;

    if log_connections & LOG_CONNECTION_AUTHENTICATION != 0 {
        ereport!(
            LOG,
            errmsg!(
                "connection authenticated: identity=\"{}\" method={} ({}:{})",
                CStr::from_ptr(MyClientConnectionInfo.authn_id).to_string_lossy(),
                CStr::from_ptr(hba_authname(MyClientConnectionInfo.auth_method))
                    .to_string_lossy(),
                CStr::from_ptr((*(*port).hba).sourcefile).to_string_lossy(),
                (*(*port).hba).linenumber
            )
        );
    }
}

// MemoryContextStrdup lives in utils/mmgr/mcxt.c.
// TODO(pg-port): real MemoryContextStrdup (utils/mmgr/mcxt.c).
unsafe fn MemoryContextStrdup(_ctx: MemoryContext, _s: *const c_char) -> *mut c_char {
    unimplemented!() // TODO(pg-port): utils/mmgr/mcxt.c
}

/*
 * Client authentication starts here.  If there is an error, this
 * function does not return and the backend process is terminated.
 */
pub unsafe fn ClientAuthentication(port: *mut Port) {
    let mut status: c_int = STATUS_ERROR;
    let mut logdetail: *const c_char = null();

    /*
     * Get the authentication method to use for this frontend/database
     * combination.  Note: we do not parse the file at this point; this has
     * already been done elsewhere.  hba.c dropped an error message into the
     * server logfile if parsing the hba config file failed.
     */
    hba_getauthmethod(port);

    CHECK_FOR_INTERRUPTS();

    /*
     * This is the first point where we have access to the hba record for the
     * current connection, so perform any verifications based on the hba
     * options field that should be done *before* the authentication here.
     */
    if (*(*port).hba).clientcert != clientCertOff {
        /* If we haven't loaded a root certificate store, fail */
        if !secure_loaded_verify_locations() {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR)
            ereport!(
                FATAL,
                errmsg!(
                    "client certificates can only be checked if a root certificate store is available"
                )
            );
        }

        /*
         * If we loaded a root certificate store, and if a certificate is
         * present on the client, then it has been verified against our root
         * certificate store, and the connection would have been aborted
         * already if it didn't verify ok.
         */
        if !(*port).peer_cert_valid {
            // C also: errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION)
            ereport!(
                FATAL,
                errmsg!("connection requires a valid client certificate")
            );
        }
    }

    /*
     * Now proceed to do the actual authentication check
     */
    match (*(*port).hba).auth_method {
        uaReject => {
            /*
             * An explicit "reject" entry in pg_hba.conf.  This report exposes
             * the fact that there's an explicit reject entry, which is
             * perhaps not so desirable from a security standpoint; but the
             * message for an implicit reject could confuse the DBA a lot when
             * the true situation is a match to an explicit reject.  And we
             * don't want to change the message for an implicit reject.  As
             * noted below, the additional information shown here doesn't
             * expose anything not known to an attacker.
             */
            let mut hostinfo: [c_char; NI_MAXHOST] = [0; NI_MAXHOST];
            let encryption_state: *const c_char;

            pg_getnameinfo_all(
                &(*port).raddr as *const _ as *const SockAddrStorage,
                salen_of(&(*port).raddr),
                hostinfo.as_mut_ptr(),
                core::mem::size_of_val(&hostinfo) as c_int,
                null_mut(),
                0,
                NI_NUMERICHOST,
            );

            encryption_state = if (*port).ssl_in_use {
                c"SSL encryption".as_ptr()
            } else {
                c"no encryption".as_ptr()
            };

            if am_walsender && !am_db_walsender {
                /* translator: last %s describes encryption state */
                // C also: errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION)
                ereport!(
                    FATAL,
                    errmsg!(
                        "pg_hba.conf rejects replication connection for host \"{}\", user \"{}\", {}",
                        CStr::from_ptr(hostinfo.as_ptr()).to_string_lossy(),
                        CStr::from_ptr((*port).user_name).to_string_lossy(),
                        CStr::from_ptr(encryption_state).to_string_lossy()
                    )
                );
            } else {
                /* translator: last %s describes encryption state */
                // C also: errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION)
                ereport!(
                    FATAL,
                    errmsg!(
                        "pg_hba.conf rejects connection for host \"{}\", user \"{}\", database \"{}\", {}",
                        CStr::from_ptr(hostinfo.as_ptr()).to_string_lossy(),
                        CStr::from_ptr((*port).user_name).to_string_lossy(),
                        CStr::from_ptr((*port).database_name).to_string_lossy(),
                        CStr::from_ptr(encryption_state).to_string_lossy()
                    )
                );
            }
        }

        uaImplicitReject => {
            /*
             * No matching entry, so tell the user we fell through.
             *
             * NOTE: the extra info reported here is not a security breach,
             * because all that info is known at the frontend and must be
             * assumed known to bad guys.  We're merely helping out the less
             * clueful good guys.
             */
            let mut hostinfo: [c_char; NI_MAXHOST] = [0; NI_MAXHOST];
            let encryption_state: *const c_char;

            pg_getnameinfo_all(
                &(*port).raddr as *const _ as *const SockAddrStorage,
                salen_of(&(*port).raddr),
                hostinfo.as_mut_ptr(),
                core::mem::size_of_val(&hostinfo) as c_int,
                null_mut(),
                0,
                NI_NUMERICHOST,
            );

            encryption_state = if (*port).ssl_in_use {
                c"SSL encryption".as_ptr()
            } else {
                c"no encryption".as_ptr()
            };

            // C also: HOSTNAME_LOOKUP_DETAIL(port) folds into errdetail_log
            //   describing forward/reverse client hostname resolution status.
            if am_walsender && !am_db_walsender {
                /* translator: last %s describes encryption state */
                // C also: errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION)
                ereport!(
                    FATAL,
                    errmsg!(
                        "no pg_hba.conf entry for replication connection from host \"{}\", user \"{}\", {}",
                        CStr::from_ptr(hostinfo.as_ptr()).to_string_lossy(),
                        CStr::from_ptr((*port).user_name).to_string_lossy(),
                        CStr::from_ptr(encryption_state).to_string_lossy()
                    )
                );
            } else {
                /* translator: last %s describes encryption state */
                // C also: errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION)
                ereport!(
                    FATAL,
                    errmsg!(
                        "no pg_hba.conf entry for host \"{}\", user \"{}\", database \"{}\", {}",
                        CStr::from_ptr(hostinfo.as_ptr()).to_string_lossy(),
                        CStr::from_ptr((*port).user_name).to_string_lossy(),
                        CStr::from_ptr((*port).database_name).to_string_lossy(),
                        CStr::from_ptr(encryption_state).to_string_lossy()
                    )
                );
            }
        }

        uaGSS => {
            // #ifdef ENABLE_GSS
            /* We might or might not have the gss workspace already */
            if (*port).gss.is_null() {
                (*port).gss =
                    MemoryContextAllocZero(TopMemoryContext, core::mem::size_of::<pg_gssinfo>())
                        as *mut pg_gssinfo;
            }
            (*(*port).gss).auth = true;

            /*
             * If GSS state was set up while enabling encryption, we can just
             * check the client's principal.  Otherwise, ask for it.
             */
            if (*(*port).gss).enc {
                status = pg_GSS_checkauth(port);
            } else {
                sendAuthRequest(port, AUTH_REQ_GSS, null(), 0);
                status = pg_GSS_recvauth(port);
            }
            // #else Assert(false); #endif
        }

        uaSSPI => {
            // #ifdef ENABLE_SSPI
            if (*port).gss.is_null() {
                (*port).gss =
                    MemoryContextAllocZero(TopMemoryContext, core::mem::size_of::<pg_gssinfo>())
                        as *mut pg_gssinfo;
            }
            sendAuthRequest(port, AUTH_REQ_SSPI, null(), 0);
            status = pg_SSPI_recvauth(port);
            // #else Assert(false); #endif
        }

        uaPeer => {
            status = auth_peer(port);
        }

        uaIdent => {
            status = ident_inet(port);
        }

        uaMD5 | uaSCRAM => {
            status = CheckPWChallengeAuth(port, &mut logdetail);
        }

        uaPassword => {
            status = CheckPasswordAuth(port, &mut logdetail);
        }

        uaPAM => {
            // #ifdef USE_PAM
            status = CheckPAMAuth(port, (*port).user_name, c"".as_ptr());
            // #else Assert(false); #endif
        }

        uaBSD => {
            // #ifdef USE_BSD_AUTH
            status = CheckBSDAuth(port, (*port).user_name);
            // #else Assert(false); #endif
        }

        uaLDAP => {
            // #ifdef USE_LDAP
            status = CheckLDAPAuth(port);
            // #else Assert(false); #endif
        }
        uaRADIUS => {
            status = CheckRADIUSAuth(port);
        }
        uaCert => {
            /* uaCert will be treated as if clientcert=verify-full (uaTrust) */
            status = STATUS_OK;
        }
        uaTrust => {
            status = STATUS_OK;
        }
        uaOAuth => {
            status = CheckSASLAuth(
                &pg_be_oauth_mech as *const _ as *const pg_be_sasl_mech,
                port as *mut c_void,
                null_mut(),
                null_mut(),
            );
        }
        _ => {}
    }

    if (status == STATUS_OK && (*(*port).hba).clientcert == clientCertFull)
        || (*(*port).hba).auth_method == uaCert
    {
        /*
         * Make sure we only check the certificate if we use the cert method
         * or verify-full option.
         */
        // #ifdef USE_SSL
        status = CheckCertAuth(port);
        // #else Assert(false); #endif
    }

    if (log_connections & LOG_CONNECTION_AUTHENTICATION != 0)
        && status == STATUS_OK
        && MyClientConnectionInfo.authn_id.is_null()
    {
        /*
         * Normally, if log_connections is set, the call to set_authn_id()
         * will log the connection.  However, if that function is never
         * called, perhaps because the trust method is in use, then we handle
         * the logging here instead.
         */
        ereport!(
            LOG,
            errmsg!(
                "connection authenticated: user=\"{}\" method={} ({}:{})",
                CStr::from_ptr((*port).user_name).to_string_lossy(),
                CStr::from_ptr(hba_authname((*(*port).hba).auth_method)).to_string_lossy(),
                CStr::from_ptr((*(*port).hba).sourcefile).to_string_lossy(),
                (*(*port).hba).linenumber
            )
        );
    }

    if let Some(hook) = ClientAuthentication_hook {
        hook(port, status);
    }

    if status == STATUS_OK {
        sendAuthRequest(port, AUTH_REQ_OK, null(), 0);
    } else {
        auth_failed(port, status, logdetail);
    }
}

// Helper to read SockAddr.salen field (pqcomm.h SockAddr).
unsafe fn salen_of(sa: *const SockAddr) -> c_int {
    (*sa).salen as c_int
}

// pg_gssinfo fields accessed by the GSS path (pg-gssapi.h, not ported).
// TODO(pg-port): dedup with pg-gssapi.h pg_gssinfo.
#[repr(C)]
pub struct pg_gssinfo {
    pub auth: bool,
    pub enc: bool,
    pub cred: gss_cred_id_t,
    pub ctx: gss_ctx_id_t,
    pub name: gss_name_t,
    pub outbuf: gss_buffer_desc,
    pub princ: *mut c_char,
    pub delegated_creds: bool,
}
type gss_cred_id_t = *mut c_void;
type gss_ctx_id_t = *mut c_void;
type gss_name_t = *mut c_void;
#[repr(C)]
pub struct gss_buffer_desc {
    pub length: Size,
    pub value: *mut c_void,
}

/*
 * Send an authentication request packet to the frontend.
 */
pub unsafe fn sendAuthRequest(
    _port: *mut Port,
    areq: AuthRequest,
    extradata: *const c_void,
    extralen: c_int,
) {
    let mut buf: StringInfoData = core::mem::zeroed();

    CHECK_FOR_INTERRUPTS();

    pq_beginmessage(&mut buf, PqMsg_AuthenticationRequest as c_char);
    pq_sendint32(&mut buf, areq as int32 as uint32);
    if extralen > 0 {
        pq_sendbytes(&mut buf, extradata, extralen);
    }

    pq_endmessage(&mut buf);

    /*
     * Flush message so client will see it, except for AUTH_REQ_OK and
     * AUTH_REQ_SASL_FIN, which need not be sent until we are ready for
     * queries.
     */
    if areq != AUTH_REQ_OK && areq != AUTH_REQ_SASL_FIN {
        pq_flush();
    }

    CHECK_FOR_INTERRUPTS();
}

/*
 * Collect password response packet from frontend.
 *
 * Returns NULL if couldn't get password, else palloc'd string.
 */
unsafe fn recv_password_packet(_port: *mut Port) -> *mut c_char {
    let mut buf: StringInfoData = core::mem::zeroed();
    let mtype: c_int;

    pq_startmsgread();

    /* Expect 'p' message type */
    mtype = pq_getbyte();
    if mtype != PqMsg_PasswordMessage as c_int {
        /*
         * If the client just disconnects without offering a password, don't
         * make a log entry.  This is legal per protocol spec and in fact
         * commonly done by psql, so complaining just clutters the log.
         */
        if mtype != EOF {
            // C also: errcode(ERRCODE_PROTOCOL_VIOLATION)
            ereport!(
                ERROR,
                errmsg!("expected password response, got message type {}", mtype)
            );
        }
        return null_mut(); /* EOF or bad message type */
    }

    initStringInfo(&mut buf);
    if pq_getmessage(&mut buf, PG_MAX_AUTH_TOKEN_LENGTH) != 0 {
        /* receive password */
        /* EOF - pq_getmessage already logged a suitable message */
        pfree(buf.data as *mut c_void);
        return null_mut();
    }

    /*
     * Apply sanity check: password packet length should agree with length of
     * contained string.  Note it is safe to use strlen here because
     * StringInfo is guaranteed to have an appended '\0'.
     */
    if strlen(buf.data) + 1 != buf.len as Size {
        // C also: errcode(ERRCODE_PROTOCOL_VIOLATION)
        ereport!(ERROR, errmsg!("invalid password packet size"));
    }

    /*
     * Don't allow an empty password. Libpq treats an empty password the same
     * as no password at all, and won't even try to authenticate. But other
     * clients might, so allowing it would be confusing.
     *
     * Note that this only catches an empty password sent by the client in
     * plaintext. There's also a check in CREATE/ALTER USER that prevents an
     * empty string from being stored as a user's password in the first place.
     * We rely on that for MD5 and SCRAM authentication, but we still need
     * this check here, to prevent an empty password from being used with
     * authentication methods that check the password against an external
     * system, like PAM, LDAP and RADIUS.
     */
    if buf.len == 1 {
        // C also: errcode(ERRCODE_INVALID_PASSWORD)
        ereport!(ERROR, errmsg!("empty password returned by client"));
    }

    /* Do not echo password to logs, for security. */
    elog!(DEBUG5, "received password packet");

    /*
     * Return the received string.  Note we do not attempt to do any
     * character-set conversion on it; since we don't yet know the client's
     * encoding, there wouldn't be much point.
     */
    buf.data
}

/*
 * Plaintext password authentication.
 */
unsafe fn CheckPasswordAuth(port: *mut Port, logdetail: *mut *const c_char) -> c_int {
    let passwd: *mut c_char;
    let result: c_int;
    let shadow_pass: *mut c_char;

    sendAuthRequest(port, AUTH_REQ_PASSWORD, null(), 0);

    passwd = recv_password_packet(port);
    if passwd.is_null() {
        return STATUS_EOF; /* client wouldn't send password */
    }

    shadow_pass = get_role_password((*port).user_name, logdetail);
    if !shadow_pass.is_null() {
        result = plain_crypt_verify((*port).user_name, shadow_pass, passwd, logdetail);
    } else {
        result = STATUS_ERROR;
    }

    if !shadow_pass.is_null() {
        pfree(shadow_pass as *mut c_void);
    }
    pfree(passwd as *mut c_void);

    if result == STATUS_OK {
        set_authn_id(port, (*port).user_name);
    }

    result
}

/*
 * MD5 and SCRAM authentication.
 */
unsafe fn CheckPWChallengeAuth(port: *mut Port, logdetail: *mut *const c_char) -> c_int {
    let auth_result: c_int;
    let shadow_pass: *mut c_char;
    let pwtype: PasswordType;

    Assert!(
        (*(*port).hba).auth_method == uaSCRAM || (*(*port).hba).auth_method == uaMD5
    );

    /* First look up the user's password. */
    shadow_pass = get_role_password((*port).user_name, logdetail);

    /*
     * If the user does not exist, or has no password or it's expired, we
     * still go through the motions of authentication, to avoid revealing to
     * the client that the user didn't exist.  If 'md5' is allowed, we choose
     * whether to use 'md5' or 'scram-sha-256' authentication based on current
     * password_encryption setting.  The idea is that most genuine users
     * probably have a password of that type, and if we pretend that this user
     * had a password of that type, too, it "blends in" best.
     */
    if shadow_pass.is_null() {
        pwtype = Password_encryption_as_type();
    } else {
        pwtype = get_password_type(shadow_pass);
    }

    /*
     * If 'md5' authentication is allowed, decide whether to perform 'md5' or
     * 'scram-sha-256' authentication based on the type of password the user
     * has.  If it's an MD5 hash, we must do MD5 authentication, and if it's a
     * SCRAM secret, we must do SCRAM authentication.
     *
     * If MD5 authentication is not allowed, always use SCRAM.  If the user
     * had an MD5 password, CheckSASLAuth() with the SCRAM mechanism will
     * fail.
     */
    if (*(*port).hba).auth_method == uaMD5 && pwtype == PASSWORD_TYPE_MD5 {
        auth_result = CheckMD5Auth(port, shadow_pass, logdetail);
    } else {
        auth_result = CheckSASLAuth(
            &pg_be_scram_mech as *const _ as *const pg_be_sasl_mech,
            port as *mut c_void,
            shadow_pass,
            logdetail,
        );
    }

    if !shadow_pass.is_null() {
        pfree(shadow_pass as *mut c_void);
    } else {
        /*
         * If get_role_password() returned error, authentication better not
         * have succeeded.
         */
        Assert!(auth_result != STATUS_OK);
    }

    if auth_result == STATUS_OK {
        set_authn_id(port, (*port).user_name);
    }

    auth_result
}

unsafe fn CheckMD5Auth(
    port: *mut Port,
    shadow_pass: *mut c_char,
    logdetail: *mut *const c_char,
) -> c_int {
    let mut md5Salt: [uint8; 4] = [0; 4]; /* Password salt */
    let passwd: *mut c_char;
    let result: c_int;

    /* include the salt to use for computing the response */
    if !pg_strong_random(md5Salt.as_mut_ptr() as *mut c_void, 4) {
        ereport!(LOG, errmsg!("could not generate random MD5 salt"));
        return STATUS_ERROR;
    }

    sendAuthRequest(port, AUTH_REQ_MD5, md5Salt.as_ptr() as *const c_void, 4);

    passwd = recv_password_packet(port);
    if passwd.is_null() {
        return STATUS_EOF; /* client wouldn't send password */
    }

    if !shadow_pass.is_null() {
        result = md5_crypt_verify(
            (*port).user_name,
            shadow_pass,
            passwd,
            md5Salt.as_ptr(),
            4,
            logdetail,
        );
    } else {
        result = STATUS_ERROR;
    }

    pfree(passwd as *mut c_void);

    result
}

// commands/user.c Password_encryption GUC is a c_int; reinterpret as PasswordType.
unsafe fn Password_encryption_as_type() -> PasswordType {
    core::mem::transmute::<c_int, PasswordType>(Password_encryption)
}

// commands/user.c
extern "C" {
    static mut Password_encryption: c_int;
}

/*----------------------------------------------------------------
 * GSSAPI authentication system
 *----------------------------------------------------------------
 */
// #ifdef ENABLE_GSS
unsafe fn pg_GSS_recvauth(port: *mut Port) -> c_int {
    let mut maj_stat: OM_uint32;
    let mut min_stat: OM_uint32 = 0;
    let mut lmin_s: OM_uint32 = 0;
    let mut gflags: OM_uint32 = 0;
    let mtype: c_int;
    let mut buf: StringInfoData = core::mem::zeroed();
    let mut gbuf: gss_buffer_desc = core::mem::zeroed();
    let mut delegated_creds: gss_cred_id_t;

    /*
     * Use the configured keytab, if there is one.  As we now require MIT
     * Kerberos, we might consider using the credential store extensions in
     * the future instead of the environment variable.
     */
    if !pg_krb_server_keyfile.is_null() && *pg_krb_server_keyfile != 0 {
        if setenv(c"KRB5_KTNAME".as_ptr(), pg_krb_server_keyfile, 1) != 0 {
            /* The only likely failure cause is OOM, so use that errcode */
            // C also: errcode(ERRCODE_OUT_OF_MEMORY)
            ereport!(FATAL, errmsg!("could not set environment: {}", "%m"));
        }
    }

    /*
     * We accept any service principal that's present in our keytab. This
     * increases interoperability between kerberos implementations that see
     * for example case sensitivity differently, while not really opening up
     * any vector of attack.
     */
    (*(*port).gss).cred = GSS_C_NO_CREDENTIAL;

    /*
     * Initialize sequence with an empty context
     */
    (*(*port).gss).ctx = GSS_C_NO_CONTEXT;

    delegated_creds = GSS_C_NO_CREDENTIAL;
    (*(*port).gss).delegated_creds = false;

    /*
     * Loop through GSSAPI message exchange. This exchange can consist of
     * multiple messages sent in both directions. First message is always from
     * the client. All messages from client to server are password packets
     * (type 'p').
     */
    loop {
        pq_startmsgread();

        CHECK_FOR_INTERRUPTS();

        let mtype2 = pq_getbyte();
        if mtype2 != PqMsg_GSSResponse as c_int {
            /* Only log error if client didn't disconnect. */
            if mtype2 != EOF {
                // C also: errcode(ERRCODE_PROTOCOL_VIOLATION)
                ereport!(
                    ERROR,
                    errmsg!("expected GSS response, got message type {}", mtype2)
                );
            }
            return STATUS_ERROR;
        }

        /* Get the actual GSS token */
        initStringInfo(&mut buf);
        if pq_getmessage(&mut buf, PG_MAX_AUTH_TOKEN_LENGTH) != 0 {
            /* EOF - pq_getmessage already logged error */
            pfree(buf.data as *mut c_void);
            return STATUS_ERROR;
        }

        /* Map to GSSAPI style buffer */
        gbuf.length = buf.len as Size;
        gbuf.value = buf.data as *mut c_void;

        elog!(
            DEBUG4,
            "processing received GSS token of length {}",
            gbuf.length as c_uint
        );

        maj_stat = gss_accept_sec_context(
            &mut min_stat,
            &mut (*(*port).gss).ctx,
            (*(*port).gss).cred,
            &mut gbuf,
            GSS_C_NO_CHANNEL_BINDINGS,
            &mut (*(*port).gss).name,
            null_mut(),
            &mut (*(*port).gss).outbuf,
            &mut gflags,
            null_mut(),
            if pg_gss_accept_delegation {
                &mut delegated_creds
            } else {
                null_mut()
            },
        );

        /* gbuf no longer used */
        pfree(buf.data as *mut c_void);

        elog!(
            DEBUG5,
            "gss_accept_sec_context major: {}, minor: {}, outlen: {}, outflags: {:x}",
            maj_stat,
            min_stat,
            (*(*port).gss).outbuf.length as c_uint,
            gflags
        );

        CHECK_FOR_INTERRUPTS();

        if delegated_creds != GSS_C_NO_CREDENTIAL && (gflags & GSS_C_DELEG_FLAG) != 0 {
            pg_store_delegated_credential(delegated_creds);
            (*(*port).gss).delegated_creds = true;
        }

        if (*(*port).gss).outbuf.length != 0 {
            /*
             * Negotiation generated data to be sent to the client.
             */
            elog!(
                DEBUG4,
                "sending GSS response token of length {}",
                (*(*port).gss).outbuf.length as c_uint
            );

            sendAuthRequest(
                port,
                AUTH_REQ_GSS_CONT,
                (*(*port).gss).outbuf.value as *const c_void,
                (*(*port).gss).outbuf.length as c_int,
            );

            gss_release_buffer(&mut lmin_s, &mut (*(*port).gss).outbuf);
        }

        if maj_stat != GSS_S_COMPLETE && maj_stat != GSS_S_CONTINUE_NEEDED {
            gss_delete_sec_context(&mut lmin_s, &mut (*(*port).gss).ctx, GSS_C_NO_BUFFER);
            pg_GSS_error(
                c"accepting GSS security context failed".as_ptr(),
                maj_stat,
                min_stat,
            );
            return STATUS_ERROR;
        }

        if maj_stat == GSS_S_CONTINUE_NEEDED {
            elog!(DEBUG4, "GSS continue needed");
        }

        if maj_stat != GSS_S_CONTINUE_NEEDED {
            break;
        }
    }

    if (*(*port).gss).cred != GSS_C_NO_CREDENTIAL {
        /*
         * Release service principal credentials
         */
        gss_release_cred(&mut min_stat, &mut (*(*port).gss).cred);
    }
    let _ = mtype;
    pg_GSS_checkauth(port)
}

/*
 * Check whether the GSSAPI-authenticated user is allowed to connect as the
 * claimed username.
 */
unsafe fn pg_GSS_checkauth(port: *mut Port) -> c_int {
    let mut ret: c_int;
    let maj_stat: OM_uint32;
    let mut min_stat: OM_uint32 = 0;
    let mut lmin_s: OM_uint32 = 0;
    let mut gbuf: gss_buffer_desc = core::mem::zeroed();
    let princ: *mut c_char;

    /*
     * Get the name of the user that authenticated, and compare it to the pg
     * username that was specified for the connection.
     */
    maj_stat = gss_display_name(&mut min_stat, (*(*port).gss).name, &mut gbuf, null_mut());
    if maj_stat != GSS_S_COMPLETE {
        pg_GSS_error(c"retrieving GSS user name failed".as_ptr(), maj_stat, min_stat);
        return STATUS_ERROR;
    }

    /*
     * gbuf.value might not be null-terminated, so turn it into a regular
     * null-terminated string.
     */
    princ = palloc(gbuf.length + 1) as *mut c_char;
    memcpy(princ as *mut c_void, gbuf.value, gbuf.length);
    *princ.add(gbuf.length) = b'\0' as c_char;
    gss_release_buffer(&mut lmin_s, &mut gbuf);

    /*
     * Copy the original name of the authenticated principal into our backend
     * memory for display later.
     *
     * This is also our authenticated identity.  Set it now, rather than
     * waiting for the usermap check below, because authentication has already
     * succeeded and we want the log file to reflect that.
     */
    (*(*port).gss).princ = MemoryContextStrdup(TopMemoryContext, princ);
    set_authn_id(port, princ);

    /*
     * Split the username at the realm separator
     */
    if !strchr(princ, b'@' as c_int).is_null() {
        let cp: *mut c_char = strchr(princ, b'@' as c_int);

        /*
         * If we are not going to include the realm in the username that is
         * passed to the ident map, destructively modify it here to remove the
         * realm. Then advance past the separator to check the realm.
         */
        if !(*(*port).hba).include_realm {
            *cp = b'\0' as c_char;
        }
        let cp = cp.add(1);

        if !(*(*port).hba).krb_realm.is_null() && strlen((*(*port).hba).krb_realm) != 0 {
            /*
             * Match the realm part of the name first
             */
            if pg_krb_caseins_users {
                ret = pg_strcasecmp((*(*port).hba).krb_realm, cp);
            } else {
                ret = strcmp((*(*port).hba).krb_realm, cp);
            }

            if ret != 0 {
                /* GSS realm does not match */
                elog!(
                    DEBUG2,
                    "GSSAPI realm ({}) and configured realm ({}) don't match",
                    CStr::from_ptr(cp).to_string_lossy(),
                    CStr::from_ptr((*(*port).hba).krb_realm).to_string_lossy()
                );
                pfree(princ as *mut c_void);
                return STATUS_ERROR;
            }
        }
    } else if !(*(*port).hba).krb_realm.is_null() && strlen((*(*port).hba).krb_realm) != 0 {
        elog!(
            DEBUG2,
            "GSSAPI did not return realm but realm matching was requested"
        );
        pfree(princ as *mut c_void);
        return STATUS_ERROR;
    }

    ret = check_usermap(
        (*(*port).hba).usermap,
        (*port).user_name,
        princ,
        pg_krb_caseins_users,
    );

    pfree(princ as *mut c_void);

    ret
}
// #endif /* ENABLE_GSS */

// GSSAPI dependencies (be-gssapi-common.h / MIT Kerberos, not ported).
// TODO(pg-port): real GSSAPI bindings (libpq/be-gssapi-common.c).
type OM_uint32 = u32;
const GSS_C_NO_CREDENTIAL: gss_cred_id_t = null_mut();
const GSS_C_NO_CONTEXT: gss_ctx_id_t = null_mut();
const GSS_C_NO_CHANNEL_BINDINGS: *mut c_void = null_mut();
const GSS_C_NO_BUFFER: *mut gss_buffer_desc = null_mut();
const GSS_C_DELEG_FLAG: OM_uint32 = 1;
const GSS_S_COMPLETE: OM_uint32 = 0;
const GSS_S_CONTINUE_NEEDED: OM_uint32 = 1;
extern "C" {
    fn setenv(name: *const c_char, value: *const c_char, overwrite: c_int) -> c_int;
}
unsafe fn gss_accept_sec_context(
    _min: *mut OM_uint32,
    _ctx: *mut gss_ctx_id_t,
    _cred: gss_cred_id_t,
    _input: *mut gss_buffer_desc,
    _chan: *mut c_void,
    _name: *mut gss_name_t,
    _mech: *mut c_void,
    _output: *mut gss_buffer_desc,
    _flags: *mut OM_uint32,
    _time: *mut OM_uint32,
    _deleg: *mut gss_cred_id_t,
) -> OM_uint32 {
    unimplemented!() // TODO(pg-port): MIT Kerberos GSSAPI
}
unsafe fn gss_display_name(
    _min: *mut OM_uint32,
    _name: gss_name_t,
    _out: *mut gss_buffer_desc,
    _type: *mut c_void,
) -> OM_uint32 {
    unimplemented!() // TODO(pg-port): MIT Kerberos GSSAPI
}
unsafe fn gss_release_buffer(_min: *mut OM_uint32, _buf: *mut gss_buffer_desc) -> OM_uint32 {
    unimplemented!() // TODO(pg-port): MIT Kerberos GSSAPI
}
unsafe fn gss_release_cred(_min: *mut OM_uint32, _cred: *mut gss_cred_id_t) -> OM_uint32 {
    unimplemented!() // TODO(pg-port): MIT Kerberos GSSAPI
}
unsafe fn gss_delete_sec_context(
    _min: *mut OM_uint32,
    _ctx: *mut gss_ctx_id_t,
    _buf: *mut gss_buffer_desc,
) -> OM_uint32 {
    unimplemented!() // TODO(pg-port): MIT Kerberos GSSAPI
}
unsafe fn pg_GSS_error(errmsg: *const c_char, maj_stat: OM_uint32, min_stat: OM_uint32) { crate::libpq::be_gssapi_common::pg_GSS_error(errmsg as _, maj_stat as _, min_stat as _) }
unsafe fn pg_store_delegated_credential(cred: gss_cred_id_t) { crate::libpq::be_gssapi_common::pg_store_delegated_credential(cred as _) }

/*----------------------------------------------------------------
 * SSPI authentication system
 *----------------------------------------------------------------
 */
// #ifdef ENABLE_SSPI

/*
 * Generate an error for SSPI authentication.  The caller should apply
 * _() to errmsg to make it translatable.
 */
unsafe fn pg_SSPI_error(severity: c_int, errmsg_str: *const c_char, r: SECURITY_STATUS) {
    let mut sysmsg: [c_char; 256] = [0; 256];

    if FormatMessage(
        FORMAT_MESSAGE_IGNORE_INSERTS | FORMAT_MESSAGE_FROM_SYSTEM,
        null_mut(),
        r,
        0,
        sysmsg.as_mut_ptr(),
        core::mem::size_of_val(&sysmsg) as DWORD,
        null_mut(),
    ) == 0
    {
        // C also: errmsg_internal("%s", errmsg) + errdetail_internal("SSPI error %x", r)
        ereport!(
            severity,
            errmsg!("{}", CStr::from_ptr(errmsg_str).to_string_lossy())
        );
    } else {
        // C also: errmsg_internal("%s", errmsg) + errdetail_internal("%s (%x)", sysmsg, r)
        ereport!(
            severity,
            errmsg!("{}", CStr::from_ptr(errmsg_str).to_string_lossy())
        );
    }
}

unsafe fn pg_SSPI_recvauth(port: *mut Port) -> c_int {
    let mtype: c_int;
    let mut buf: StringInfoData = core::mem::zeroed();
    let mut r: SECURITY_STATUS;
    let mut sspicred: CredHandle = core::mem::zeroed();
    let mut sspictx: *mut CtxtHandle = null_mut();
    let mut newctx: CtxtHandle = core::mem::zeroed();
    let mut expiry: TimeStamp = core::mem::zeroed();
    let mut contextattr: ULONG = 0;
    let mut inbuf: SecBufferDesc = core::mem::zeroed();
    let mut outbuf: SecBufferDesc = core::mem::zeroed();
    let mut OutBuffers: [SecBuffer; 1] = core::mem::zeroed();
    let mut InBuffers: [SecBuffer; 1] = core::mem::zeroed();
    let mut token: HANDLE = null_mut();
    let tokenuser: *mut TOKEN_USER;
    let mut retlen: DWORD = 0;
    let mut accountname: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut domainname: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut accountnamesize: DWORD = core::mem::size_of_val(&accountname) as DWORD;
    let mut domainnamesize: DWORD = core::mem::size_of_val(&domainname) as DWORD;
    let mut accountnameuse: SID_NAME_USE = core::mem::zeroed();
    let authn_id: *mut c_char;

    /*
     * Acquire a handle to the server credentials.
     */
    r = AcquireCredentialsHandle(
        null_mut(),
        c"negotiate".as_ptr() as *mut c_char,
        SECPKG_CRED_INBOUND,
        null_mut(),
        null_mut(),
        null_mut(),
        null_mut(),
        &mut sspicred,
        &mut expiry,
    );
    if r != SEC_E_OK {
        pg_SSPI_error(ERROR, c"could not acquire SSPI credentials".as_ptr(), r);
    }

    /*
     * Loop through SSPI message exchange. This exchange can consist of
     * multiple messages sent in both directions. First message is always from
     * the client. All messages from client to server are password packets
     * (type 'p').
     */
    loop {
        pq_startmsgread();
        let mtype2 = pq_getbyte();
        if mtype2 != PqMsg_GSSResponse as c_int {
            if !sspictx.is_null() {
                DeleteSecurityContext(sspictx);
                free(sspictx as *mut c_void);
            }
            FreeCredentialsHandle(&mut sspicred);

            /* Only log error if client didn't disconnect. */
            if mtype2 != EOF {
                // C also: errcode(ERRCODE_PROTOCOL_VIOLATION)
                ereport!(
                    ERROR,
                    errmsg!("expected SSPI response, got message type {}", mtype2)
                );
            }
            return STATUS_ERROR;
        }

        /* Get the actual SSPI token */
        initStringInfo(&mut buf);
        if pq_getmessage(&mut buf, PG_MAX_AUTH_TOKEN_LENGTH) != 0 {
            /* EOF - pq_getmessage already logged error */
            pfree(buf.data as *mut c_void);
            if !sspictx.is_null() {
                DeleteSecurityContext(sspictx);
                free(sspictx as *mut c_void);
            }
            FreeCredentialsHandle(&mut sspicred);
            return STATUS_ERROR;
        }

        /* Map to SSPI style buffer */
        inbuf.ulVersion = SECBUFFER_VERSION;
        inbuf.cBuffers = 1;
        inbuf.pBuffers = InBuffers.as_mut_ptr();
        InBuffers[0].pvBuffer = buf.data as *mut c_void;
        InBuffers[0].cbBuffer = buf.len as ULONG;
        InBuffers[0].BufferType = SECBUFFER_TOKEN;

        /* Prepare output buffer */
        OutBuffers[0].pvBuffer = null_mut();
        OutBuffers[0].BufferType = SECBUFFER_TOKEN;
        OutBuffers[0].cbBuffer = 0;
        outbuf.cBuffers = 1;
        outbuf.pBuffers = OutBuffers.as_mut_ptr();
        outbuf.ulVersion = SECBUFFER_VERSION;

        elog!(
            DEBUG4,
            "processing received SSPI token of length {}",
            buf.len as c_uint
        );

        r = AcceptSecurityContext(
            &mut sspicred,
            sspictx,
            &mut inbuf,
            ASC_REQ_ALLOCATE_MEMORY,
            SECURITY_NETWORK_DREP,
            &mut newctx,
            &mut outbuf,
            &mut contextattr,
            null_mut(),
        );

        /* input buffer no longer used */
        pfree(buf.data as *mut c_void);

        if outbuf.cBuffers > 0 && (*outbuf.pBuffers.add(0)).cbBuffer > 0 {
            /*
             * Negotiation generated data to be sent to the client.
             */
            elog!(
                DEBUG4,
                "sending SSPI response token of length {}",
                (*outbuf.pBuffers.add(0)).cbBuffer as c_uint
            );

            (*(*port).gss).outbuf.length = (*outbuf.pBuffers.add(0)).cbBuffer as Size;
            (*(*port).gss).outbuf.value = (*outbuf.pBuffers.add(0)).pvBuffer;

            sendAuthRequest(
                port,
                AUTH_REQ_GSS_CONT,
                (*(*port).gss).outbuf.value as *const c_void,
                (*(*port).gss).outbuf.length as c_int,
            );

            FreeContextBuffer((*outbuf.pBuffers.add(0)).pvBuffer);
        }

        if r != SEC_E_OK && r != SEC_I_CONTINUE_NEEDED {
            if !sspictx.is_null() {
                DeleteSecurityContext(sspictx);
                free(sspictx as *mut c_void);
            }
            FreeCredentialsHandle(&mut sspicred);
            pg_SSPI_error(ERROR, c"could not accept SSPI security context".as_ptr(), r);
        }

        /*
         * Overwrite the current context with the one we just received. If
         * sspictx is NULL it was the first loop and we need to allocate a
         * buffer for it. On subsequent runs, we can just overwrite the buffer
         * contents since the size does not change.
         */
        if sspictx.is_null() {
            sspictx = malloc(core::mem::size_of::<CtxtHandle>()) as *mut CtxtHandle;
            if sspictx.is_null() {
                ereport!(ERROR, errmsg!("out of memory"));
            }
        }

        memcpy(
            sspictx as *mut c_void,
            &newctx as *const _ as *const c_void,
            core::mem::size_of::<CtxtHandle>(),
        );

        if r == SEC_I_CONTINUE_NEEDED {
            elog!(DEBUG4, "SSPI continue needed");
        }

        if r != SEC_I_CONTINUE_NEEDED {
            break;
        }
    }

    /*
     * Release service principal credentials
     */
    FreeCredentialsHandle(&mut sspicred);

    /*
     * SEC_E_OK indicates that authentication is now complete.
     *
     * Get the name of the user that authenticated, and compare it to the pg
     * username that was specified for the connection.
     */

    r = QuerySecurityContextToken(sspictx, &mut token);
    if r != SEC_E_OK {
        pg_SSPI_error(
            ERROR,
            c"could not get token from SSPI security context".as_ptr(),
            r,
        );
    }

    /*
     * No longer need the security context, everything from here on uses the
     * token instead.
     */
    DeleteSecurityContext(sspictx);
    free(sspictx as *mut c_void);

    if !GetTokenInformation(token, TokenUser, null_mut(), 0, &mut retlen) && GetLastError() != 122 {
        // C also: errmsg_internal("could not get token information buffer size: error code %lu", GetLastError())
        ereport!(
            ERROR,
            errmsg!(
                "could not get token information buffer size: error code {}",
                GetLastError()
            )
        );
    }

    tokenuser = malloc(retlen as Size) as *mut TOKEN_USER;
    if tokenuser.is_null() {
        ereport!(ERROR, errmsg!("out of memory"));
    }

    if !GetTokenInformation(token, TokenUser, tokenuser as *mut c_void, retlen, &mut retlen) {
        // C also: errmsg_internal("could not get token information: error code %lu", GetLastError())
        ereport!(
            ERROR,
            errmsg!("could not get token information: error code {}", GetLastError())
        );
    }

    CloseHandle(token);

    if !LookupAccountSid(
        null_mut(),
        (*tokenuser).User.Sid,
        accountname.as_mut_ptr(),
        &mut accountnamesize,
        domainname.as_mut_ptr(),
        &mut domainnamesize,
        &mut accountnameuse,
    ) {
        // C also: errmsg_internal("could not look up account SID: error code %lu", GetLastError())
        ereport!(
            ERROR,
            errmsg!("could not look up account SID: error code {}", GetLastError())
        );
    }

    free(tokenuser as *mut c_void);

    if !(*(*port).hba).compat_realm {
        let status: c_int = pg_SSPI_make_upn(
            accountname.as_mut_ptr(),
            core::mem::size_of_val(&accountname),
            domainname.as_mut_ptr(),
            core::mem::size_of_val(&domainname),
            (*(*port).hba).upn_username,
        );

        if status != STATUS_OK {
            /* Error already reported from pg_SSPI_make_upn */
            return status;
        }
    }

    /*
     * We have all of the information necessary to construct the authenticated
     * identity.  Set it now, rather than waiting for check_usermap below,
     * because authentication has already succeeded and we want the log file
     * to reflect that.
     */
    if (*(*port).hba).compat_realm {
        /* SAM-compatible format. */
        // C also: psprintf("%s\\%s", domainname, accountname)
        authn_id = psprintf(domainname.as_ptr());
    } else {
        /* Kerberos principal format. */
        // C also: psprintf("%s@%s", accountname, domainname)
        authn_id = psprintf(accountname.as_ptr());
    }

    set_authn_id(port, authn_id);
    pfree(authn_id as *mut c_void);

    /*
     * Compare realm/domain if requested. In SSPI, always compare case
     * insensitive.
     */
    if !(*(*port).hba).krb_realm.is_null() && strlen((*(*port).hba).krb_realm) != 0 {
        if pg_strcasecmp((*(*port).hba).krb_realm, domainname.as_ptr()) != 0 {
            elog!(
                DEBUG2,
                "SSPI domain ({}) and configured domain ({}) don't match",
                CStr::from_ptr(domainname.as_ptr()).to_string_lossy(),
                CStr::from_ptr((*(*port).hba).krb_realm).to_string_lossy()
            );

            return STATUS_ERROR;
        }
    }

    /*
     * We have the username (without domain/realm) in accountname, compare to
     * the supplied value. In SSPI, always compare case insensitive.
     *
     * If set to include realm, append it in <username>@<realm> format.
     */
    let _ = mtype;
    if (*(*port).hba).include_realm {
        let namebuf: *mut c_char;
        let retval: c_int;

        // C also: psprintf("%s@%s", accountname, domainname)
        namebuf = psprintf(accountname.as_ptr());
        retval = check_usermap((*(*port).hba).usermap, (*port).user_name, namebuf, true);
        pfree(namebuf as *mut c_void);
        retval
    } else {
        check_usermap(
            (*(*port).hba).usermap,
            (*port).user_name,
            accountname.as_ptr(),
            true,
        )
    }
}

/*
 * Replaces the domainname with the Kerberos realm name,
 * and optionally the accountname with the Kerberos user name.
 */
unsafe fn pg_SSPI_make_upn(
    accountname: *mut c_char,
    accountnamesize: Size,
    domainname: *mut c_char,
    domainnamesize: Size,
    update_accountname: bool,
) -> c_int {
    let samname: *mut c_char;
    let mut upname: *mut c_char = null_mut();
    let mut p: *mut c_char = null_mut();
    let mut upnamesize: ULONG = 0;
    let upnamerealmsize: Size;
    let mut res: BOOLEAN;

    /*
     * Build SAM name (DOMAIN\user), then translate to UPN
     * (user@kerberos.realm). The realm name is returned in lower case, but
     * that is fine because in SSPI auth, string comparisons are always
     * case-insensitive.
     */

    // C also: psprintf("%s\\%s", domainname, accountname)
    samname = psprintf(domainname);
    res = TranslateName(
        samname,
        NameSamCompatible,
        NameUserPrincipal,
        null_mut(),
        &mut upnamesize,
    );

    if (res == 0 && GetLastError() != ERROR_INSUFFICIENT_BUFFER) || upnamesize == 0 {
        pfree(samname as *mut c_void);
        // C also: errcode(ERRCODE_INVALID_ROLE_SPECIFICATION)
        ereport!(LOG, errmsg!("could not translate name"));
        return STATUS_ERROR;
    }

    /* upnamesize includes the terminating NUL. */
    upname = palloc(upnamesize as Size) as *mut c_char;

    res = TranslateName(
        samname,
        NameSamCompatible,
        NameUserPrincipal,
        upname,
        &mut upnamesize,
    );

    pfree(samname as *mut c_void);
    if res != 0 {
        p = strchr(upname, b'@' as c_int);
    }

    if res == 0 || p.is_null() {
        pfree(upname as *mut c_void);
        // C also: errcode(ERRCODE_INVALID_ROLE_SPECIFICATION)
        ereport!(LOG, errmsg!("could not translate name"));
        return STATUS_ERROR;
    }

    /* Length of realm name after the '@', including the NUL. */
    upnamerealmsize = upnamesize as Size - (p as Size - upname as Size + 1);

    /* Replace domainname with realm name. */
    if upnamerealmsize > domainnamesize {
        pfree(upname as *mut c_void);
        // C also: errcode(ERRCODE_INVALID_ROLE_SPECIFICATION)
        ereport!(LOG, errmsg!("realm name too long"));
        return STATUS_ERROR;
    }

    /* Length is now safe. */
    strcpy(domainname, p.add(1));

    /* Replace account name as well (in case UPN != SAM)? */
    if update_accountname {
        if (p as Size - upname as Size + 1) > accountnamesize {
            pfree(upname as *mut c_void);
            // C also: errcode(ERRCODE_INVALID_ROLE_SPECIFICATION)
            ereport!(LOG, errmsg!("translated account name too long"));
            return STATUS_ERROR;
        }

        *p = 0;
        strcpy(accountname, upname);
    }

    pfree(upname as *mut c_void);
    STATUS_OK
}
// #endif /* ENABLE_SSPI */

// SSPI dependencies (Windows Security API, not ported).
// TODO(pg-port): real SSPI bindings (Windows only).
type SECURITY_STATUS = c_long;
type DWORD = u32;
type ULONG = u32;
type HANDLE = *mut c_void;
type BOOLEAN = u8;
type TimeStamp = i64;
const MAXPGPATH: usize = 1024;
const SEC_E_OK: SECURITY_STATUS = 0;
const SEC_I_CONTINUE_NEEDED: SECURITY_STATUS = 0x00090312;
const SECPKG_CRED_INBOUND: ULONG = 1;
const SECBUFFER_VERSION: ULONG = 0;
const SECBUFFER_TOKEN: ULONG = 2;
const ASC_REQ_ALLOCATE_MEMORY: ULONG = 0x100;
const SECURITY_NETWORK_DREP: ULONG = 0;
const FORMAT_MESSAGE_IGNORE_INSERTS: DWORD = 0x200;
const FORMAT_MESSAGE_FROM_SYSTEM: DWORD = 0x1000;
const ERROR_INSUFFICIENT_BUFFER: DWORD = 122;
const TokenUser: c_int = 1;
const NameSamCompatible: c_int = 2;
const NameUserPrincipal: c_int = 8;
#[repr(C)]
struct CredHandle {
    _opaque: [u8; 16],
}
#[repr(C)]
struct CtxtHandle {
    _opaque: [u8; 16],
}
#[repr(C)]
struct SecBuffer {
    cbBuffer: ULONG,
    BufferType: ULONG,
    pvBuffer: *mut c_void,
}
#[repr(C)]
struct SecBufferDesc {
    ulVersion: ULONG,
    cBuffers: ULONG,
    pBuffers: *mut SecBuffer,
}
#[repr(C)]
struct SID_AND_ATTRIBUTES {
    Sid: *mut c_void,
    Attributes: DWORD,
}
#[repr(C)]
struct TOKEN_USER {
    User: SID_AND_ATTRIBUTES,
}
type SID_NAME_USE = c_int;
unsafe fn FormatMessage(
    _flags: DWORD,
    _source: *mut c_void,
    _msgid: SECURITY_STATUS,
    _langid: DWORD,
    _buf: *mut c_char,
    _size: DWORD,
    _args: *mut c_void,
) -> DWORD {
    unimplemented!() // TODO(pg-port): Windows API
}
unsafe fn AcquireCredentialsHandle(
    _principal: *mut c_char,
    _package: *mut c_char,
    _creduse: ULONG,
    _logonid: *mut c_void,
    _authdata: *mut c_void,
    _getkeyfn: *mut c_void,
    _getkeyarg: *mut c_void,
    _cred: *mut CredHandle,
    _expiry: *mut TimeStamp,
) -> SECURITY_STATUS {
    unimplemented!() // TODO(pg-port): Windows API
}
unsafe fn AcceptSecurityContext(
    _cred: *mut CredHandle,
    _ctx: *mut CtxtHandle,
    _input: *mut SecBufferDesc,
    _contextreq: ULONG,
    _targetdatarep: ULONG,
    _newctx: *mut CtxtHandle,
    _output: *mut SecBufferDesc,
    _contextattr: *mut ULONG,
    _expiry: *mut TimeStamp,
) -> SECURITY_STATUS {
    unimplemented!() // TODO(pg-port): Windows API
}
unsafe fn DeleteSecurityContext(_ctx: *mut CtxtHandle) -> SECURITY_STATUS {
    unimplemented!() // TODO(pg-port): Windows API
}
unsafe fn FreeCredentialsHandle(_cred: *mut CredHandle) -> SECURITY_STATUS {
    unimplemented!() // TODO(pg-port): Windows API
}
unsafe fn FreeContextBuffer(_buf: *mut c_void) -> SECURITY_STATUS {
    unimplemented!() // TODO(pg-port): Windows API
}
unsafe fn QuerySecurityContextToken(_ctx: *mut CtxtHandle, _token: *mut HANDLE) -> SECURITY_STATUS {
    unimplemented!() // TODO(pg-port): Windows API
}
unsafe fn GetTokenInformation(
    _token: HANDLE,
    _class: c_int,
    _info: *mut c_void,
    _len: DWORD,
    _retlen: *mut DWORD,
) -> bool {
    unimplemented!() // TODO(pg-port): Windows API
}
unsafe fn GetLastError() -> DWORD {
    unimplemented!() // TODO(pg-port): Windows API
}
unsafe fn CloseHandle(_h: HANDLE) -> bool {
    unimplemented!() // TODO(pg-port): Windows API
}
unsafe fn LookupAccountSid(
    _system: *mut c_char,
    _sid: *mut c_void,
    _name: *mut c_char,
    _namesize: *mut DWORD,
    _domain: *mut c_char,
    _domainsize: *mut DWORD,
    _use: *mut SID_NAME_USE,
) -> bool {
    unimplemented!() // TODO(pg-port): Windows API
}
unsafe fn TranslateName(
    _name: *mut c_char,
    _fmt: c_int,
    _desiredfmt: c_int,
    _translated: *mut c_char,
    _size: *mut ULONG,
) -> BOOLEAN {
    unimplemented!() // TODO(pg-port): Windows API
}
extern "C" {
    fn malloc(size: Size) -> *mut c_void;
    fn free(ptr: *mut c_void);
}

/*----------------------------------------------------------------
 * Ident authentication system
 *----------------------------------------------------------------
 */

/*
 *	Parse the string "*ident_response" as a response from a query to an Ident
 *	server.  If it's a normal response indicating a user name, return true
 *	and store the user name at *ident_user. If it's anything else,
 *	return false.
 */
unsafe fn interpret_ident_response(ident_response: *const c_char, ident_user: *mut c_char) -> bool {
    let mut cursor: *const c_char = ident_response; /* Cursor into *ident_response */

    /*
     * Ident's response, in the telnet tradition, should end in crlf (\r\n).
     */
    if strlen(ident_response) < 2 {
        return false;
    } else if *ident_response.add(strlen(ident_response) - 2) != b'\r' as c_char {
        return false;
    } else {
        while *cursor != b':' as c_char && *cursor != b'\r' as c_char {
            cursor = cursor.add(1); /* skip port field */
        }

        if *cursor != b':' as c_char {
            return false;
        } else {
            /* We're positioned to colon before response type field */
            let mut response_type: [c_char; 80] = [0; 80];
            let mut i: c_int; /* Index into *response_type */

            cursor = cursor.add(1); /* Go over colon */
            while pg_isblank(*cursor) {
                cursor = cursor.add(1); /* skip blanks */
            }
            i = 0;
            while *cursor != b':' as c_char
                && *cursor != b'\r' as c_char
                && !pg_isblank(*cursor)
                && i < (core::mem::size_of_val(&response_type) - 1) as c_int
            {
                response_type[i as usize] = *cursor;
                cursor = cursor.add(1);
                i += 1;
            }
            response_type[i as usize] = b'\0' as c_char;
            while pg_isblank(*cursor) {
                cursor = cursor.add(1); /* skip blanks */
            }
            if strcmp(response_type.as_ptr(), c"USERID".as_ptr()) != 0 {
                return false;
            } else {
                /*
                 * It's a USERID response.  Good.  "cursor" should be pointing
                 * to the colon that precedes the operating system type.
                 */
                if *cursor != b':' as c_char {
                    return false;
                } else {
                    cursor = cursor.add(1); /* Go over colon */
                    /* Skip over operating system field. */
                    while *cursor != b':' as c_char && *cursor != b'\r' as c_char {
                        cursor = cursor.add(1);
                    }
                    if *cursor != b':' as c_char {
                        return false;
                    } else {
                        cursor = cursor.add(1); /* Go over colon */
                        while pg_isblank(*cursor) {
                            cursor = cursor.add(1); /* skip blanks */
                        }
                        /* Rest of line is user name.  Copy it over. */
                        i = 0;
                        while *cursor != b'\r' as c_char && i < IDENT_USERNAME_MAX as c_int {
                            *ident_user.add(i as usize) = *cursor;
                            cursor = cursor.add(1);
                            i += 1;
                        }
                        *ident_user.add(i as usize) = b'\0' as c_char;
                        return true;
                    }
                }
            }
        }
    }
}

/*
 *	Talk to the ident server on "remote_addr" and find out who
 *	owns the tcp connection to "local_addr"
 *	If the username is successfully retrieved, check the usermap.
 *
 *	XXX: Using WaitLatchOrSocket() and doing a CHECK_FOR_INTERRUPTS() if the
 *	latch was set would improve the responsiveness to timeouts/cancellations.
 */
unsafe fn ident_inet(port: *mut Port) -> c_int {
    let remote_addr: SockAddrStorage = core::ptr::read(&(*port).raddr as *const _ as *const SockAddrStorage);
    let local_addr: SockAddrStorage = core::ptr::read(&(*port).laddr as *const _ as *const SockAddrStorage);
    let mut ident_user: [c_char; IDENT_USERNAME_MAX + 1] = [0; IDENT_USERNAME_MAX + 1];
    let mut sock_fd: c_int = PGINVALID_SOCKET; /* for talking to Ident server */
    let mut rc: c_int; /* Return code from a locally called function */
    let ident_return: bool;
    let mut remote_addr_s: [c_char; NI_MAXHOST] = [0; NI_MAXHOST];
    let mut remote_port: [c_char; NI_MAXSERV] = [0; NI_MAXSERV];
    let mut local_addr_s: [c_char; NI_MAXHOST] = [0; NI_MAXHOST];
    let mut local_port: [c_char; NI_MAXSERV] = [0; NI_MAXSERV];
    let mut ident_port: [c_char; NI_MAXSERV] = [0; NI_MAXSERV];
    let mut ident_query: [c_char; 80] = [0; 80];
    let mut ident_response: [c_char; 80 + IDENT_USERNAME_MAX] = [0; 80 + IDENT_USERNAME_MAX];
    let mut ident_serv: *mut addrinfo = null_mut();
    let mut la: *mut addrinfo = null_mut();
    let mut hints: addrinfo = core::mem::zeroed();

    'ident_inet_done: {
        /*
         * Might look a little weird to first convert it to text and then back to
         * sockaddr, but it's protocol independent.
         */
        pg_getnameinfo_all(
            &remote_addr as *const SockAddrStorage,
            remote_addr.salen,
            remote_addr_s.as_mut_ptr(),
            core::mem::size_of_val(&remote_addr_s) as c_int,
            remote_port.as_mut_ptr(),
            core::mem::size_of_val(&remote_port) as c_int,
            NI_NUMERICHOST | NI_NUMERICSERV,
        );
        pg_getnameinfo_all(
            &local_addr as *const SockAddrStorage,
            local_addr.salen,
            local_addr_s.as_mut_ptr(),
            core::mem::size_of_val(&local_addr_s) as c_int,
            local_port.as_mut_ptr(),
            core::mem::size_of_val(&local_port) as c_int,
            NI_NUMERICHOST | NI_NUMERICSERV,
        );

        snprintf(
            ident_port.as_mut_ptr(),
            core::mem::size_of_val(&ident_port),
            c"%d".as_ptr(),
            IDENT_PORT,
        );
        hints.ai_flags = AI_NUMERICHOST;
        hints.ai_family = remote_addr.addr.ss_family as c_int;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_protocol = 0;
        hints.ai_addrlen = 0;
        hints.ai_canonname = null_mut();
        hints.ai_addr = null_mut();
        hints.ai_next = null_mut();
        rc = pg_getaddrinfo_all(
            remote_addr_s.as_ptr(),
            ident_port.as_ptr(),
            &hints,
            &mut ident_serv,
        );
        if rc != 0 || ident_serv.is_null() {
            /* we don't expect this to happen */
            ident_return = false;
            break 'ident_inet_done;
        }

        hints.ai_flags = AI_NUMERICHOST;
        hints.ai_family = local_addr.addr.ss_family as c_int;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_protocol = 0;
        hints.ai_addrlen = 0;
        hints.ai_canonname = null_mut();
        hints.ai_addr = null_mut();
        hints.ai_next = null_mut();
        rc = pg_getaddrinfo_all(local_addr_s.as_ptr(), null(), &hints, &mut la);
        if rc != 0 || la.is_null() {
            /* we don't expect this to happen */
            ident_return = false;
            break 'ident_inet_done;
        }

        sock_fd = socket(
            (*ident_serv).ai_family,
            (*ident_serv).ai_socktype,
            (*ident_serv).ai_protocol,
        );
        if sock_fd == PGINVALID_SOCKET {
            // C also: errcode_for_socket_access()
            ereport!(
                LOG,
                errmsg!("could not create socket for Ident connection: {}", "%m")
            );
            ident_return = false;
            break 'ident_inet_done;
        }

        /*
         * Bind to the address which the client originally contacted, otherwise
         * the ident server won't be able to match up the right connection. This
         * is necessary if the PostgreSQL server is running on an IP alias.
         */
        rc = bind(sock_fd, (*la).ai_addr, (*la).ai_addrlen);
        if rc != 0 {
            // C also: errcode_for_socket_access()
            ereport!(
                LOG,
                errmsg!(
                    "could not bind to local address \"{}\": {}",
                    CStr::from_ptr(local_addr_s.as_ptr()).to_string_lossy(),
                    "%m"
                )
            );
            ident_return = false;
            break 'ident_inet_done;
        }

        rc = connect(sock_fd, (*ident_serv).ai_addr, (*ident_serv).ai_addrlen);
        if rc != 0 {
            // C also: errcode_for_socket_access()
            ereport!(
                LOG,
                errmsg!(
                    "could not connect to Ident server at address \"{}\", port {}: {}",
                    CStr::from_ptr(remote_addr_s.as_ptr()).to_string_lossy(),
                    CStr::from_ptr(ident_port.as_ptr()).to_string_lossy(),
                    "%m"
                )
            );
            ident_return = false;
            break 'ident_inet_done;
        }

        /* The query we send to the Ident server */
        snprintf(
            ident_query.as_mut_ptr(),
            core::mem::size_of_val(&ident_query),
            c"%s,%s\r\n".as_ptr(),
            remote_port.as_ptr(),
            local_port.as_ptr(),
        );

        /* loop in case send is interrupted */
        loop {
            CHECK_FOR_INTERRUPTS();

            rc = send(
                sock_fd,
                ident_query.as_ptr() as *const c_void,
                strlen(ident_query.as_ptr()),
                0,
            ) as c_int;

            if !(rc < 0 && errno() == EINTR) {
                break;
            }
        }

        if rc < 0 {
            // C also: errcode_for_socket_access()
            ereport!(
                LOG,
                errmsg!(
                    "could not send query to Ident server at address \"{}\", port {}: {}",
                    CStr::from_ptr(remote_addr_s.as_ptr()).to_string_lossy(),
                    CStr::from_ptr(ident_port.as_ptr()).to_string_lossy(),
                    "%m"
                )
            );
            ident_return = false;
            break 'ident_inet_done;
        }

        loop {
            CHECK_FOR_INTERRUPTS();

            rc = recv(
                sock_fd,
                ident_response.as_mut_ptr() as *mut c_void,
                core::mem::size_of_val(&ident_response) - 1,
                0,
            ) as c_int;

            if !(rc < 0 && errno() == EINTR) {
                break;
            }
        }

        if rc < 0 {
            // C also: errcode_for_socket_access()
            ereport!(
                LOG,
                errmsg!(
                    "could not receive response from Ident server at address \"{}\", port {}: {}",
                    CStr::from_ptr(remote_addr_s.as_ptr()).to_string_lossy(),
                    CStr::from_ptr(ident_port.as_ptr()).to_string_lossy(),
                    "%m"
                )
            );
            ident_return = false;
            break 'ident_inet_done;
        }

        ident_response[rc as usize] = b'\0' as c_char;
        ident_return = interpret_ident_response(ident_response.as_ptr(), ident_user.as_mut_ptr());
        if !ident_return {
            ereport!(
                LOG,
                errmsg!(
                    "invalidly formatted response from Ident server: \"{}\"",
                    CStr::from_ptr(ident_response.as_ptr()).to_string_lossy()
                )
            );
        }
    } /* ident_inet_done: */

    if sock_fd != PGINVALID_SOCKET {
        closesocket(sock_fd);
    }
    if !ident_serv.is_null() {
        pg_freeaddrinfo_all(remote_addr.addr.ss_family as c_int, ident_serv);
    }
    if !la.is_null() {
        pg_freeaddrinfo_all(local_addr.addr.ss_family as c_int, la);
    }

    if ident_return {
        /*
         * Success!  Store the identity, then check the usermap. Note that
         * setting the authenticated identity is done before checking the
         * usermap, because at this point authentication has succeeded.
         */
        set_authn_id(port, ident_user.as_ptr());
        return check_usermap(
            (*(*port).hba).usermap,
            (*port).user_name,
            ident_user.as_ptr(),
            false,
        );
    }
    STATUS_ERROR
}

/*----------------------------------------------------------------
 * Peer authentication system
 *----------------------------------------------------------------
 */

/*
 *	Ask kernel about the credentials of the connecting process,
 *	determine the symbolic name of the corresponding user, and check
 *	if valid per the usermap.
 *
 *	Iff authorized, return STATUS_OK, otherwise return STATUS_ERROR.
 */
unsafe fn auth_peer(port: *mut Port) -> c_int {
    let mut uid: uid_t = 0;
    let mut gid: gid_t = 0;
    // #ifndef WIN32
    let mut pwbuf: passwd = core::mem::zeroed();
    let mut pw: *mut passwd = null_mut();
    let mut buf: [c_char; 1024] = [0; 1024];
    let rc: c_int;
    let ret: c_int;
    // #endif

    if getpeereid((*port).sock, &mut uid, &mut gid) != 0 {
        /* Provide special error message if getpeereid is a stub */
        if errno() == ENOSYS {
            // C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            ereport!(
                LOG,
                errmsg!("peer authentication is not supported on this platform")
            );
        } else {
            // C also: errcode_for_socket_access()
            ereport!(LOG, errmsg!("could not get peer credentials: {}", "%m"));
        }
        return STATUS_ERROR;
    }

    // #ifndef WIN32
    rc = getpwuid_r(
        uid,
        &mut pwbuf,
        buf.as_mut_ptr(),
        core::mem::size_of_val(&buf),
        &mut pw,
    );
    if rc != 0 {
        set_errno(rc);
        ereport!(
            LOG,
            errmsg!("could not look up local user ID {}: {}", uid as c_long, "%m")
        );
        return STATUS_ERROR;
    } else if pw.is_null() {
        ereport!(
            LOG,
            errmsg!("local user with ID {} does not exist", uid as c_long)
        );
        return STATUS_ERROR;
    }

    /*
     * Make a copy of static getpw*() result area; this is our authenticated
     * identity.  Set it before calling check_usermap, because authentication
     * has already succeeded and we want the log file to reflect that.
     */
    set_authn_id(port, (*pw).pw_name);

    ret = check_usermap(
        (*(*port).hba).usermap,
        (*port).user_name,
        MyClientConnectionInfo.authn_id,
        false,
    );

    ret
    // #else /* should have failed with ENOSYS above */ Assert(false); STATUS_ERROR #endif
}

/*----------------------------------------------------------------
 * PAM authentication system
 *----------------------------------------------------------------
 */
// #ifdef USE_PAM

// PAM library types and constants (security/pam_appl.h, not ported in this
// build). TODO(pg-port): real PAM bindings (USE_PAM).
const PAM_MAX_NUM_MSG: c_int = 32;
const PAM_SUCCESS: c_int = 0;
const PAM_CONV_ERR: c_int = 19;
const PAM_PROMPT_ECHO_OFF: c_int = 1;
const PAM_ERROR_MSG: c_int = 3;
const PAM_TEXT_INFO: c_int = 4;

#[repr(C)]
struct pam_message {
    msg_style: c_int,
    msg: *const c_char,
}

#[repr(C)]
struct pam_response {
    resp: *mut c_char,
    resp_retcode: c_int,
}

// PG_PAM_CONST resolves to const on modern PAM. Statics shared with
// pam_passwd_conv_proc, set up by CheckPAMAuth before the conversation runs.
static mut pam_passwd: *const c_char = null();
static mut pam_port_cludge: *mut Port = null_mut();
static mut pam_no_password: bool = false;

// PAM item identifiers (security/pam_appl.h).
const PAM_USER: c_int = 2;
const PAM_CONV: c_int = 5;
const PAM_RHOST: c_int = 4;

// Opaque PAM authenticator handle.
#[repr(C)]
struct pam_handle_t {
    _private: [u8; 0],
}

// struct pam_conv: conversation callback plus appdata pointer.
#[repr(C)]
struct pam_conv {
    conv: Option<
        unsafe extern "C" fn(c_int, *mut *const pam_message, *mut *mut pam_response, *mut c_void) -> c_int,
    >,
    appdata_ptr: *mut c_void,
}

// static struct pam_conv pam_passw_conv = { &pam_passwd_conv_proc, NULL };
static mut pam_passw_conv: pam_conv = pam_conv {
    conv: Some(pam_passwd_conv_proc),
    appdata_ptr: null_mut(),
};

// TODO(pg-port): real PAM library bindings (USE_PAM, -lpam).
unsafe fn pam_start(
    _service: *const c_char,
    _user: *const c_char,
    _conv: *const pam_conv,
    _pamh: *mut *mut pam_handle_t,
) -> c_int {
    unimplemented!() // TODO(pg-port): libpam pam_start
}
unsafe fn pam_set_item(_pamh: *mut pam_handle_t, _item_type: c_int, _item: *const c_void) -> c_int {
    unimplemented!() // TODO(pg-port): libpam pam_set_item
}
unsafe fn pam_authenticate(_pamh: *mut pam_handle_t, _flags: c_int) -> c_int {
    unimplemented!() // TODO(pg-port): libpam pam_authenticate
}
unsafe fn pam_acct_mgmt(_pamh: *mut pam_handle_t, _flags: c_int) -> c_int {
    unimplemented!() // TODO(pg-port): libpam pam_acct_mgmt
}
unsafe fn pam_end(_pamh: *mut pam_handle_t, _pam_status: c_int) -> c_int {
    unimplemented!() // TODO(pg-port): libpam pam_end
}
unsafe fn pam_strerror(_pamh: *mut pam_handle_t, _errnum: c_int) -> *const c_char {
    unimplemented!() // TODO(pg-port): libpam pam_strerror
}

// TODO(pg-port): libc strdup/calloc for PAM-owned memory. (free/strlen already declared above.)
extern "C" {
    fn strdup(s: *const c_char) -> *mut c_char;
    fn calloc(nmemb: usize, size: usize) -> *mut c_void;
}

/*
 * pam_passwd_conv_proc: PAM conversation function
 */
unsafe extern "C" fn pam_passwd_conv_proc(
    num_msg: c_int,
    msg: *mut *const pam_message,
    resp: *mut *mut pam_response,
    appdata_ptr: *mut c_void,
) -> c_int {
    let mut passwd: *const c_char;
    let reply: *mut pam_response;
    let mut i: c_int;

    if !appdata_ptr.is_null() {
        passwd = appdata_ptr as *const c_char;
    } else {
        /*
         * Workaround for Solaris 2.6 where the PAM library is broken and does
         * not pass appdata_ptr to the conversation routine
         */
        passwd = pam_passwd;
    }

    *resp = null_mut(); /* in case of error exit */

    if num_msg <= 0 || num_msg > PAM_MAX_NUM_MSG {
        return PAM_CONV_ERR;
    }

    /*
     * Explicitly not using palloc here - PAM will free this memory in
     * pam_end()
     */
    reply = calloc(num_msg as usize, core::mem::size_of::<pam_response>()) as *mut pam_response;
    if reply.is_null() {
        ereport!(
            LOG,
            errmsg!("out of memory")
        );
        /* C also: errcode(ERRCODE_OUT_OF_MEMORY) */
        return PAM_CONV_ERR;
    }

    i = 0;
    while i < num_msg {
        let cur = *msg.add(i as usize);
        let slot = reply.add(i as usize);
        match (*cur).msg_style {
            x if x == PAM_PROMPT_ECHO_OFF => {
                if strlen(passwd) == 0 {
                    /*
                     * Password wasn't passed to PAM the first time around -
                     * let's go ask the client to send a password, which we
                     * then stuff into PAM.
                     */
                    sendAuthRequest(pam_port_cludge, AUTH_REQ_PASSWORD, null(), 0);
                    passwd = recv_password_packet(pam_port_cludge);
                    if passwd.is_null() {
                        /*
                         * Client didn't want to send password.  We
                         * intentionally do not log anything about this,
                         * either here or at higher levels.
                         */
                        pam_no_password = true;
                        return pam_passwd_conv_fail(reply, num_msg);
                    }
                }
                (*slot).resp = strdup(passwd);
                if (*slot).resp.is_null() {
                    return pam_passwd_conv_fail(reply, num_msg);
                }
                (*slot).resp_retcode = PAM_SUCCESS;
            }
            x if x == PAM_ERROR_MSG => {
                ereport!(
                    LOG,
                    errmsg!(
                        "error from underlying PAM layer: {}",
                        CStr::from_ptr((*cur).msg).to_string_lossy()
                    )
                );
                /* FALL THROUGH */
                /* we don't bother to log TEXT_INFO messages */
                (*slot).resp = strdup(c"".as_ptr());
                if (*slot).resp.is_null() {
                    return pam_passwd_conv_fail(reply, num_msg);
                }
                (*slot).resp_retcode = PAM_SUCCESS;
            }
            x if x == PAM_TEXT_INFO => {
                /* we don't bother to log TEXT_INFO messages */
                (*slot).resp = strdup(c"".as_ptr());
                if (*slot).resp.is_null() {
                    return pam_passwd_conv_fail(reply, num_msg);
                }
                (*slot).resp_retcode = PAM_SUCCESS;
            }
            _ => {
                let msg_str = if !(*cur).msg.is_null() {
                    CStr::from_ptr((*cur).msg).to_string_lossy().into_owned()
                } else {
                    "(none)".to_string()
                };
                ereport!(
                    LOG,
                    errmsg!(
                        "unsupported PAM conversation {}/\"{}\"",
                        (*cur).msg_style,
                        msg_str
                    )
                );
                return pam_passwd_conv_fail(reply, num_msg);
            }
        }
        i += 1;
    }

    *resp = reply;
    PAM_SUCCESS
}

/* fail: free up whatever we allocated, return PAM_CONV_ERR */
unsafe fn pam_passwd_conv_fail(reply: *mut pam_response, num_msg: c_int) -> c_int {
    let mut i = 0;
    while i < num_msg {
        free((*reply.add(i as usize)).resp as *mut c_void);
        i += 1;
    }
    free(reply as *mut c_void);

    PAM_CONV_ERR
}

/*
 * Check authentication against PAM.
 */
unsafe fn CheckPAMAuth(port: *mut Port, user: *const c_char, password: *const c_char) -> c_int {
    let mut retval: c_int;
    let mut pamh: *mut pam_handle_t = null_mut();

    /*
     * We can't entirely rely on PAM to pass through appdata --- it appears
     * not to work on at least Solaris 2.6.  So use these ugly static
     * variables instead.
     */
    pam_passwd = password;
    pam_port_cludge = port;
    pam_no_password = false;

    /*
     * Set the application data portion of the conversation struct.  This is
     * later used inside the PAM conversation to pass the password to the
     * authentication module.
     */
    pam_passw_conv.appdata_ptr = password as *mut c_char as *mut c_void; /* from password above, not allocated */

    /* Optionally, one can set the service name in pg_hba.conf */
    if !(*(*port).hba).pamservice.is_null() && *(*(*port).hba).pamservice != 0 {
        retval = pam_start(
            (*(*port).hba).pamservice,
            c"pgsql@".as_ptr(),
            &raw const pam_passw_conv,
            &mut pamh,
        );
    } else {
        retval = pam_start(
            PGSQL_PAM_SERVICE.as_ptr(),
            c"pgsql@".as_ptr(),
            &raw const pam_passw_conv,
            &mut pamh,
        );
    }

    if retval != PAM_SUCCESS {
        ereport!(
            LOG,
            errmsg!(
                "could not create PAM authenticator: {}",
                CStr::from_ptr(pam_strerror(pamh, retval)).to_string_lossy()
            )
        );
        pam_passwd = null(); /* Unset pam_passwd */
        return STATUS_ERROR;
    }

    retval = pam_set_item(pamh, PAM_USER, user as *const c_void);

    if retval != PAM_SUCCESS {
        ereport!(
            LOG,
            errmsg!(
                "pam_set_item(PAM_USER) failed: {}",
                CStr::from_ptr(pam_strerror(pamh, retval)).to_string_lossy()
            )
        );
        pam_passwd = null(); /* Unset pam_passwd */
        return STATUS_ERROR;
    }

    if (*(*port).hba).conntype != ctLocal {
        let mut hostinfo: [c_char; NI_MAXHOST] = [0; NI_MAXHOST];
        let flags: c_int;

        if (*(*port).hba).pam_use_hostname {
            flags = 0;
        } else {
            flags = NI_NUMERICHOST | NI_NUMERICSERV;
        }

        retval = pg_getnameinfo_all(
            &(*port).raddr as *const _ as *const SockAddrStorage,
            salen_of(&(*port).raddr),
            hostinfo.as_mut_ptr(),
            core::mem::size_of_val(&hostinfo) as c_int,
            null_mut(),
            0,
            flags,
        );
        if retval != 0 {
            ereport!(
                WARNING,
                errmsg!(
                    "pg_getnameinfo_all() failed: {}",
                    CStr::from_ptr(gai_strerror(retval)).to_string_lossy()
                )
            );
            /* C: errmsg_internal */
            return STATUS_ERROR;
        }

        retval = pam_set_item(pamh, PAM_RHOST, hostinfo.as_ptr() as *const c_void);

        if retval != PAM_SUCCESS {
            ereport!(
                LOG,
                errmsg!(
                    "pam_set_item(PAM_RHOST) failed: {}",
                    CStr::from_ptr(pam_strerror(pamh, retval)).to_string_lossy()
                )
            );
            pam_passwd = null();
            return STATUS_ERROR;
        }
    }

    retval = pam_set_item(pamh, PAM_CONV, &raw const pam_passw_conv as *const c_void);

    if retval != PAM_SUCCESS {
        ereport!(
            LOG,
            errmsg!(
                "pam_set_item(PAM_CONV) failed: {}",
                CStr::from_ptr(pam_strerror(pamh, retval)).to_string_lossy()
            )
        );
        pam_passwd = null(); /* Unset pam_passwd */
        return STATUS_ERROR;
    }

    retval = pam_authenticate(pamh, 0);

    if retval != PAM_SUCCESS {
        /* If pam_passwd_conv_proc saw EOF, don't log anything */
        if !pam_no_password {
            ereport!(
                LOG,
                errmsg!(
                    "pam_authenticate failed: {}",
                    CStr::from_ptr(pam_strerror(pamh, retval)).to_string_lossy()
                )
            );
        }
        pam_passwd = null(); /* Unset pam_passwd */
        return if pam_no_password { STATUS_EOF } else { STATUS_ERROR };
    }

    retval = pam_acct_mgmt(pamh, 0);

    if retval != PAM_SUCCESS {
        /* If pam_passwd_conv_proc saw EOF, don't log anything */
        if !pam_no_password {
            ereport!(
                LOG,
                errmsg!(
                    "pam_acct_mgmt failed: {}",
                    CStr::from_ptr(pam_strerror(pamh, retval)).to_string_lossy()
                )
            );
        }
        pam_passwd = null(); /* Unset pam_passwd */
        return if pam_no_password { STATUS_EOF } else { STATUS_ERROR };
    }

    retval = pam_end(pamh, retval);

    if retval != PAM_SUCCESS {
        ereport!(
            LOG,
            errmsg!(
                "could not release PAM authenticator: {}",
                CStr::from_ptr(pam_strerror(pamh, retval)).to_string_lossy()
            )
        );
    }

    pam_passwd = null(); /* Unset pam_passwd */

    if retval == PAM_SUCCESS {
        set_authn_id(port, user);
    }

    if retval == PAM_SUCCESS {
        STATUS_OK
    } else {
        STATUS_ERROR
    }
}
// #endif /* USE_PAM */

/*----------------------------------------------------------------
 * BSD authentication system
 *----------------------------------------------------------------
 */
// #ifdef USE_BSD_AUTH
unsafe fn CheckBSDAuth(port: *mut Port, user: *mut c_char) -> c_int {
    let passwd: *mut c_char;
    let retval: c_int;

    /* Send regular password request to client, and get the response */
    sendAuthRequest(port, AUTH_REQ_PASSWORD, null(), 0);

    passwd = recv_password_packet(port);
    if passwd.is_null() {
        return STATUS_EOF;
    }

    /*
     * Ask the BSD auth system to verify password.  Note that auth_userokay
     * will overwrite the password string with zeroes, but it's just a
     * temporary string so we don't care.
     */
    retval = auth_userokay(user, null_mut(), c"auth-postgresql".as_ptr(), passwd);

    pfree(passwd as *mut c_void);

    if retval == 0 {
        return STATUS_ERROR;
    }

    set_authn_id(port, user);
    STATUS_OK
}
// #endif /* USE_BSD_AUTH */

// BSD auth dependency (bsd_auth.h, not ported).
// TODO(pg-port): real BSD auth bindings (USE_BSD_AUTH).
unsafe fn auth_userokay(
    _name: *mut c_char,
    _style: *mut c_char,
    _type: *const c_char,
    _password: *mut c_char,
) -> c_int {
    unimplemented!() // TODO(pg-port): bsd_auth.h
}

use crate::nodes::pg_list::{list_head, list_length, lfirst, lnext, ListCell, NIL};
use crate::{current_cell, foreach};

/*----------------------------------------------------------------
 * LDAP authentication system
 *----------------------------------------------------------------
 */
// #ifdef USE_LDAP

/* Default LDAP password mutator hook, can be overridden by a shared library */
unsafe fn dummy_ldap_password_mutator(input: *mut c_char) -> *mut c_char {
    input
}
pub type auth_password_hook_typ = Option<unsafe extern "C" fn(*mut c_char) -> *mut c_char>;
// auth_password_hook_typ ldap_password_hook = dummy_ldap_password_mutator;
#[no_mangle]
pub static mut ldap_password_hook: auth_password_hook_typ = None;

/* Placeholders recognized by FormatSearchFilter.  For now just one. */
const LPH_USERNAME: &CStr = c"$username";
const LPH_USERNAME_LEN: Size = 9; /* sizeof("$username") - 1 */

/* Not all LDAP implementations define this. */
const LDAP_NO_ATTRS: &CStr = c"1.1";

/* Not all LDAP implementations define this. */
const LDAPS_PORT: c_int = 636;
const LDAP_PORT: c_int = 389;
const LDAP_VERSION3: c_int = 3;
const LDAP_SUCCESS: c_int = 0;
const LDAP_OPT_PROTOCOL_VERSION: c_int = 0x0011;
const LDAP_OPT_ERROR_NUMBER: c_int = 0x0031;
const LDAP_OPT_DIAGNOSTIC_MESSAGE: c_int = 0x0032;

type LDAP = c_void;
type LDAPMessage = c_void;

/*
 * Initialize a connection to the LDAP server, including setting up
 * TLS if requested.
 */
unsafe fn InitializeLDAPConnection(port: *mut Port, ldap: *mut *mut LDAP) -> c_int {
    let mut scheme: *const c_char;
    let ldapversion: c_int = LDAP_VERSION3;
    let r: c_int;

    scheme = (*(*port).hba).ldapscheme;
    if scheme.is_null() {
        scheme = c"ldap".as_ptr();
    }
    // #ifdef HAVE_LDAP_INITIALIZE
    {
        let mut uris: StringInfoData = core::mem::zeroed();
        let mut hostlist: *mut c_char = null_mut();
        let mut p: *mut c_char;
        let append_port: bool;

        /* We'll build a space-separated scheme://hostname:port list here */
        initStringInfo(&mut uris);

        /*
         * If pg_hba.conf provided no hostnames, we can ask OpenLDAP to try to
         * find some by extracting a domain name from the base DN and looking
         * up DSN SRV records for _ldap._tcp.<domain>.
         */
        if (*(*port).hba).ldapserver.is_null() || *(*(*port).hba).ldapserver == 0 {
            let mut domain: *mut c_char = null_mut();

            /* ou=blah,dc=foo,dc=bar -> foo.bar */
            if ldap_dn2domain((*(*port).hba).ldapbasedn, &mut domain) != 0 {
                ereport!(
                    LOG,
                    errmsg!("could not extract domain name from ldapbasedn")
                );
                return STATUS_ERROR;
            }

            /* Look up a list of LDAP server hosts and port numbers */
            if ldap_domain2hostlist(domain, &mut hostlist) != 0 {
                // C also: errhint("Set an LDAP server name explicitly.")
                ereport!(
                    LOG,
                    errmsg!(
                        "LDAP authentication could not find DNS SRV records for \"{}\"",
                        CStr::from_ptr(domain).to_string_lossy()
                    )
                );
                ldap_memfree(domain as *mut c_void);
                return STATUS_ERROR;
            }
            ldap_memfree(domain as *mut c_void);

            /* We have a space-separated list of host:port entries */
            p = hostlist;
            append_port = false;
        } else {
            /* We have a space-separated list of hosts from pg_hba.conf */
            p = (*(*port).hba).ldapserver;
            append_port = true;
        }

        /* Convert the list of host[:port] entries to full URIs */
        loop {
            let size: Size;

            /* Find the span of the next entry */
            size = strcspn(p, c" ".as_ptr());

            /* Append a space separator if this isn't the first URI */
            if uris.len > 0 {
                appendStringInfoChar(&mut uris, b' ' as c_char);
            }

            /* Append scheme://host:port */
            appendStringInfoString(&mut uris, scheme);
            appendStringInfoString(&mut uris, c"://".as_ptr());
            appendBinaryStringInfo(&mut uris, p as *const c_void, size as c_int);
            if append_port {
                // C also: appendStringInfo(&uris, ":%d", port->hba->ldapport)
                appendStringInfo!(&mut uris, ":{}", (*(*port).hba).ldapport);
            }

            /* Step over this entry and any number of trailing spaces */
            p = p.add(size);
            while *p == b' ' as c_char {
                p = p.add(1);
            }

            if *p == 0 {
                break;
            }
        }

        /* Free memory from OpenLDAP if we looked up SRV records */
        if !hostlist.is_null() {
            ldap_memfree(hostlist as *mut c_void);
        }

        /* Finally, try to connect using the URI list */
        r = ldap_initialize(ldap, uris.data);
        pfree(uris.data as *mut c_void);
        if r != LDAP_SUCCESS {
            ereport!(
                LOG,
                errmsg!(
                    "could not initialize LDAP: {}",
                    CStr::from_ptr(ldap_err2string(r)).to_string_lossy()
                )
            );

            return STATUS_ERROR;
        }
    }
    // #endif

    let mut r2: c_int = ldap_set_option(
        *ldap,
        LDAP_OPT_PROTOCOL_VERSION,
        &ldapversion as *const _ as *const c_void,
    );
    if r2 != LDAP_SUCCESS {
        // C also: errdetail_for_ldap(*ldap)
        ereport!(
            LOG,
            errmsg!(
                "could not set LDAP protocol version: {}",
                CStr::from_ptr(ldap_err2string(r2)).to_string_lossy()
            )
        );
        ldap_unbind(*ldap);
        return STATUS_ERROR;
    }

    if (*(*port).hba).ldaptls {
        // #ifndef WIN32
        r2 = ldap_start_tls_s(*ldap, null_mut(), null_mut());
        if r2 != LDAP_SUCCESS {
            // C also: errdetail_for_ldap(*ldap)
            ereport!(
                LOG,
                errmsg!(
                    "could not start LDAP TLS session: {}",
                    CStr::from_ptr(ldap_err2string(r2)).to_string_lossy()
                )
            );
            ldap_unbind(*ldap);
            return STATUS_ERROR;
        }
    }

    STATUS_OK
}

/*
 * Return a newly allocated C string copied from "pattern" with all
 * occurrences of the placeholder "$username" replaced with "user_name".
 */
unsafe fn FormatSearchFilter(mut pattern: *const c_char, user_name: *const c_char) -> *mut c_char {
    let mut output: StringInfoData = core::mem::zeroed();

    initStringInfo(&mut output);
    while *pattern != b'\0' as c_char {
        if strncmp(pattern, LPH_USERNAME.as_ptr(), LPH_USERNAME_LEN) == 0 {
            appendStringInfoString(&mut output, user_name);
            pattern = pattern.add(LPH_USERNAME_LEN);
        } else {
            appendStringInfoChar(&mut output, *pattern);
            pattern = pattern.add(1);
        }
    }

    output.data
}

/*
 * Perform LDAP authentication
 */
unsafe fn CheckLDAPAuth(port: *mut Port) -> c_int {
    let passwd: *mut c_char;
    let mut ldap: *mut LDAP = null_mut();
    let mut r: c_int;
    let fulluser: *mut c_char;
    let server_name: *const c_char;

    // #ifdef HAVE_LDAP_INITIALIZE
    /*
     * For OpenLDAP, allow empty hostname if we have a basedn.  We'll look for
     * servers with DNS SRV records via OpenLDAP library facilities.
     */
    if ((*(*port).hba).ldapserver.is_null() || *(*(*port).hba).ldapserver == 0)
        && ((*(*port).hba).ldapbasedn.is_null() || *(*(*port).hba).ldapbasedn == 0)
    {
        ereport!(
            LOG,
            errmsg!("LDAP server not specified, and no ldapbasedn")
        );
        return STATUS_ERROR;
    }
    // #endif

    /*
     * If we're using SRV records, we don't have a server name so we'll just
     * show an empty string in error messages.
     */
    server_name = if !(*(*port).hba).ldapserver.is_null() {
        (*(*port).hba).ldapserver
    } else {
        c"".as_ptr()
    };

    if (*(*port).hba).ldapport == 0 {
        if !(*(*port).hba).ldapscheme.is_null()
            && strcmp((*(*port).hba).ldapscheme, c"ldaps".as_ptr()) == 0
        {
            (*(*port).hba).ldapport = LDAPS_PORT;
        } else {
            (*(*port).hba).ldapport = LDAP_PORT;
        }
    }

    sendAuthRequest(port, AUTH_REQ_PASSWORD, null(), 0);

    passwd = recv_password_packet(port);
    if passwd.is_null() {
        return STATUS_EOF; /* client wouldn't send password */
    }

    if InitializeLDAPConnection(port, &mut ldap) == STATUS_ERROR {
        /* Error message already sent */
        pfree(passwd as *mut c_void);
        return STATUS_ERROR;
    }

    if !(*(*port).hba).ldapbasedn.is_null() {
        /*
         * First perform an LDAP search to find the DN for the user we are
         * trying to log in as.
         */
        let filter: *mut c_char;
        let mut search_message: *mut LDAPMessage;
        let entry: *mut LDAPMessage;
        let mut attributes: [*const c_char; 2] = [LDAP_NO_ATTRS.as_ptr(), null()];
        let dn: *mut c_char;
        let mut c: *mut c_char;
        let count: c_int;

        /*
         * Disallow any characters that we would otherwise need to escape,
         * since they aren't really reasonable in a username anyway. Allowing
         * them would make it possible to inject any kind of custom filters in
         * the LDAP filter.
         */
        c = (*port).user_name;
        while *c != 0 {
            if *c == b'*' as c_char
                || *c == b'(' as c_char
                || *c == b')' as c_char
                || *c == b'\\' as c_char
                || *c == b'/' as c_char
            {
                ereport!(
                    LOG,
                    errmsg!("invalid character in user name for LDAP authentication")
                );
                ldap_unbind(ldap);
                pfree(passwd as *mut c_void);
                return STATUS_ERROR;
            }
            c = c.add(1);
        }

        /*
         * Bind with a pre-defined username/password (if available) for
         * searching. If none is specified, this turns into an anonymous bind.
         */
        r = ldap_simple_bind_s(
            ldap,
            if !(*(*port).hba).ldapbinddn.is_null() {
                (*(*port).hba).ldapbinddn
            } else {
                c"".as_ptr() as *mut c_char
            },
            if !(*(*port).hba).ldapbindpasswd.is_null() {
                ldap_password_hook.unwrap()((*(*port).hba).ldapbindpasswd)
            } else {
                c"".as_ptr() as *mut c_char
            },
        );
        if r != LDAP_SUCCESS {
            // C also: errdetail_for_ldap(ldap)
            ereport!(
                LOG,
                errmsg!(
                    "could not perform initial LDAP bind for ldapbinddn \"{}\" on server \"{}\": {}",
                    CStr::from_ptr(if !(*(*port).hba).ldapbinddn.is_null() {
                        (*(*port).hba).ldapbinddn
                    } else {
                        c"".as_ptr() as *mut c_char
                    })
                    .to_string_lossy(),
                    CStr::from_ptr(server_name).to_string_lossy(),
                    CStr::from_ptr(ldap_err2string(r)).to_string_lossy()
                )
            );
            ldap_unbind(ldap);
            pfree(passwd as *mut c_void);
            return STATUS_ERROR;
        }

        /* Build a custom filter or a single attribute filter? */
        if !(*(*port).hba).ldapsearchfilter.is_null() {
            filter = FormatSearchFilter((*(*port).hba).ldapsearchfilter, (*port).user_name);
        } else if !(*(*port).hba).ldapsearchattribute.is_null() {
            // C also: psprintf("(%s=%s)", port->hba->ldapsearchattribute, port->user_name)
            filter = psprintf((*(*port).hba).ldapsearchattribute);
        } else {
            // C also: psprintf("(uid=%s)", port->user_name)
            filter = psprintf((*port).user_name);
        }

        search_message = null_mut();
        r = ldap_search_s(
            ldap,
            (*(*port).hba).ldapbasedn,
            (*(*port).hba).ldapscope,
            filter,
            attributes.as_mut_ptr(),
            0,
            &mut search_message,
        );

        if r != LDAP_SUCCESS {
            // C also: errdetail_for_ldap(ldap)
            ereport!(
                LOG,
                errmsg!(
                    "could not search LDAP for filter \"{}\" on server \"{}\": {}",
                    CStr::from_ptr(filter).to_string_lossy(),
                    CStr::from_ptr(server_name).to_string_lossy(),
                    CStr::from_ptr(ldap_err2string(r)).to_string_lossy()
                )
            );
            if !search_message.is_null() {
                ldap_msgfree(search_message);
            }
            ldap_unbind(ldap);
            pfree(passwd as *mut c_void);
            pfree(filter as *mut c_void);
            return STATUS_ERROR;
        }

        count = ldap_count_entries(ldap, search_message);
        if count != 1 {
            if count == 0 {
                // C also: errdetail("LDAP search for filter \"%s\" on server \"%s\" returned no entries.", filter, server_name)
                ereport!(
                    LOG,
                    errmsg!(
                        "LDAP user \"{}\" does not exist",
                        CStr::from_ptr((*port).user_name).to_string_lossy()
                    )
                );
            } else {
                // C also: errdetail_plural("...returned %d entry."/"...returned %d entries.", count, filter, server_name, count)
                ereport!(
                    LOG,
                    errmsg!(
                        "LDAP user \"{}\" is not unique",
                        CStr::from_ptr((*port).user_name).to_string_lossy()
                    )
                );
            }

            ldap_unbind(ldap);
            pfree(passwd as *mut c_void);
            pfree(filter as *mut c_void);
            ldap_msgfree(search_message);
            return STATUS_ERROR;
        }

        entry = ldap_first_entry(ldap, search_message);
        dn = ldap_get_dn(ldap, entry);
        if dn.is_null() {
            let mut error: c_int = 0;

            ldap_get_option(
                ldap,
                LDAP_OPT_ERROR_NUMBER,
                &mut error as *mut _ as *mut c_void,
            );
            // C also: errdetail_for_ldap(ldap)
            ereport!(
                LOG,
                errmsg!(
                    "could not get dn for the first entry matching \"{}\" on server \"{}\": {}",
                    CStr::from_ptr(filter).to_string_lossy(),
                    CStr::from_ptr(server_name).to_string_lossy(),
                    CStr::from_ptr(ldap_err2string(error)).to_string_lossy()
                )
            );
            ldap_unbind(ldap);
            pfree(passwd as *mut c_void);
            pfree(filter as *mut c_void);
            ldap_msgfree(search_message);
            return STATUS_ERROR;
        }
        fulluser = pstrdup(dn);

        pfree(filter as *mut c_void);
        ldap_memfree(dn as *mut c_void);
        ldap_msgfree(search_message);
    } else {
        // C also: psprintf("%s%s%s", ldapprefix?:"" , user_name, ldapsuffix?:"")
        fulluser = psprintf((*port).user_name);
    }

    r = ldap_simple_bind_s(ldap, fulluser, passwd);

    if r != LDAP_SUCCESS {
        // C also: errdetail_for_ldap(ldap)
        ereport!(
            LOG,
            errmsg!(
                "LDAP login failed for user \"{}\" on server \"{}\": {}",
                CStr::from_ptr(fulluser).to_string_lossy(),
                CStr::from_ptr(server_name).to_string_lossy(),
                CStr::from_ptr(ldap_err2string(r)).to_string_lossy()
            )
        );
        ldap_unbind(ldap);
        pfree(passwd as *mut c_void);
        pfree(fulluser as *mut c_void);
        return STATUS_ERROR;
    }

    /* Save the original bind DN as the authenticated identity. */
    set_authn_id(port, fulluser);

    ldap_unbind(ldap);
    pfree(passwd as *mut c_void);
    pfree(fulluser as *mut c_void);

    STATUS_OK
}

/*
 * Add a detail error message text to the current error if one can be
 * constructed from the LDAP 'diagnostic message'.
 */
unsafe fn errdetail_for_ldap(ldap: *mut LDAP) -> c_int {
    let mut message: *mut c_char = null_mut();
    let rc: c_int;

    rc = ldap_get_option(
        ldap,
        LDAP_OPT_DIAGNOSTIC_MESSAGE,
        &mut message as *mut _ as *mut c_void,
    );
    if rc == LDAP_SUCCESS && !message.is_null() {
        // C also: errdetail("LDAP diagnostics: %s", message)
        ldap_memfree(message as *mut c_void);
    }

    0
}
// #endif /* USE_LDAP */

// LDAP dependencies (OpenLDAP, not ported).
// TODO(pg-port): real LDAP bindings (USE_LDAP).
unsafe fn ldap_dn2domain(_dn: *const c_char, _domain: *mut *mut c_char) -> c_int {
    unimplemented!() // TODO(pg-port): OpenLDAP
}
unsafe fn ldap_domain2hostlist(_domain: *const c_char, _hostlist: *mut *mut c_char) -> c_int {
    unimplemented!() // TODO(pg-port): OpenLDAP
}
unsafe fn ldap_memfree(_p: *mut c_void) {
    unimplemented!() // TODO(pg-port): OpenLDAP
}
unsafe fn ldap_initialize(_ldap: *mut *mut LDAP, _uri: *const c_char) -> c_int {
    unimplemented!() // TODO(pg-port): OpenLDAP
}
unsafe fn ldap_err2string(_err: c_int) -> *const c_char {
    unimplemented!() // TODO(pg-port): OpenLDAP
}
unsafe fn ldap_set_option(_ldap: *mut LDAP, _option: c_int, _value: *const c_void) -> c_int {
    unimplemented!() // TODO(pg-port): OpenLDAP
}
unsafe fn ldap_get_option(_ldap: *mut LDAP, _option: c_int, _value: *mut c_void) -> c_int {
    unimplemented!() // TODO(pg-port): OpenLDAP
}
unsafe fn ldap_start_tls_s(
    _ldap: *mut LDAP,
    _serverctrls: *mut c_void,
    _clientctrls: *mut c_void,
) -> c_int {
    unimplemented!() // TODO(pg-port): OpenLDAP
}
unsafe fn ldap_unbind(_ldap: *mut LDAP) -> c_int {
    unimplemented!() // TODO(pg-port): OpenLDAP
}
unsafe fn ldap_simple_bind_s(
    _ldap: *mut LDAP,
    _who: *mut c_char,
    _passwd: *mut c_char,
) -> c_int {
    unimplemented!() // TODO(pg-port): OpenLDAP
}
unsafe fn ldap_search_s(
    _ldap: *mut LDAP,
    _base: *const c_char,
    _scope: c_int,
    _filter: *const c_char,
    _attrs: *mut *const c_char,
    _attrsonly: c_int,
    _res: *mut *mut LDAPMessage,
) -> c_int {
    unimplemented!() // TODO(pg-port): OpenLDAP
}
unsafe fn ldap_count_entries(_ldap: *mut LDAP, _res: *mut LDAPMessage) -> c_int {
    unimplemented!() // TODO(pg-port): OpenLDAP
}
unsafe fn ldap_first_entry(_ldap: *mut LDAP, _res: *mut LDAPMessage) -> *mut LDAPMessage {
    unimplemented!() // TODO(pg-port): OpenLDAP
}
unsafe fn ldap_get_dn(_ldap: *mut LDAP, _entry: *mut LDAPMessage) -> *mut c_char {
    unimplemented!() // TODO(pg-port): OpenLDAP
}
unsafe fn ldap_msgfree(_res: *mut LDAPMessage) -> c_int {
    unimplemented!() // TODO(pg-port): OpenLDAP
}

/*----------------------------------------------------------------
 * SSL client certificate authentication
 *----------------------------------------------------------------
 */
// #ifdef USE_SSL
unsafe fn CheckCertAuth(port: *mut Port) -> c_int {
    let mut status_check_usermap: c_int = STATUS_ERROR;
    let mut peer_username: *mut c_char = null_mut();

    Assert!(!(*port).ssl.is_null());

    /* select the correct field to compare */
    match (*(*port).hba).clientcertname {
        clientCertDN => {
            peer_username = (*port).peer_dn;
        }
        clientCertCN => {
            peer_username = (*port).peer_cn;
        }
        _ => {}
    }

    /* Make sure we have received a username in the certificate */
    if peer_username.is_null() || strlen(peer_username) == 0 {
        ereport!(
            LOG,
            errmsg!(
                "certificate authentication failed for user \"{}\": client certificate contains no user name",
                CStr::from_ptr((*port).user_name).to_string_lossy()
            )
        );
        return STATUS_ERROR;
    }

    if (*(*port).hba).auth_method == uaCert {
        /*
         * For cert auth, the client's Subject DN is always our authenticated
         * identity, even if we're only using its CN for authorization.  Set
         * it now, rather than waiting for check_usermap() below, because
         * authentication has already succeeded and we want the log file to
         * reflect that.
         */
        if (*port).peer_dn.is_null() {
            /*
             * This should not happen as both peer_dn and peer_cn should be
             * set in this context.
             */
            ereport!(
                LOG,
                errmsg!(
                    "certificate authentication failed for user \"{}\": unable to retrieve subject DN",
                    CStr::from_ptr((*port).user_name).to_string_lossy()
                )
            );
            return STATUS_ERROR;
        }

        set_authn_id(port, (*port).peer_dn);
    }

    /* Just pass the certificate cn/dn to the usermap check */
    status_check_usermap =
        check_usermap((*(*port).hba).usermap, (*port).user_name, peer_username, false);
    if status_check_usermap != STATUS_OK {
        /*
         * If clientcert=verify-full was specified and the authentication
         * method is other than uaCert, log the reason for rejecting the
         * authentication.
         */
        if (*(*port).hba).clientcert == clientCertFull && (*(*port).hba).auth_method != uaCert {
            match (*(*port).hba).clientcertname {
                clientCertDN => {
                    ereport!(
                        LOG,
                        errmsg!(
                            "certificate validation (clientcert=verify-full) failed for user \"{}\": DN mismatch",
                            CStr::from_ptr((*port).user_name).to_string_lossy()
                        )
                    );
                }
                clientCertCN => {
                    ereport!(
                        LOG,
                        errmsg!(
                            "certificate validation (clientcert=verify-full) failed for user \"{}\": CN mismatch",
                            CStr::from_ptr((*port).user_name).to_string_lossy()
                        )
                    );
                }
                _ => {}
            }
        }
    }
    status_check_usermap
}
// #endif

/*----------------------------------------------------------------
 * RADIUS authentication
 *----------------------------------------------------------------
 */

/*
 * RADIUS authentication is described in RFC2865 (and several others).
 */

const RADIUS_VECTOR_LENGTH: usize = 16;
const RADIUS_HEADER_LENGTH: usize = 20;
const RADIUS_MAX_PASSWORD_LENGTH: usize = 128;

/* Maximum size of a RADIUS packet we will create or accept */
const RADIUS_BUFFER_SIZE: usize = 1024;

#[repr(C)]
struct radius_attribute {
    attribute: uint8,
    length: uint8,
    data: [uint8; 0], /* FLEXIBLE_ARRAY_MEMBER */
}

#[repr(C)]
struct radius_packet {
    code: uint8,
    id: uint8,
    length: uint16,
    vector: [uint8; RADIUS_VECTOR_LENGTH],
    /* this is a bit longer than strictly necessary: */
    pad: [c_char; RADIUS_BUFFER_SIZE - RADIUS_VECTOR_LENGTH],
}

/* RADIUS packet types */
const RADIUS_ACCESS_REQUEST: uint8 = 1;
const RADIUS_ACCESS_ACCEPT: uint8 = 2;
const RADIUS_ACCESS_REJECT: uint8 = 3;

/* RADIUS attributes */
const RADIUS_USER_NAME: uint8 = 1;
const RADIUS_PASSWORD: uint8 = 2;
const RADIUS_SERVICE_TYPE: uint8 = 6;
const RADIUS_NAS_IDENTIFIER: uint8 = 32;

/* RADIUS service types */
const RADIUS_AUTHENTICATE_ONLY: uint32 = 8;

/* Seconds to wait - XXX: should be in a config variable! */
const RADIUS_TIMEOUT: c_long = 3;

unsafe fn radius_add_attribute(
    packet: *mut radius_packet,
    r#type: uint8,
    data: *const c_uchar,
    len: c_int,
) {
    let attr: *mut radius_attribute;

    if (*packet).length as c_int + len > RADIUS_BUFFER_SIZE as c_int {
        /*
         * With remotely realistic data, this can never happen. But catch it
         * just to make sure we don't overrun a buffer. We'll just skip adding
         * the broken attribute, which will in the end cause authentication to
         * fail.
         */
        elog!(
            WARNING,
            "adding attribute code {} with length {} to radius packet would create oversize packet, ignoring",
            r#type,
            len
        );
        return;
    }

    attr = (packet as *mut c_uchar).add((*packet).length as usize) as *mut radius_attribute;
    (*attr).attribute = r#type;
    (*attr).length = (len + 2) as uint8; /* total size includes type and length */
    memcpy((*attr).data.as_mut_ptr() as *mut c_void, data as *const c_void, len as Size);
    (*packet).length += (*attr).length as uint16;
}

unsafe fn CheckRADIUSAuth(port: *mut Port) -> c_int {
    let passwd: *mut c_char;
    let mut secrets: *mut ListCell;
    let mut radiusports: *mut ListCell;
    let mut identifiers: *mut ListCell;

    /* Make sure struct alignment is correct */
    Assert!(core::mem::offset_of!(radius_packet, vector) == 4);

    /* Verify parameters */
    if (*(*port).hba).radiusservers == NIL {
        ereport!(LOG, errmsg!("RADIUS server not specified"));
        return STATUS_ERROR;
    }

    if (*(*port).hba).radiussecrets == NIL {
        ereport!(LOG, errmsg!("RADIUS secret not specified"));
        return STATUS_ERROR;
    }

    /* Send regular password request to client, and get the response */
    sendAuthRequest(port, AUTH_REQ_PASSWORD, null(), 0);

    passwd = recv_password_packet(port);
    if passwd.is_null() {
        return STATUS_EOF; /* client wouldn't send password */
    }

    if strlen(passwd) > RADIUS_MAX_PASSWORD_LENGTH as Size {
        ereport!(
            LOG,
            errmsg!(
                "RADIUS authentication does not support passwords longer than {} characters",
                RADIUS_MAX_PASSWORD_LENGTH
            )
        );
        pfree(passwd as *mut c_void);
        return STATUS_ERROR;
    }

    /*
     * Loop over and try each server in order.
     */
    secrets = list_head((*(*port).hba).radiussecrets);
    radiusports = list_head((*(*port).hba).radiusports);
    identifiers = list_head((*(*port).hba).radiusidentifiers);
    foreach!(server, (*(*port).hba).radiusservers, {
        let ret: c_int = PerformRadiusTransaction(
            lfirst(current_cell!(server)) as *const c_char,
            lfirst(secrets) as *const c_char,
            if !radiusports.is_null() {
                lfirst(radiusports) as *const c_char
            } else {
                null()
            },
            if !identifiers.is_null() {
                lfirst(identifiers) as *const c_char
            } else {
                null()
            },
            (*port).user_name,
            passwd,
        );

        /*------
         * STATUS_OK = Login OK
         * STATUS_ERROR = Login not OK, but try next server
         * STATUS_EOF = Login not OK, and don't try next server
         *------
         */
        if ret == STATUS_OK {
            set_authn_id(port, (*port).user_name);

            pfree(passwd as *mut c_void);
            return STATUS_OK;
        } else if ret == STATUS_EOF {
            pfree(passwd as *mut c_void);
            return STATUS_ERROR;
        }

        /*
         * secret, port and identifiers either have length 0 (use default),
         * length 1 (use the same everywhere) or the same length as servers.
         * So if the length is >1, we advance one step. In other cases, we
         * don't and will then reuse the correct value.
         */
        if list_length((*(*port).hba).radiussecrets) > 1 {
            secrets = lnext((*(*port).hba).radiussecrets, secrets);
        }
        if list_length((*(*port).hba).radiusports) > 1 {
            radiusports = lnext((*(*port).hba).radiusports, radiusports);
        }
        if list_length((*(*port).hba).radiusidentifiers) > 1 {
            identifiers = lnext((*(*port).hba).radiusidentifiers, identifiers);
        }
    });

    /* No servers left to try, so give up */
    pfree(passwd as *mut c_void);
    STATUS_ERROR
}

unsafe fn PerformRadiusTransaction(
    server: *const c_char,
    secret: *const c_char,
    mut portstr: *const c_char,
    mut identifier: *const c_char,
    user_name: *const c_char,
    passwd: *const c_char,
) -> c_int {
    let mut radius_send_pack: radius_packet = core::mem::zeroed();
    let mut radius_recv_pack: radius_packet = core::mem::zeroed();
    let packet: *mut radius_packet = &mut radius_send_pack;
    let receivepacket: *mut radius_packet = &mut radius_recv_pack;
    let radius_buffer: *mut c_void = &mut radius_send_pack as *mut _ as *mut c_void;
    let receive_buffer: *mut c_void = &mut radius_recv_pack as *mut _ as *mut c_void;
    let service: int32 = pg_hton32(RADIUS_AUTHENTICATE_ONLY) as int32;
    let mut cryptvector: *mut uint8;
    let encryptedpasswordlen: c_int;
    let mut encryptedpassword: [uint8; RADIUS_MAX_PASSWORD_LENGTH] = [0; RADIUS_MAX_PASSWORD_LENGTH];
    let mut md5trailer: *mut uint8;
    let mut packetlength: c_int;
    let sock: c_int;

    let mut localaddr: sockaddr_in6 = core::mem::zeroed();
    let mut remoteaddr: sockaddr_in6 = core::mem::zeroed();
    let mut hint: addrinfo = core::mem::zeroed();
    let mut serveraddrs: *mut addrinfo = null_mut();
    let r#port: c_int;
    let mut addrsize: socklen_t;
    let mut fdset: fd_set = core::mem::zeroed();
    let mut endtime: timeval = core::mem::zeroed();
    let mut i: c_int;
    let mut j: c_int;
    let mut r: c_int;

    /* Assign default values */
    if portstr.is_null() {
        portstr = c"1812".as_ptr();
    }
    if identifier.is_null() {
        identifier = c"postgresql".as_ptr();
    }

    memset(
        &mut hint as *mut _ as *mut c_void,
        0,
        core::mem::size_of_val(&hint),
    );
    hint.ai_socktype = SOCK_DGRAM;
    hint.ai_family = AF_UNSPEC;
    r#port = atoi(portstr);

    r = pg_getaddrinfo_all(server, portstr, &hint, &mut serveraddrs);
    if r != 0 || serveraddrs.is_null() {
        ereport!(
            LOG,
            errmsg!(
                "could not translate RADIUS server name \"{}\" to address: {}",
                CStr::from_ptr(server).to_string_lossy(),
                CStr::from_ptr(gai_strerror(r)).to_string_lossy()
            )
        );
        if !serveraddrs.is_null() {
            pg_freeaddrinfo_all(hint.ai_family, serveraddrs);
        }
        return STATUS_ERROR;
    }
    /* XXX: add support for multiple returned addresses? */

    /* Construct RADIUS packet */
    (*packet).code = RADIUS_ACCESS_REQUEST;
    (*packet).length = RADIUS_HEADER_LENGTH as uint16;
    if !pg_strong_random((*packet).vector.as_mut_ptr() as *mut c_void, RADIUS_VECTOR_LENGTH) {
        ereport!(LOG, errmsg!("could not generate random encryption vector"));
        pg_freeaddrinfo_all(hint.ai_family, serveraddrs);
        return STATUS_ERROR;
    }
    (*packet).id = (*packet).vector[0];
    radius_add_attribute(
        packet,
        RADIUS_SERVICE_TYPE,
        &service as *const _ as *const c_uchar,
        core::mem::size_of_val(&service) as c_int,
    );
    radius_add_attribute(
        packet,
        RADIUS_USER_NAME,
        user_name as *const c_uchar,
        strlen(user_name) as c_int,
    );
    radius_add_attribute(
        packet,
        RADIUS_NAS_IDENTIFIER,
        identifier as *const c_uchar,
        strlen(identifier) as c_int,
    );

    /*
     * RADIUS password attributes are calculated as: e[0] = p[0] XOR
     * MD5(secret + Request Authenticator) for the first group of 16 octets,
     * and then: e[i] = p[i] XOR MD5(secret + e[i-1]) for the following ones
     * (if necessary)
     */
    encryptedpasswordlen = ((strlen(passwd) as c_int + RADIUS_VECTOR_LENGTH as c_int - 1)
        / RADIUS_VECTOR_LENGTH as c_int)
        * RADIUS_VECTOR_LENGTH as c_int;
    cryptvector = palloc(strlen(secret) + RADIUS_VECTOR_LENGTH) as *mut uint8;
    memcpy(cryptvector as *mut c_void, secret as *const c_void, strlen(secret));

    /* for the first iteration, we use the Request Authenticator vector */
    md5trailer = (*packet).vector.as_mut_ptr();
    i = 0;
    while i < encryptedpasswordlen {
        let mut errstr: *const c_char = null();

        memcpy(
            cryptvector.add(strlen(secret)) as *mut c_void,
            md5trailer as *const c_void,
            RADIUS_VECTOR_LENGTH,
        );

        /*
         * .. and for subsequent iterations the result of the previous XOR
         * (calculated below)
         */
        md5trailer = encryptedpassword.as_mut_ptr().add(i as usize);

        if !pg_md5_binary(
            cryptvector as *const c_void,
            strlen(secret) + RADIUS_VECTOR_LENGTH,
            encryptedpassword.as_mut_ptr().add(i as usize) as *mut c_void,
            &mut errstr,
        ) {
            ereport!(
                LOG,
                errmsg!(
                    "could not perform MD5 encryption of password: {}",
                    CStr::from_ptr(errstr).to_string_lossy()
                )
            );
            pfree(cryptvector as *mut c_void);
            pg_freeaddrinfo_all(hint.ai_family, serveraddrs);
            return STATUS_ERROR;
        }

        j = i;
        while j < i + RADIUS_VECTOR_LENGTH as c_int {
            if (j as Size) < strlen(passwd) {
                encryptedpassword[j as usize] =
                    (*passwd.add(j as usize) as uint8) ^ encryptedpassword[j as usize];
            } else {
                encryptedpassword[j as usize] = (b'\0' as uint8) ^ encryptedpassword[j as usize];
            }
            j += 1;
        }
        i += RADIUS_VECTOR_LENGTH as c_int;
    }
    pfree(cryptvector as *mut c_void);

    radius_add_attribute(
        packet,
        RADIUS_PASSWORD,
        encryptedpassword.as_ptr(),
        encryptedpasswordlen,
    );

    /* Length needs to be in network order on the wire */
    packetlength = (*packet).length as c_int;
    (*packet).length = pg_hton16((*packet).length);

    sock = socket((*serveraddrs.add(0)).ai_family, SOCK_DGRAM, 0);
    if sock == PGINVALID_SOCKET {
        ereport!(LOG, errmsg!("could not create RADIUS socket: {}", "%m"));
        pg_freeaddrinfo_all(hint.ai_family, serveraddrs);
        return STATUS_ERROR;
    }

    memset(
        &mut localaddr as *mut _ as *mut c_void,
        0,
        core::mem::size_of_val(&localaddr),
    );
    localaddr.sin6_family = (*serveraddrs.add(0)).ai_family as c_ushort;
    localaddr.sin6_addr = in6addr_any;
    if localaddr.sin6_family == AF_INET6 as c_ushort {
        addrsize = core::mem::size_of::<sockaddr_in6>() as socklen_t;
    } else {
        addrsize = core::mem::size_of::<sockaddr_in>() as socklen_t;
    }

    if bind(sock, &localaddr as *const _ as *const c_void, addrsize) != 0 {
        ereport!(LOG, errmsg!("could not bind local RADIUS socket: {}", "%m"));
        closesocket(sock);
        pg_freeaddrinfo_all(hint.ai_family, serveraddrs);
        return STATUS_ERROR;
    }

    if sendto(
        sock,
        radius_buffer,
        packetlength as Size,
        0,
        (*serveraddrs.add(0)).ai_addr,
        (*serveraddrs.add(0)).ai_addrlen,
    ) < 0
    {
        ereport!(LOG, errmsg!("could not send RADIUS packet: {}", "%m"));
        closesocket(sock);
        pg_freeaddrinfo_all(hint.ai_family, serveraddrs);
        return STATUS_ERROR;
    }

    /* Don't need the server address anymore */
    pg_freeaddrinfo_all(hint.ai_family, serveraddrs);

    /*
     * Figure out at what time we should time out. We can't just use a single
     * call to select() with a timeout, since somebody can be sending invalid
     * packets to our port thus causing us to retry in a loop and never time
     * out.
     *
     * XXX: Using WaitLatchOrSocket() and doing a CHECK_FOR_INTERRUPTS() if
     * the latch was set would improve the responsiveness to
     * timeouts/cancellations.
     */
    gettimeofday(&mut endtime, null_mut());
    endtime.tv_sec += RADIUS_TIMEOUT;

    loop {
        let mut timeout: timeval = core::mem::zeroed();
        let mut now: timeval = core::mem::zeroed();
        let timeoutval: int64;
        let mut errstr: *const c_char = null();

        gettimeofday(&mut now, null_mut());
        timeoutval = (endtime.tv_sec as int64 * 1000000 + endtime.tv_usec as int64)
            - (now.tv_sec as int64 * 1000000 + now.tv_usec as int64);
        if timeoutval <= 0 {
            ereport!(
                LOG,
                errmsg!(
                    "timeout waiting for RADIUS response from {}",
                    CStr::from_ptr(server).to_string_lossy()
                )
            );
            closesocket(sock);
            return STATUS_ERROR;
        }
        timeout.tv_sec = (timeoutval / 1000000) as c_long;
        timeout.tv_usec = (timeoutval % 1000000) as c_long;

        FD_ZERO(&mut fdset);
        FD_SET(sock, &mut fdset);

        r = select(sock + 1, &mut fdset, null_mut(), null_mut(), &mut timeout);
        if r < 0 {
            if errno() == EINTR {
                continue;
            }

            /* Anything else is an actual error */
            ereport!(
                LOG,
                errmsg!("could not check status on RADIUS socket: {}", "%m")
            );
            closesocket(sock);
            return STATUS_ERROR;
        }
        if r == 0 {
            ereport!(
                LOG,
                errmsg!(
                    "timeout waiting for RADIUS response from {}",
                    CStr::from_ptr(server).to_string_lossy()
                )
            );
            closesocket(sock);
            return STATUS_ERROR;
        }

        /*
         * Attempt to read the response packet, and verify the contents.
         *
         * Any packet that's not actually a RADIUS packet, or otherwise does
         * not validate as an explicit reject, is just ignored and we retry
         * for another packet (until we reach the timeout). This is to avoid
         * the possibility to denial-of-service the login by flooding the
         * server with invalid packets on the port that we're expecting the
         * RADIUS response on.
         */

        addrsize = core::mem::size_of_val(&remoteaddr) as socklen_t;
        packetlength = recvfrom(
            sock,
            receive_buffer,
            RADIUS_BUFFER_SIZE,
            0,
            &mut remoteaddr as *mut _ as *mut c_void,
            &mut addrsize,
        ) as c_int;
        if packetlength < 0 {
            ereport!(LOG, errmsg!("could not read RADIUS response: {}", "%m"));
            closesocket(sock);
            return STATUS_ERROR;
        }

        if remoteaddr.sin6_port != pg_hton16(r#port as uint16) {
            ereport!(
                LOG,
                errmsg!(
                    "RADIUS response from {} was sent from incorrect port: {}",
                    CStr::from_ptr(server).to_string_lossy(),
                    pg_ntoh16(remoteaddr.sin6_port)
                )
            );
            continue;
        }

        if packetlength < RADIUS_HEADER_LENGTH as c_int {
            ereport!(
                LOG,
                errmsg!(
                    "RADIUS response from {} too short: {}",
                    CStr::from_ptr(server).to_string_lossy(),
                    packetlength
                )
            );
            continue;
        }

        if packetlength != pg_ntoh16((*receivepacket).length) as c_int {
            ereport!(
                LOG,
                errmsg!(
                    "RADIUS response from {} has corrupt length: {} (actual length {})",
                    CStr::from_ptr(server).to_string_lossy(),
                    pg_ntoh16((*receivepacket).length),
                    packetlength
                )
            );
            continue;
        }

        if (*packet).id != (*receivepacket).id {
            ereport!(
                LOG,
                errmsg!(
                    "RADIUS response from {} is to a different request: {} (should be {})",
                    CStr::from_ptr(server).to_string_lossy(),
                    (*receivepacket).id,
                    (*packet).id
                )
            );
            continue;
        }

        /*
         * Verify the response authenticator, which is calculated as
         * MD5(Code+ID+Length+RequestAuthenticator+Attributes+Secret)
         */
        cryptvector = palloc(packetlength as Size + strlen(secret)) as *mut uint8;

        memcpy(cryptvector as *mut c_void, receivepacket as *const c_void, 4); /* code+id+length */
        memcpy(
            cryptvector.add(4) as *mut c_void,
            (*packet).vector.as_ptr() as *const c_void,
            RADIUS_VECTOR_LENGTH,
        ); /* request authenticator, from original packet */
        if packetlength > RADIUS_HEADER_LENGTH as c_int {
            /* there may be no attributes at all */
            memcpy(
                cryptvector.add(RADIUS_HEADER_LENGTH) as *mut c_void,
                (receive_buffer as *const c_char).add(RADIUS_HEADER_LENGTH) as *const c_void,
                packetlength as Size - RADIUS_HEADER_LENGTH as Size,
            );
        }
        memcpy(
            cryptvector.add(packetlength as usize) as *mut c_void,
            secret as *const c_void,
            strlen(secret),
        );

        if !pg_md5_binary(
            cryptvector as *const c_void,
            packetlength as Size + strlen(secret),
            encryptedpassword.as_mut_ptr() as *mut c_void,
            &mut errstr,
        ) {
            ereport!(
                LOG,
                errmsg!(
                    "could not perform MD5 encryption of received packet: {}",
                    CStr::from_ptr(errstr).to_string_lossy()
                )
            );
            pfree(cryptvector as *mut c_void);
            continue;
        }
        pfree(cryptvector as *mut c_void);

        if memcmp(
            (*receivepacket).vector.as_ptr() as *const c_void,
            encryptedpassword.as_ptr() as *const c_void,
            RADIUS_VECTOR_LENGTH,
        ) != 0
        {
            ereport!(
                LOG,
                errmsg!(
                    "RADIUS response from {} has incorrect MD5 signature",
                    CStr::from_ptr(server).to_string_lossy()
                )
            );
            continue;
        }

        if (*receivepacket).code == RADIUS_ACCESS_ACCEPT {
            closesocket(sock);
            return STATUS_OK;
        } else if (*receivepacket).code == RADIUS_ACCESS_REJECT {
            closesocket(sock);
            return STATUS_EOF;
        } else {
            ereport!(
                LOG,
                errmsg!(
                    "RADIUS response from {} has invalid code ({}) for user \"{}\"",
                    CStr::from_ptr(server).to_string_lossy(),
                    (*receivepacket).code,
                    CStr::from_ptr(user_name).to_string_lossy()
                )
            );
            continue;
        }
    } /* while (true) */
}

// RADIUS socket-address dependencies (sys/socket.h / netinet/in.h).
type socklen_t = c_uint;
#[repr(C)]
struct sockaddr_in6 {
    sin6_len: u8,
    sin6_family: c_ushort,
    sin6_port: uint16,
    sin6_flowinfo: u32,
    sin6_addr: in6_addr,
    sin6_scope_id: u32,
}
#[repr(C)]
struct sockaddr_in {
    _opaque: [u8; 16],
}
#[repr(C)]
#[derive(Clone, Copy)]
struct in6_addr {
    s6_addr: [u8; 16],
}
const in6addr_any: in6_addr = in6_addr { s6_addr: [0; 16] };
unsafe fn FD_ZERO(_set: *mut fd_set) {
    // TODO(pg-port): libc FD_ZERO macro
}
unsafe fn FD_SET(_fd: c_int, _set: *mut fd_set) {
    // TODO(pg-port): libc FD_SET macro
}
