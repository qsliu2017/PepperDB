//! libpq/libpq-be.h - backend-internal structures/externs for client authentication.

use std::ffi::{c_char, c_int, c_void};

use crate::c::{uint8, Size};
use crate::common::scram_common::SCRAM_MAX_KEY_LEN;
use crate::nodes::pg_list::List;
use crate::port::noblock::pgsocket;

// ---------------------------------------------------------------------------
// Locally-stubbed types for not-yet-ported headers / system / SSL / GSSAPI.
// TODO: dedup once pqcomm.h, hba.h, pg-gssapi.h, and SSL glue are ported.
// ---------------------------------------------------------------------------

// From libpq/pqcomm.h (not yet ported).
pub type ProtocolVersion = u32;
pub use crate::libpq::hba::SockAddr; // canonical SockAddr (pqcomm.h)

// From libpq/hba.h (not yet ported).
pub type UserAuth = c_int; // TODO: dedup with hba.h UserAuth enum
// HbaLine canonical def lives in libpq::hba (hba.h home).
pub use crate::libpq::hba::HbaLine;

// GSSAPI / SSPI state (system gssapi.h, not ported).
// pg_gssinfo's real (still local) definition lives in libpq/auth.rs.
pub use crate::libpq::auth::pg_gssinfo; // TODO: dedup with pg-gssapi.h

// OpenSSL types (system openssl/ssl.h, not ported).
pub type SSL = c_void; // TODO: dedup with openssl
pub type SSL_CTX = c_void; // TODO: dedup with openssl
pub type X509 = c_void; // TODO: dedup with openssl

// From <sys/types.h>: ssize_t.
pub type ssize_t = isize;

/*
 * ClientConnectionInfo includes the fields describing the client connection
 * that are copied over to parallel workers as nothing from Port does that.
 */
#[repr(C)]
pub struct ClientConnectionInfo {
    /* Authenticated identity. */
    pub authn_id: *const c_char,

    /* The HBA method that determined the above authn_id. */
    pub auth_method: UserAuth,
}

/*
 * The Port structure holds state information about a client connection in a
 * backend process.  It is available in the global variable MyProcPort.
 */
#[repr(C)]
pub struct Port {
    pub sock: pgsocket,                  /* File descriptor */
    pub noblock: bool,                   /* is the socket in non-blocking mode? */
    pub proto: ProtocolVersion,          /* FE/BE protocol version */
    pub laddr: SockAddr,                 /* local addr (postmaster) */
    pub raddr: SockAddr,                 /* remote addr (client) */
    pub remote_host: *mut c_char,        /* name (or ip addr) of remote host */
    pub remote_hostname: *mut c_char,    /* name (not ip addr) of remote host */
    pub remote_hostname_resolv: c_int,   /* see header */
    pub remote_hostname_errcode: c_int,  /* see header */
    pub remote_port: *mut c_char,        /* text rep of remote port */

    /* local_host is filled only if needed (see log_status_format) */
    pub local_host: [c_char; 64], /* ip addr of local socket for client conn */

    /*
     * Information that needs to be saved from the startup packet and passed
     * into backend execution.
     */
    pub database_name: *mut c_char,
    pub user_name: *mut c_char,
    pub cmdline_options: *mut c_char,
    pub guc_options: *mut List,

    /* The startup packet application name. */
    pub application_name: *mut c_char,

    /* Information that needs to be held during the authentication cycle. */
    pub hba: *mut HbaLine,

    /* TCP keepalive and user timeout settings. */
    pub default_keepalives_idle: c_int,
    pub default_keepalives_interval: c_int,
    pub default_keepalives_count: c_int,
    pub default_tcp_user_timeout: c_int,
    pub keepalives_idle: c_int,
    pub keepalives_interval: c_int,
    pub keepalives_count: c_int,
    pub tcp_user_timeout: c_int,

    /* SCRAM structures. */
    pub scram_ClientKey: [uint8; SCRAM_MAX_KEY_LEN],
    pub scram_ServerKey: [uint8; SCRAM_MAX_KEY_LEN],
    pub has_scram_keys: bool, /* true if the above two are valid */

    /*
     * GSSAPI structures.  Even when GSSAPI is not compiled in, store a NULL
     * pointer to keep struct offsets the same (for extension ABI compat).
     */
    pub gss: *mut pg_gssinfo,

    /* SSL structures. */
    pub ssl_in_use: bool,
    pub peer_cn: *mut c_char,
    pub peer_dn: *mut c_char,
    pub peer_cert_valid: bool,
    pub alpn_used: bool,
    pub last_read_was_eof: bool,

    /*
     * OpenSSL structures.  As with GSSAPI above, NULL pointers are stored when
     * SSL support is not enabled.
     */
    pub ssl: *mut SSL,
    pub peer: *mut X509,

    /*
     * raw_buf is data that was previously read and buffered in a higher layer
     * but then "unread" and needs to be read again during SSL setup.
     */
    pub raw_buf: *mut c_char,
    pub raw_buf_consumed: ssize_t,
    pub raw_buf_remaining: ssize_t,
}

/*
 * ClientSocket holds a socket for an accepted connection, along with the
 * information about the remote endpoint.
 */
#[repr(C)]
pub struct ClientSocket {
    pub sock: pgsocket,  /* File descriptor */
    pub raddr: SockAddr, /* remote addr (client) */
}

/*
 *	Hardcoded DH parameters, used in ephemeral DH keying.
 *	This is the 2048-bit DH parameter from RFC 3526.
 */
pub const FILE_DH2048: &str = "-----BEGIN DH PARAMETERS-----\n\
MIIBCAKCAQEA///////////JD9qiIWjCNMTGYouA3BzRKQJOCIpnzHQCC76mOxOb\n\
IlFKCHmONATd75UZs806QxswKwpt8l8UN0/hNW1tUcJF5IW1dmJefsb0TELppjft\n\
awv/XLb0Brft7jhr+1qJn6WunyQRfEsf5kkoZlHs5Fs9wgB8uKFjvwWY2kg2HFXT\n\
mmkWP6j9JM9fg2VdI9yjrZYcYvNWIIVSu57VKQdwlpZtZww1Tkq8mATxdGwIyhgh\n\
fDKQXkYuNs474553LBgOhgObJ4Oi7Aeij7XFXfBvTFLJ3ivL9pVYFxg5lUl86pVq\n\
5RXSJhiY+gUQFXKOWoqsqmj//////////wIBAg==\n\
-----END DH PARAMETERS-----\n";

/*
 * These functions are implemented by the glue code specific to each
 * SSL implementation (e.g. be-secure-openssl.c).
 */

/* Initialize global SSL context. */
pub unsafe fn be_tls_init(isServerStart: bool) -> c_int { unimplemented!() }

/* Destroy global SSL context, if any. */
pub unsafe fn be_tls_destroy() { unimplemented!() }

/* Attempt to negotiate SSL connection. */
pub unsafe fn be_tls_open_server(port: *mut Port) -> c_int { unimplemented!() }

/* Close SSL connection. */
pub unsafe fn be_tls_close(port: *mut Port) { unimplemented!() }

/* Read data from a secure connection. */
pub unsafe fn be_tls_read(
    port: *mut Port,
    ptr: *mut c_void,
    len: Size,
    waitfor: *mut c_int,
) -> ssize_t { unimplemented!() }

/* Write data to a secure connection. */
pub unsafe fn be_tls_write(
    port: *mut Port,
    ptr: *const c_void,
    len: Size,
    waitfor: *mut c_int,
) -> ssize_t { unimplemented!() }

/* Return information about the SSL connection. */
pub unsafe fn be_tls_get_cipher_bits(port: *mut Port) -> c_int { unimplemented!() }
pub unsafe fn be_tls_get_version(port: *mut Port) -> *const c_char { unimplemented!() }
pub unsafe fn be_tls_get_cipher(port: *mut Port) -> *const c_char { unimplemented!() }
pub unsafe fn be_tls_get_peer_subject_name(port: *mut Port, ptr: *mut c_char, len: Size) { unimplemented!() }
pub unsafe fn be_tls_get_peer_issuer_name(port: *mut Port, ptr: *mut c_char, len: Size) { unimplemented!() }
pub unsafe fn be_tls_get_peer_serial(port: *mut Port, ptr: *mut c_char, len: Size) { unimplemented!() }

/*
 * Get the server certificate hash for SCRAM channel binding type
 * tls-server-end-point.
 */
pub unsafe fn be_tls_get_certificate_hash(port: *mut Port, len: *mut Size) -> *mut c_char { unimplemented!() }

/* init hook for SSL, the default sets the password callback if appropriate */
pub type openssl_tls_init_hook_typ =
    Option<unsafe extern "C" fn(context: *mut SSL_CTX, isServerStart: bool)>;

#[allow(improper_ctypes)]
extern "C" {
    pub static mut openssl_tls_init_hook: openssl_tls_init_hook_typ;
}

/*
 * Return information about the GSSAPI authenticated connection
 */
pub unsafe fn be_gssapi_get_auth(port: *mut Port) -> bool { unimplemented!() }
pub unsafe fn be_gssapi_get_enc(port: *mut Port) -> bool { unimplemented!() }
pub unsafe fn be_gssapi_get_princ(port: *mut Port) -> *const c_char { unimplemented!() }
pub unsafe fn be_gssapi_get_delegation(port: *mut Port) -> bool { unimplemented!() }

/* Read and write to a GSSAPI-encrypted connection. */
pub unsafe fn be_gssapi_read(port: *mut Port, ptr: *mut c_void, len: Size) -> ssize_t { unimplemented!() }
pub unsafe fn be_gssapi_write(port: *mut Port, ptr: *const c_void, len: Size) -> ssize_t { unimplemented!() }

extern "C" {
    pub static mut FrontendProtocol: ProtocolVersion;
    pub static mut MyClientConnectionInfo: ClientConnectionInfo;
}

/* TCP keepalives configuration. These are no-ops on an AF_UNIX socket. */

pub unsafe fn pq_getkeepalivesidle(port: *mut Port) -> c_int { crate::libpq::pqcomm::pq_getkeepalivesidle(port as _) }
pub unsafe fn pq_getkeepalivesinterval(port: *mut Port) -> c_int { crate::libpq::pqcomm::pq_getkeepalivesinterval(port as _) }
pub unsafe fn pq_getkeepalivescount(port: *mut Port) -> c_int { crate::libpq::pqcomm::pq_getkeepalivescount(port as _) }
pub unsafe fn pq_gettcpusertimeout(port: *mut Port) -> c_int { crate::libpq::pqcomm::pq_gettcpusertimeout(port as _) }

pub unsafe fn pq_setkeepalivesidle(idle: c_int, port: *mut Port) -> c_int { crate::libpq::pqcomm::pq_setkeepalivesidle(idle as _, port as _) }
pub unsafe fn pq_setkeepalivesinterval(interval: c_int, port: *mut Port) -> c_int { crate::libpq::pqcomm::pq_setkeepalivesinterval(interval as _, port as _) }
pub unsafe fn pq_setkeepalivescount(count: c_int, port: *mut Port) -> c_int { crate::libpq::pqcomm::pq_setkeepalivescount(count as _, port as _) }
pub unsafe fn pq_settcpusertimeout(timeout: c_int, port: *mut Port) -> c_int { crate::libpq::pqcomm::pq_settcpusertimeout(timeout as _, port as _) }
