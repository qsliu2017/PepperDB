//! Translated from PostgreSQL src/include/libpq/libpq-be.h
//!
//! Backend connection state. In-memory structs (not on-disk / not wire-format),
//! modeled idiomatically. Resolves the level-4 forward-decls
//! `crate::libpq::libpq_be::Port` and `crate::libpq::libpq_be::ClientSocket`.
//!
//! Raw FFI bits for SSL/GSSAPI are stubbed: OpenSSL `SSL`/`X509` and the GSSAPI
//! credential/context handles have no Rust definition yet, so the corresponding
//! fields become `Option<()>` placeholders behind a note. CAC_state lives in
//! `crate::tcop::backend_startup` and is not redefined here.

use crate::common::scram_common::SCRAM_MAX_KEY_LEN;
use crate::libpq::hba::{HbaLine, UserAuth};
use crate::libpq::pqcomm::{ProtocolVersion, SockAddr};
use crate::port::pgsocket;

/// GSSAPI-specific state information (`pg_gssinfo`). The raw GSSAPI handles
/// (`gss_cred_id_t`, `gss_ctx_id_t`, `gss_name_t`) are opaque FFI types with no
/// Rust definition; stubbed as `Option<()>` until a GSSAPI binding exists.
/// Gated as in C behind GSS/SSPI; we keep it unconditionally as in-memory state.
pub struct PgGssinfo {
    /// GSSAPI output token buffer (raw `gss_buffer_desc`).
    pub outbuf: Option<()>, // TODO(ffi): gss_buffer_desc
    /// GSSAPI connection cred's.
    pub cred: Option<()>, // TODO(ffi): gss_cred_id_t
    /// GSSAPI connection context.
    pub ctx: Option<()>, // TODO(ffi): gss_ctx_id_t
    /// GSSAPI client name.
    pub name: Option<()>, // TODO(ffi): gss_name_t
    /// GSSAPI principal used for auth, None if GSSAPI auth was not used.
    pub princ: Option<String>,
    /// GSSAPI authentication used.
    pub auth: bool,
    /// GSSAPI encryption in use.
    pub enc: bool,
    /// GSSAPI delegated credentials.
    pub delegated_creds: bool,
}

/// `ClientConnectionInfo` - fields describing the client connection that are
/// copied to parallel workers (nothing from Port does that).
pub struct ClientConnectionInfo {
    /// Authenticated identity; None if not actually authenticated (e.g. trust).
    pub authn_id: Option<String>,
    /// The HBA method that determined `authn_id` (meaningful only if set).
    pub auth_method: UserAuth,
}

/// `Port` - state about a client connection in a backend process (was the global
/// `MyProcPort`). In-memory.
pub struct Port {
    /// File descriptor.
    pub sock: pgsocket,
    /// Is the socket in non-blocking mode?
    pub noblock: bool,
    /// FE/BE protocol version.
    pub proto: ProtocolVersion,
    /// Local addr (postmaster).
    pub laddr: SockAddr,
    /// Remote addr (client).
    pub raddr: SockAddr,
    /// Name (or ip addr) of remote host.
    pub remote_host: Option<String>,
    /// Name (not ip addr) of remote host, if available.
    pub remote_hostname: Option<String>,
    /// Hostname verification state: +1 resolves, -1 not, 0 not done, -2 error.
    pub remote_hostname_resolv: i32,
    /// gai lookup return code, for later gai_strerror.
    pub remote_hostname_errcode: i32,
    /// Text rep of remote port.
    pub remote_port: Option<String>,
    /// ip addr of local socket for client conn (filled only if needed).
    pub local_host: [u8; 64],

    /// Startup-packet database name.
    pub database_name: Option<String>,
    /// Startup-packet user name.
    pub user_name: Option<String>,
    /// Startup-packet command-line options.
    pub cmdline_options: Option<String>,
    /// Alternating GUC option names and values (C: `List *`, now a Vec).
    pub guc_options: Vec<String>,
    /// Startup-packet application name (used only for the auth log message).
    pub application_name: Option<String>,

    /// Information held during the authentication cycle.
    pub hba: Option<Box<HbaLine>>, // TODO(ptr): ownership unclear from header

    // TCP keepalive and user timeout settings.
    pub default_keepalives_idle: i32,
    pub default_keepalives_interval: i32,
    pub default_keepalives_count: i32,
    pub default_tcp_user_timeout: i32,
    pub keepalives_idle: i32,
    pub keepalives_interval: i32,
    pub keepalives_count: i32,
    pub tcp_user_timeout: i32,

    // SCRAM structures.
    pub scram_client_key: [u8; SCRAM_MAX_KEY_LEN],
    pub scram_server_key: [u8; SCRAM_MAX_KEY_LEN],
    /// true if the above two are valid.
    pub has_scram_keys: bool,

    /// GSSAPI structures (None when not used / not compiled in).
    pub gss: Option<Box<PgGssinfo>>,

    // SSL structures.
    pub ssl_in_use: bool,
    pub peer_cn: Option<String>,
    pub peer_dn: Option<String>,
    pub peer_cert_valid: bool,
    pub alpn_used: bool,
    pub last_read_was_eof: bool,

    /// OpenSSL `SSL` handle (opaque FFI, stubbed).
    pub ssl: Option<()>, // TODO(ffi): openssl SSL*
    /// OpenSSL `X509` peer cert (opaque FFI, stubbed).
    pub peer: Option<()>, // TODO(ffi): openssl X509*

    /// Data "unread" by a higher layer, to be re-read during SSL setup.
    pub raw_buf: Option<Vec<u8>>,
    pub raw_buf_consumed: isize,
    pub raw_buf_remaining: isize,
}

/// `ClientSocket` - an accepted connection's socket plus remote endpoint,
/// passed from postmaster to the backend.
pub struct ClientSocket {
    /// File descriptor.
    pub sock: pgsocket,
    /// Remote addr (client).
    pub raddr: SockAddr,
}

// --- be-secure-* / be-gssapi glue functions (impl in be-secure-openssl.c etc.) ---
// Signatures kept synchronous; bodies stubbed. SSL/GSS data uses Port fields.

/// Initialize global SSL context. Returns 0 if OK, -1 on trouble.
pub fn be_tls_init(_is_server_start: bool) -> i32 {
    unimplemented!()
}

/// Destroy global SSL context, if any.
pub fn be_tls_destroy() {
    unimplemented!()
}

/// Attempt to negotiate SSL connection.
pub fn be_tls_open_server(_port: &mut Port) -> i32 {
    unimplemented!()
}

/// Close SSL connection.
pub fn be_tls_close(_port: &mut Port) {
    unimplemented!()
}

/// Read data from a secure connection. `waitfor` is an out-param.
pub fn be_tls_read(_port: &mut Port, _ptr: &mut [u8], _waitfor: &mut i32) -> isize {
    unimplemented!()
}

/// Write data to a secure connection. `waitfor` is an out-param.
pub fn be_tls_write(_port: &mut Port, _ptr: &[u8], _waitfor: &mut i32) -> isize {
    unimplemented!()
}

pub fn be_tls_get_cipher_bits(_port: &mut Port) -> i32 {
    unimplemented!()
}
pub fn be_tls_get_version(_port: &mut Port) -> Option<String> {
    unimplemented!()
}
pub fn be_tls_get_cipher(_port: &mut Port) -> Option<String> {
    unimplemented!()
}
pub fn be_tls_get_peer_subject_name(_port: &mut Port) -> String {
    unimplemented!()
}
pub fn be_tls_get_peer_issuer_name(_port: &mut Port) -> String {
    unimplemented!()
}
pub fn be_tls_get_peer_serial(_port: &mut Port) -> String {
    unimplemented!()
}

/// Server certificate hash for SCRAM channel binding tls-server-end-point.
/// None if no certificate available.
pub fn be_tls_get_certificate_hash(_port: &mut Port) -> Option<Vec<u8>> {
    unimplemented!()
}

/// Return information about the GSSAPI authenticated connection.
pub fn be_gssapi_get_auth(_port: &mut Port) -> bool {
    unimplemented!()
}
pub fn be_gssapi_get_enc(_port: &mut Port) -> bool {
    unimplemented!()
}
pub fn be_gssapi_get_princ(_port: &mut Port) -> Option<String> {
    unimplemented!()
}
pub fn be_gssapi_get_delegation(_port: &mut Port) -> bool {
    unimplemented!()
}

/// Read from a GSSAPI-encrypted connection.
pub fn be_gssapi_read(_port: &mut Port, _ptr: &mut [u8]) -> isize {
    unimplemented!()
}
/// Write to a GSSAPI-encrypted connection.
pub fn be_gssapi_write(_port: &mut Port, _ptr: &[u8]) -> isize {
    unimplemented!()
}

// Globals -> session/task-local state later. Kept as fn stubs avoids unsafe statics.
// FrontendProtocol, MyClientConnectionInfo: see global-state plan.

/// TCP keepalives configuration (no-ops on an AF_UNIX socket).
pub fn pq_getkeepalivesidle(_port: &mut Port) -> i32 {
    unimplemented!()
}
pub fn pq_getkeepalivesinterval(_port: &mut Port) -> i32 {
    unimplemented!()
}
pub fn pq_getkeepalivescount(_port: &mut Port) -> i32 {
    unimplemented!()
}
pub fn pq_gettcpusertimeout(_port: &mut Port) -> i32 {
    unimplemented!()
}
pub fn pq_setkeepalivesidle(_idle: i32, _port: &mut Port) -> i32 {
    unimplemented!()
}
pub fn pq_setkeepalivesinterval(_interval: i32, _port: &mut Port) -> i32 {
    unimplemented!()
}
pub fn pq_setkeepalivescount(_count: i32, _port: &mut Port) -> i32 {
    unimplemented!()
}
pub fn pq_settcpusertimeout(_timeout: i32, _port: &mut Port) -> i32 {
    unimplemented!()
}