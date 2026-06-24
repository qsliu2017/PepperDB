//! Translated from PostgreSQL src/include/libpq/pqcomm.h
//
// Definitions common to frontends and backends. Wire-protocol/connection types:
// in-memory model (NOT repr(C)); the few persisted/wire integers are noted as
// network byte order and (de)serialized explicitly by the protocol layer.
//
// Includes libpq/protocol.h (request/response codes) - re-exported by the
// protocol layer; this header itself defines no items that reference them.

/// A resolved socket address. The C `struct sockaddr_storage` + length maps to
/// the std socket address type.
pub struct SockAddr {
    pub addr: std::net::SocketAddr,
}

/// An address family plus its socket address.
pub struct AddrInfo {
    pub family: i32,
    pub addr: SockAddr,
}

/// Compute the UNIX socket path for a port under `sockdir`.
pub fn unixsock_path(port: i32, sockdir: &str) -> String {
    format!("{sockdir}/.s.PGSQL.{port}")
}

/// Max workable length of a Unix-domain socket path (struct sockaddr_un.sun_path).
/// 108 on Linux, 104 on macOS; use the smaller as the portable buffer length.
pub const UNIXSOCK_PATH_BUFLEN: usize = 104;

/// A host that looks like an absolute path or starts with @ is a Unix socket.
pub fn is_unixsock_path(path: &str) -> bool {
    is_absolute_path(path) || path.as_bytes().first() == Some(&b'@')
}

// is_absolute_path lives in port.h (not yet translated); local helper for now.
fn is_absolute_path(path: &str) -> bool {
    path.as_bytes().first() == Some(&b'/')
}

// Protocol version number manipulation.
pub const fn pg_protocol_major(v: u32) -> u32 {
    v >> 16
}
pub const fn pg_protocol_minor(v: u32) -> u32 {
    v & 0x0000ffff
}
pub const fn pg_protocol_full(v: u32) -> u32 {
    pg_protocol_major(v) * 10000 + pg_protocol_minor(v)
}
pub const fn pg_protocol(m: u32, n: u32) -> u32 {
    (m << 16) | n
}

/// Earliest supported FE/BE protocol version.
pub const PG_PROTOCOL_EARLIEST: u32 = pg_protocol(3, 0);
/// Latest supported FE/BE protocol version.
pub const PG_PROTOCOL_LATEST: u32 = pg_protocol(3, 2);

/// FE/BE protocol version number.
pub type ProtocolVersion = u32;
pub type MsgType = ProtocolVersion;

/// Packet lengths are 4 bytes in network byte order.
pub type PacketLen = u32;

/// Arbitrary limit on startup packet length (anti-DoS).
pub const MAX_STARTUP_PACKET_LENGTH: usize = 10000;

pub type AuthRequest = u32;

/// Cancel-current-operation request code (must not match a protocol version).
pub const CANCEL_REQUEST_CODE: u32 = pg_protocol(1234, 5678);

/// Cancel request packet. Each field is stored in network byte order on the wire;
/// the trailing variable-length cancel key is the cancelAuthCode tail.
pub struct CancelRequestPacket {
    pub cancel_request_code: MsgType,
    pub backend_pid: u32,
    /// Secret key to authorize cancel (variable length since protocol 3.2).
    pub cancel_auth_code: Vec<u8>,
}

// ALPN protocol id required for direct connections (RFC 7301).
pub const PG_ALPN_PROTOCOL: &str = "postgresql";
pub const PG_ALPN_PROTOCOL_VECTOR: &[u8] =
    &[10, b'p', b'o', b's', b't', b'g', b'r', b'e', b's', b'q', b'l'];

/// SSL negotiation request code.
pub const NEGOTIATE_SSL_CODE: u32 = pg_protocol(1234, 5679);
/// GSSAPI negotiation request code.
pub const NEGOTIATE_GSS_CODE: u32 = pg_protocol(1234, 5680);
