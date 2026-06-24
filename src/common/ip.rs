//! Translated from PostgreSQL src/include/common/ip.h
//
// IPv6-aware network access: thin wrappers over getaddrinfo/getnameinfo. These
// map onto Rust std::net (ToSocketAddrs / resolution) at impl time; the C
// addrinfo/sockaddr_storage surface is an OS/libc boundary kept as stubs here.

use std::net::SocketAddr;

/// `pg_getaddrinfo_all(host, servname, hints, **result)` returns an EAI_* code.
/// Resolves to a list of socket addresses -> Result<Vec<SocketAddr>>.
pub fn pg_getaddrinfo_all(_hostname: Option<&str>, _servname: Option<&str>) -> Result<Vec<SocketAddr>, i32> {
    unimplemented!() // TODO: back with std::net resolver
}

/// Frees the addrinfo list returned above; under Rust the Vec owns it, so this
/// is a no-op kept for call-site parity.
pub fn pg_freeaddrinfo_all(_addrs: Vec<SocketAddr>) {}

/// `pg_getnameinfo_all(addr, salen, node, nodelen, service, servicelen, flags)`
/// returns an EAI_* code; out-params node/service -> returned (host, service).
pub fn pg_getnameinfo_all(_addr: &SocketAddr, _flags: i32) -> Result<(String, String), i32> {
    unimplemented!()
}
