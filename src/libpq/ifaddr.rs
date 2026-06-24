//! Translated from PostgreSQL src/include/libpq/ifaddr.h
//
// IP netmask calculations and enumerating network interfaces. The C surface is
// over sockaddr_storage/sockaddr (OS boundary); kept as stubs. The per-interface
// callback (`void (*)(addr, netmask, cb_data)`) -> a captured closure.

use std::net::IpAddr;

/// `pg_range_sockaddr(addr, netaddr, netmask)` -> true if addr is in the subnet.
pub fn pg_range_sockaddr(_addr: &IpAddr, _netaddr: &IpAddr, _netmask: &IpAddr) -> bool {
    unimplemented!()
}

/// `pg_sockaddr_cidr_mask(mask, numbits, family)` returns 0/-1; builds a netmask
/// from a CIDR prefix length -> Result with the produced mask.
pub fn pg_sockaddr_cidr_mask(_numbits: Option<&str>, _family: i32) -> Result<IpAddr, ()> {
    unimplemented!()
}

/// `pg_foreach_ifaddr(callback, cb_data)`: enumerate interface addresses. The C
/// `void *cb_data` is captured by the closure. Returns 0/-1 -> Result.
pub fn pg_foreach_ifaddr(_callback: impl FnMut(IpAddr, IpAddr)) -> Result<(), ()> {
    unimplemented!()
}
