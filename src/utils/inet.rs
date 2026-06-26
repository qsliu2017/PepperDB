//! Translated from PostgreSQL src/include/utils/inet.h
//!
//! Internal storage for INET/CIDR/MAC types. inet_struct is on-disk; inet wraps
//! it behind a varlena header. macaddr/macaddr8 are fixed-size pass-by-reference.

use crate::postgres::Datum;

/// Internal storage format for IP addresses (both INET and CIDR). On-disk.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct inet_struct {
    pub family: u8,      // PGSQL_AF_INET or PGSQL_AF_INET6
    pub bits: u8,        // number of bits in netmask
    pub ipaddr: [u8; 16], // up to 128 bits of address
}
const _: () = assert!(core::mem::size_of::<inet_struct>() == 18);
const _: () = assert!(core::mem::offset_of!(inet_struct, ipaddr) == 2);

// "family" field values. AF_INET is libc's address family (2 on Linux+macOS).
pub const AF_INET: i32 = 2;
pub const PGSQL_AF_INET: i32 = AF_INET;
pub const PGSQL_AF_INET6: i32 = AF_INET + 1;

/// INET/CIDR varlena wrapper (uncompressed in-memory shape). On-disk varlena.
#[repr(C)]
pub struct inet {
    pub vl_len_: [u8; 4], // varlena header (do not touch directly)
    pub inet_data: inet_struct,
}
const _: () = assert!(core::mem::offset_of!(inet, inet_data) == 4);

/// Number of address bytes for an inet_struct family.
#[inline]
pub fn ip_addrsize(family: i32) -> i32 {
    if family == PGSQL_AF_INET {
        4
    } else {
        16
    }
}

/// Maximum netmask bits for an inet_struct family.
#[inline]
pub fn ip_maxbits(family: i32) -> i32 {
    if family == PGSQL_AF_INET {
        32
    } else {
        128
    }
}

/// Internal storage format for MAC addresses. On-disk fixed-size.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct macaddr {
    pub a: u8,
    pub b: u8,
    pub c: u8,
    pub d: u8,
    pub e: u8,
    pub f: u8,
}
const _: () = assert!(core::mem::size_of::<macaddr>() == 6);

/// Internal storage format for MAC8 addresses. On-disk fixed-size.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct macaddr8 {
    pub a: u8,
    pub b: u8,
    pub c: u8,
    pub d: u8,
    pub e: u8,
    pub f: u8,
    pub g: u8,
    pub h: u8,
}
const _: () = assert!(core::mem::size_of::<macaddr8>() == 8);

// fmgr interface
#[inline]
pub fn DatumGetInetPP(x: Datum) -> *mut inet {
    unimplemented!() // PG_DETOAST_DATUM_PACKED; TODO(ptr)
}
#[inline]
pub fn InetPGetDatum(x: &inet) -> Datum {
    Datum(std::ptr::from_ref::<inet>(x) as usize)
}
#[inline]
pub fn DatumGetInetP(x: Datum) -> *mut inet {
    unimplemented!() // PG_DETOAST_DATUM; TODO(ptr)
}
#[inline]
pub fn DatumGetMacaddrP(x: Datum) -> *mut macaddr {
    x.0 as *mut macaddr // TODO(ptr)
}
#[inline]
pub fn MacaddrPGetDatum(x: &macaddr) -> Datum {
    Datum(std::ptr::from_ref::<macaddr>(x) as usize)
}
#[inline]
pub fn DatumGetMacaddr8P(x: Datum) -> *mut macaddr8 {
    x.0 as *mut macaddr8 // TODO(ptr)
}
#[inline]
pub fn Macaddr8PGetDatum(x: &macaddr8) -> Datum {
    Datum(std::ptr::from_ref::<macaddr8>(x) as usize)
}

// Support functions in network.c
pub fn cidr_set_masklen_internal(src: &inet, bits: i32) -> *mut inet {
    unimplemented!() // TODO(ptr)
}
pub fn bitncmp(l: &[u8], r: &[u8], n: i32) -> i32 {
    unimplemented!()
}
pub fn bitncommon(l: &[u8], r: &[u8], n: i32) -> i32 {
    unimplemented!()
}
