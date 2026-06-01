//! Translation of postgres/src/include/port/pg_bswap.h
//!
//! Byte swapping: reverse the byte order of 16/32/64-bit unsigned integers, plus
//! portable ntoh/hton equivalents extended to 64 bits. The C macros wrap compiler
//! builtins; Rust provides `u16/u32/u64::swap_bytes`, which lowers to the same
//! instructions.
//!
//! All functions take and return UNSIGNED integers (use caution with signed).
//!
//! Copyright (c) 2015-2025, PostgreSQL Global Development Group

use crate::prelude::*;

/// `pg_bswap16(x)`: reverse the two bytes of a uint16.
#[inline]
pub fn pg_bswap16(x: uint16) -> uint16 {
    x.swap_bytes()
}

/// `pg_bswap32(x)`: reverse the four bytes of a uint32.
#[inline]
pub fn pg_bswap32(x: uint32) -> uint32 {
    x.swap_bytes()
}

/// `pg_bswap64(x)`: reverse the eight bytes of a uint64.
#[inline]
pub fn pg_bswap64(x: uint64) -> uint64 {
    x.swap_bytes()
}

// Portable/fast equivalents for ntohs/ntohl/htons/htonl, extended to 64 bits.
// This build is little-endian (WORDS_BIGENDIAN undefined), so host<->network
// conversion is a byte swap.

/// `pg_hton16(x)`: host to network (big-endian) byte order.
#[inline]
pub fn pg_hton16(x: uint16) -> uint16 {
    pg_bswap16(x)
}
/// `pg_hton32(x)`
#[inline]
pub fn pg_hton32(x: uint32) -> uint32 {
    pg_bswap32(x)
}
/// `pg_hton64(x)`
#[inline]
pub fn pg_hton64(x: uint64) -> uint64 {
    pg_bswap64(x)
}

/// `pg_ntoh16(x)`: network (big-endian) to host byte order.
#[inline]
pub fn pg_ntoh16(x: uint16) -> uint16 {
    pg_bswap16(x)
}
/// `pg_ntoh32(x)`
#[inline]
pub fn pg_ntoh32(x: uint32) -> uint32 {
    pg_bswap32(x)
}
/// `pg_ntoh64(x)`
#[inline]
pub fn pg_ntoh64(x: uint64) -> uint64 {
    pg_bswap64(x)
}

/// `DatumBigEndianToNative(x)`: rearrange a Datum's bytes from big-endian into
/// native order. SIZEOF_DATUM == 8 on this build, so this is a 64-bit swap on the
/// little-endian host.
#[inline]
pub fn DatumBigEndianToNative(x: Datum) -> Datum {
    pg_bswap64(x as uint64) as Datum
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn swaps() {
        assert_eq!(pg_bswap16(0xAABB), 0xBBAA);
        assert_eq!(pg_bswap32(0xAABBCCDD), 0xDDCCBBAA);
        assert_eq!(pg_bswap64(0x0123456789ABCDEF), 0xEFCDAB8967452301);
        // round-trip via hton/ntoh
        assert_eq!(pg_ntoh32(pg_hton32(0x12345678)), 0x12345678);
    }
}
