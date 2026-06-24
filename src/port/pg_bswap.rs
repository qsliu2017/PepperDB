//! Translated from PostgreSQL src/include/port/pg_bswap.h

// Byte swapping. PG wraps compiler builtins; Rust has them in core.
// Target is little-endian only (Linux x86_64 + macOS aarch64).

pub const fn pg_bswap16(x: u16) -> u16 {
    x.swap_bytes()
}

pub const fn pg_bswap32(x: u32) -> u32 {
    x.swap_bytes()
}

pub const fn pg_bswap64(x: u64) -> u64 {
    x.swap_bytes()
}

// hton/ntoh: big-endian network order. LE target -> swap.
pub const fn pg_hton16(x: u16) -> u16 {
    x.to_be()
}

pub const fn pg_hton32(x: u32) -> u32 {
    x.to_be()
}

pub const fn pg_hton64(x: u64) -> u64 {
    x.to_be()
}

pub const fn pg_ntoh16(x: u16) -> u16 {
    u16::from_be(x)
}

pub const fn pg_ntoh32(x: u32) -> u32 {
    u32::from_be(x)
}

pub const fn pg_ntoh64(x: u64) -> u64 {
    u64::from_be(x)
}

// Datum is 8 bytes on target; big-endian -> native is a 64-bit swap on LE.
pub const fn datum_big_endian_to_native(x: usize) -> usize {
    (x as u64).swap_bytes() as usize
}
