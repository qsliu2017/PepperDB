//! Translated from PostgreSQL src/include/utils/pg_crc.h
//
// 32-bit CRC support. Two variants live here: TRADITIONAL_CRC32 (the Ethernet
// CRC-32 polynomial, reversed 0xEDB88320 / normal 0x04C11DB7) and LEGACY_CRC32 (a
// historical, non-polynomial mistake kept only for on-disk pg_upgrade compat). Both
// share `pg_crc32_table`. CRC-32C (Castagnoli, WAL integrity) lives in
// port/pg_crc32c.h, not here. These are COMPAT-SENSITIVE: the table and bit order
// must round-trip byte-for-byte, so do not swap in a generic crate.

pub type pg_crc32 = u32;

// === Traditional CRC-32 (Ethernet polynomial) ===

pub const fn init_traditional_crc32() -> pg_crc32 {
    0xFFFF_FFFF
}
pub const fn fin_traditional_crc32(crc: pg_crc32) -> pg_crc32 {
    crc ^ 0xFFFF_FFFF
}
pub fn comp_traditional_crc32(crc: pg_crc32, data: &[u8]) -> pg_crc32 {
    comp_crc32_normal_table(crc, data, &PG_CRC32_TABLE)
}
pub const fn eq_traditional_crc32(c1: pg_crc32, c2: pg_crc32) -> bool {
    c1 == c2
}

/// Sarwate's algorithm with a "normal" lookup table.
pub fn comp_crc32_normal_table(mut crc: pg_crc32, data: &[u8], table: &[u32; 256]) -> pg_crc32 {
    for &b in data {
        let tab_index = ((crc ^ u32::from(b)) & 0xFF) as usize;
        crc = table[tab_index] ^ (crc >> 8);
    }
    crc
}

// === Legacy CRC-32 (pre-9.5 WAL; on-disk compat only, do not use in new code) ===

pub const fn init_legacy_crc32() -> pg_crc32 {
    0xFFFF_FFFF
}
pub const fn fin_legacy_crc32(crc: pg_crc32) -> pg_crc32 {
    crc ^ 0xFFFF_FFFF
}
pub fn comp_legacy_crc32(crc: pg_crc32, data: &[u8]) -> pg_crc32 {
    comp_crc32_reflected_table(crc, data, &PG_CRC32_TABLE)
}
pub const fn eq_legacy_crc32(c1: pg_crc32, c2: pg_crc32) -> bool {
    c1 == c2
}

/// Sarwate's algorithm with a "reflected" lookup table (legacy uses it on the
/// normal table, hence the historical mismatch this comment preserves).
pub fn comp_crc32_reflected_table(mut crc: pg_crc32, data: &[u8], table: &[u32; 256]) -> pg_crc32 {
    for &b in data {
        let tab_index = (((crc >> 24) ^ u32::from(b)) & 0xFF) as usize;
        crc = table[tab_index] ^ (crc << 8);
    }
    crc
}

/// Lookup table for the CRC-32 polynomials, shared by both variants above.
// TODO(crc): populate the 256-entry table (from src/backend/utils/hash/pg_crc.c)
// before any on-disk CRC is verified; it MUST match PG byte-for-byte.
pub static PG_CRC32_TABLE: [u32; 256] = [0; 256];
