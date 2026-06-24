//! Translated from PostgreSQL src/include/port/pg_crc32c.h
//
// CRC-32C (Castagnoli). COMPAT-SENSITIVE: used for WAL/page integrity, so the
// byte-exact result must match PostgreSQL. Polynomial is Castagnoli 0x1EDC6F41,
// used reflected as 0x82F63B78; init = 0xFFFFFFFF, output xored with
// 0xFFFFFFFF (FIN). Big-endian byte-reorder paths are dropped (LE targets only).
// Hardware SSE4.2/ARMv8 acceleration is omitted; the slicing-by-8 software path
// is the reference. If a crate is adopted later, verify its seed/xorout/refin.

pub type pg_crc32c = u32;

/// C: `INIT_CRC32C(crc)`.
pub const fn init_crc32c() -> pg_crc32c {
    0xFFFF_FFFF
}

/// C: `EQ_CRC32C(c1, c2)`.
pub const fn eq_crc32c(c1: pg_crc32c, c2: pg_crc32c) -> bool {
    c1 == c2
}

/// C: `FIN_CRC32C(crc)` on little-endian.
pub const fn fin_crc32c(crc: pg_crc32c) -> pg_crc32c {
    crc ^ 0xFFFF_FFFF
}

/// C: `COMP_CRC32C(crc, data, len)` -> accumulate `data` into `crc`.
/// Software slicing-by-8 reference implementation. TODO: emit the 8x256 lookup
/// table (or use the reflected-poly bit algorithm) so the output matches PG.
pub fn comp_crc32c(crc: pg_crc32c, data: &[u8]) -> pg_crc32c {
    unimplemented!()
}

/// Convenience: full CRC32C over `data` (init -> comp -> fin).
pub fn crc32c(data: &[u8]) -> pg_crc32c {
    fin_crc32c(comp_crc32c(init_crc32c(), data))
}
