//! Translated from PostgreSQL src/include/port/pg_crc32c.h
//
// CRC-32C (Castagnoli). COMPAT-SENSITIVE: used for WAL/page integrity, so the
// byte-exact result must match PostgreSQL. Polynomial is Castagnoli 0x1EDC6F41,
// used reflected as 0x82F63B78; init = 0xFFFFFFFF, output xored with
// 0xFFFFFFFF (FIN). Big-endian byte-reorder paths are dropped (LE targets only).
// Hardware SSE4.2/ARMv8 acceleration is omitted; the reflected-poly slicing-by-8
// software path (PG `pg_crc32c_sb8.c`) is the reference. If a crate is adopted
// later, verify its seed/xorout/refin.

pub type pg_crc32c = u32;

/// Reflected Castagnoli polynomial (PG `pg_crc32c_table`'s generator).
const CRC32C_POLY_REFLECTED: u32 = 0x82F6_3B78;

/// Slicing-by-8 lookup table, generated at compile time so the output is
/// bit-identical to PG's `pg_crc32c_table` (same reflected bit algorithm).
const CRC32C_TABLE: [[u32; 256]; 8] = build_crc32c_table();

const fn build_crc32c_table() -> [[u32; 256]; 8] {
    let mut table = [[0u32; 256]; 8];
    let mut n = 0;
    while n < 256 {
        let mut crc = n as u32;
        let mut k = 0;
        while k < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ CRC32C_POLY_REFLECTED
            } else {
                crc >> 1
            };
            k += 1;
        }
        table[0][n] = crc;
        n += 1;
    }
    // Higher-order slices: table[i][n] = table[i-1][n] processed one more byte.
    let mut n = 0;
    while n < 256 {
        let mut crc = table[0][n];
        let mut i = 1;
        while i < 8 {
            crc = table[0][(crc & 0xFF) as usize] ^ (crc >> 8);
            table[i][n] = crc;
            i += 1;
        }
        n += 1;
    }
    table
}

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
/// Slicing-by-8 reference implementation (PG `pg_crc32c_sb8`); the running CRC is
/// kept in its reflected (not-yet-finalized) form, exactly like the C macro.
pub fn comp_crc32c(mut crc: pg_crc32c, data: &[u8]) -> pg_crc32c {
    let mut chunks = data.chunks_exact(8);
    for chunk in &mut chunks {
        crc ^= u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        let hi = u32::from_le_bytes([chunk[4], chunk[5], chunk[6], chunk[7]]);
        crc = CRC32C_TABLE[7][(crc & 0xFF) as usize]
            ^ CRC32C_TABLE[6][((crc >> 8) & 0xFF) as usize]
            ^ CRC32C_TABLE[5][((crc >> 16) & 0xFF) as usize]
            ^ CRC32C_TABLE[4][(crc >> 24) as usize]
            ^ CRC32C_TABLE[3][(hi & 0xFF) as usize]
            ^ CRC32C_TABLE[2][((hi >> 8) & 0xFF) as usize]
            ^ CRC32C_TABLE[1][((hi >> 16) & 0xFF) as usize]
            ^ CRC32C_TABLE[0][(hi >> 24) as usize];
    }
    for &b in chunks.remainder() {
        crc = CRC32C_TABLE[0][((crc ^ b as u32) & 0xFF) as usize] ^ (crc >> 8);
    }
    crc
}

/// Convenience: full CRC32C over `data` (init -> comp -> fin).
pub fn crc32c(data: &[u8]) -> pg_crc32c {
    fin_crc32c(comp_crc32c(init_crc32c(), data))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_test_vector() {
        // CRC-32C of "123456789" is 0xE3069283 (standard check value).
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn empty_is_zero_after_fin() {
        assert_eq!(crc32c(b""), 0);
    }

    #[test]
    fn incremental_matches_oneshot() {
        let data = b"the quick brown fox jumps over the lazy dog";
        let oneshot = comp_crc32c(init_crc32c(), data);
        let mut inc = init_crc32c();
        inc = comp_crc32c(inc, &data[..10]);
        inc = comp_crc32c(inc, &data[10..]);
        assert_eq!(inc, oneshot);
    }

    #[test]
    fn incremental_init_comp_fin_vector() {
        // PG INIT/COMP/FIN over the standard check string yields 0xE3069283.
        assert_eq!(
            fin_crc32c(comp_crc32c(init_crc32c(), b"123456789")),
            0xE306_9283
        );
        // comp-in-two-chunks == comp-in-one-chunk (running CRC is composable).
        let one = comp_crc32c(init_crc32c(), b"123456789");
        let two = comp_crc32c(comp_crc32c(init_crc32c(), b"1234"), b"56789");
        assert_eq!(one, two);
    }
}
