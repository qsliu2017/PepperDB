//! Translation of postgres/src/include/port/pg_crc32c.h
//!                + postgres/src/port/pg_crc32c_sb8.c  (portable slicing-by-8)
//!
//! CRC-32C (Castagnoli), used for WAL records, the control file, etc.
//!
//! The C source ships an 8x256 precomputed `pg_crc32c_table` (1000+ literal
//! constants, with separate big/little-endian variants selected by #ifdef).
//! Rather than transcribe all 2048 constants, this port computes the identical
//! slicing-by-8 table once at first use from the reflected Castagnoli polynomial
//! 0x82F63B78. The result is byte-for-byte identical to PostgreSQL's table on a
//! little-endian host; the `sb8_known_answer` test pins it to the standard
//! CRC-32C check value. The compression loop itself (`pg_comp_crc32c_sb8`) is a
//! faithful translation of the little-endian (!WORDS_BIGENDIAN) C path.
//!
//! TODO(pg-port): runtime SSE4.2 / AVX-512 / ARMv8 dispatch (pg_crc32c_*_choose.c)
//! and the big-endian table variant.

use crate::prelude::*;
use std::sync::OnceLock;

/// CRC accumulator type (pg_crc32c.h: `typedef uint32 pg_crc32c;`).
pub type pg_crc32c = uint32;

/// `INIT_CRC32C(crc)`: start a fresh CRC computation.
#[inline]
pub fn INIT_CRC32C() -> pg_crc32c {
    0xFFFFFFFF
}

/// `FIN_CRC32C(crc)`: finalize (invert) a CRC.
#[inline]
pub fn FIN_CRC32C(crc: pg_crc32c) -> pg_crc32c {
    crc ^ 0xFFFFFFFF
}

/// `EQ_CRC32C(c1, c2)`.
#[inline]
pub fn EQ_CRC32C(c1: pg_crc32c, c2: pg_crc32c) -> bool {
    c1 == c2
}

/// The reflected Castagnoli polynomial (CRC-32C), used to build the table.
const CRC32C_POLY_REFLECTED: u32 = 0x82F63B78;

/// The slicing-by-8 lookup table, computed once. `table[0]` is the standard
/// byte-wise reflected CRC-32C table; `table[i]` extends it for slice `i`.
fn pg_crc32c_table() -> &'static [[u32; 256]; 8] {
    static TABLE: OnceLock<[[u32; 256]; 8]> = OnceLock::new();
    TABLE.get_or_init(|| {
        let mut t = [[0u32; 256]; 8];
        // table[0]: CRC of each single byte.
        for n in 0..256usize {
            let mut crc = n as u32;
            for _ in 0..8 {
                if crc & 1 != 0 {
                    crc = (crc >> 1) ^ CRC32C_POLY_REFLECTED;
                } else {
                    crc >>= 1;
                }
            }
            t[0][n] = crc;
        }
        // table[1..8]: each derived from the previous slice.
        for n in 0..256usize {
            let mut crc = t[0][n];
            for i in 1..8usize {
                crc = (crc >> 8) ^ t[0][(crc & 0xFF) as usize];
                t[i][n] = crc;
            }
        }
        t
    })
}

/// `CRC8(x)` helper macro (little-endian variant from sb8.c):
/// `pg_crc32c_table[0][(crc ^ x) & 0xFF] ^ (crc >> 8)`.
#[inline]
fn CRC8(crc: u32, x: u8, table: &[[u32; 256]; 8]) -> u32 {
    table[0][((crc ^ (x as u32)) & 0xFF) as usize] ^ (crc >> 8)
}

/// `pg_comp_crc32c_sb8(crc, data, len)`: the portable slicing-by-8 CRC-32C.
/// Faithful translation of the !WORDS_BIGENDIAN path.
///
/// # Safety
/// `data` must be valid for `len` bytes.
pub unsafe fn pg_comp_crc32c_sb8(
    mut crc: pg_crc32c,
    data: *const c_void,
    mut len: Size,
) -> pg_crc32c {
    let table = pg_crc32c_table();
    let mut p = data as *const u8;

    /*
     * Handle 0-3 initial bytes one at a time, so that the loop below starts
     * with a pointer aligned to four bytes.
     */
    while len > 0 && ((p as usize) & 3) != 0 {
        crc = CRC8(crc, *p, table);
        p = p.add(1);
        len -= 1;
    }

    /*
     * Process eight bytes of data at a time.
     */
    let mut p4 = p as *const u32;
    while len >= 8 {
        let a = *p4 ^ crc;
        p4 = p4.add(1);
        let b = *p4;
        p4 = p4.add(1);

        // !WORDS_BIGENDIAN byte extraction
        let c0 = (b >> 24) as u8;
        let c1 = (b >> 16) as u8;
        let c2 = (b >> 8) as u8;
        let c3 = b as u8;
        let c4 = (a >> 24) as u8;
        let c5 = (a >> 16) as u8;
        let c6 = (a >> 8) as u8;
        let c7 = a as u8;

        crc = table[0][c0 as usize]
            ^ table[1][c1 as usize]
            ^ table[2][c2 as usize]
            ^ table[3][c3 as usize]
            ^ table[4][c4 as usize]
            ^ table[5][c5 as usize]
            ^ table[6][c6 as usize]
            ^ table[7][c7 as usize];

        len -= 8;
    }

    /*
     * Handle any remaining bytes one at a time.
     */
    p = p4 as *const u8;
    while len > 0 {
        crc = CRC8(crc, *p, table);
        p = p.add(1);
        len -= 1;
    }

    crc
}

/// `pg_comp_crc32c`: the active CRC-32C implementation. In C this is a function
/// pointer set by a runtime CPU-feature check; this portable build always uses
/// the slicing-by-8 routine.
///
/// # Safety
/// See [`pg_comp_crc32c_sb8`].
#[inline]
pub unsafe fn pg_comp_crc32c(crc: pg_crc32c, data: *const c_void, len: Size) -> pg_crc32c {
    pg_comp_crc32c_sb8(crc, data, len)
}

/// `COMP_CRC32C(crc, data, len)`: accumulate `len` bytes into `crc`.
///
/// # Safety
/// `data` must be valid for `len` bytes.
#[inline]
pub unsafe fn COMP_CRC32C(crc: pg_crc32c, data: *const c_void, len: Size) -> pg_crc32c {
    pg_comp_crc32c(crc, data, len)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sb8_known_answer() {
        unsafe {
            // Standard CRC-32C check value: CRC of the ASCII string "123456789"
            // (init 0xFFFFFFFF, reflected, final XOR 0xFFFFFFFF) is 0xE3069283.
            let msg = b"123456789";
            let mut crc = INIT_CRC32C();
            crc = COMP_CRC32C(crc, msg.as_ptr() as *const c_void, msg.len());
            crc = FIN_CRC32C(crc);
            assert_eq!(crc, 0xE3069283);

            // Empty input -> 0 after init+final.
            let mut e = INIT_CRC32C();
            e = COMP_CRC32C(e, core::ptr::null(), 0);
            e = FIN_CRC32C(e);
            assert_eq!(e, 0x00000000);
        }
    }
}
