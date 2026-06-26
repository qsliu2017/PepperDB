//! Translated from PostgreSQL src/include/storage/checksum_impl.h
//!
//! Checksum implementation for data pages. COMPAT-SENSITIVE: this must match
//! PostgreSQL byte-for-byte. FNV-1a-based, 32 parallel partial sums, folded to a
//! uint16. Targets are 64-bit little-endian, so the page's native uint32 reads
//! are little-endian.

use crate::pg_config::BLCKSZ;
use crate::storage::block::BlockNumber;

/// number of checksums to calculate in parallel
pub const N_SUMS: usize = 32;
/// prime multiplier of FNV-1a hash
pub const FNV_PRIME: u32 = 16777619;

/// number of uint32 rows when the page is viewed as `[BLCKSZ/(4*N_SUMS)][N_SUMS]`
const N_ROWS: usize = (BLCKSZ as usize) / (core::mem::size_of::<u32>() * N_SUMS);

/// Base offsets to initialize each of the parallel FNV hashes into a different
/// initial state.
pub static CHECKSUM_BASE_OFFSETS: [u32; N_SUMS] = [
    0x5B1F36E9, 0xB8525960, 0x02AB50AA, 0x1DE66D2A, 0x79FF467A, 0x9BB9F8A3, 0x217E7CD2, 0x83E13D2C,
    0xF8D4474F, 0xE39EB970, 0x42C6AE16, 0x993216FA, 0x7B093B5D, 0x98DAFF3C, 0xF718902A, 0x0B1C9CDB,
    0xE58F764B, 0x187636BC, 0x5D7B3BB1, 0xE73DE7DE, 0x92BEC979, 0xCCA6C0B2, 0x304A0979, 0x85AA43D4,
    0x783125BB, 0x6CA8EAA2, 0xE407EAC6, 0x4B5CFC3E, 0x9FBF8C76, 0x15CA20BE, 0xF2CA9FD3, 0x959BD756,
];

/// One round of the checksum (was CHECKSUM_COMP macro).
#[inline]
const fn checksum_comp(checksum: u32, value: u32) -> u32 {
    let tmp = checksum ^ value;
    tmp.wrapping_mul(FNV_PRIME) ^ (tmp >> 17)
}

/// Block checksum algorithm. `page` is the BLCKSZ-byte page viewed as a
/// row-major `[N_ROWS][N_SUMS]` array of native (little-endian) u32 values.
pub fn pg_checksum_block(page: &[u8]) -> u32 {
    assert_eq!(page.len(), BLCKSZ as usize);

    let mut sums: [u32; N_SUMS] = CHECKSUM_BASE_OFFSETS;

    let u32_at = |i: usize, j: usize| -> u32 {
        let off = (i * N_SUMS + j) * 4;
        u32::from_le_bytes([page[off], page[off + 1], page[off + 2], page[off + 3]])
    };

    // main checksum calculation
    for i in 0..N_ROWS {
        for (j, s) in sums.iter_mut().enumerate() {
            *s = checksum_comp(*s, u32_at(i, j));
        }
    }

    // two rounds of zeroes for additional mixing
    for _ in 0..2 {
        for s in &mut sums {
            *s = checksum_comp(*s, 0);
        }
    }

    // xor fold partial checksums together
    let mut result = 0u32;
    for s in sums {
        result ^= s;
    }
    result
}

/// Compute the checksum for a Postgres page.
///
/// The checksum includes the block number (to detect transposed pages), the page
/// header (excluding the checksum itself), and the page data. The caller must NOT
/// have set checksum; this function reads the page with the checksum field
/// treated as zero (offset 8..10 in the header).
pub fn pg_checksum_page(page: &[u8], blkno: BlockNumber) -> u16 {
    // Compute over a copy with checksum zeroed (the C code transiently zeroes
    // it on the page itself; we avoid mutating the caller's buffer).
    let mut buf = [0u8; BLCKSZ as usize];
    buf.copy_from_slice(page);
    buf[8] = 0; // checksum low byte
    buf[9] = 0; // checksum high byte

    let mut checksum = pg_checksum_block(&buf);

    // Mix in the block number to detect transposed pages
    checksum ^= blkno;

    // Reduce to uint16 with an offset of one (avoids checksums of zero).
    ((checksum % 65535) + 1) as u16
}
