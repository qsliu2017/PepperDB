//! Checksum implementation for data pages. Translated from backend/storage/page/checksum.c.
//!
//! PostgreSQL computes a page checksum with an algorithm chosen for very fast
//! calculation, so that the checksum does not become the bottleneck when pages
//! stream in from the OS file cache. It is based on the FNV-1a hash, modified to
//! mix high-order bits better by also xor-ing in the value shifted right by 17
//! bits, and to fold in four bytes at a time. To hide multiply latency and exploit
//! SIMD parallelism, the page is viewed as a two-dimensional array of 32 columns of
//! 32-bit words; each column accumulates a partial checksum from a distinct initial
//! offset basis, two extra zero rounds mix in the final word, and the partial sums
//! are XOR-folded into one 32-bit value. The block number is mixed in to catch
//! transposed pages, and the result is reduced modulo 65535 with an offset of one so
//! it is always a non-zero u16. The stored pd_checksum field is excluded from the
//! computation (treated as zero). In C the actual code lives in checksum_impl.h so
//! external programs can incorporate it; that algorithm is translated directly here.
//!
//! The output is bit-for-bit identical to PostgreSQL: an existing data directory's
//! checksums validate unchanged, and a regression anchor cross-checked against the C
//! reference guards the on-disk format. Where C zeroes pd_checksum in place and then
//! restores it, this version copies the page into a word array with that field
//! masked, avoiding any transient mutation of the live page. The computation is a
//! pure synchronous value calculation invoked while the caller holds the buffer
//! lock; nothing here is stored back onto the page.

use crate::pg_config::BLCKSZ;
use crate::storage::block::BlockNumber;
use crate::storage::bufpage::{Page, SizeOfPageHeaderData};

/// Number of checksums to calculate in parallel.
const N_SUMS: usize = 32;
/// Prime multiplier of FNV-1a hash.
const FNV_PRIME: u32 = 16777619;
/// Number of 32-word blocks in a page (BLCKSZ / (4 * N_SUMS) = 8192/128 = 64).
const N_BLOCKS: usize = BLCKSZ as usize / (4 * N_SUMS);

/// Byte offset of pd_checksum within PageHeaderData (lsn:8 bytes precede it).
const PD_CHECKSUM_OFFSET: usize = 8;

const _: () = assert!(N_BLOCKS * 4 * N_SUMS == BLCKSZ as usize);
const _: () = assert!(N_BLOCKS == 64);
const _: () = assert!(PD_CHECKSUM_OFFSET < SizeOfPageHeaderData);

/// Base offsets to initialize each of the parallel FNV hashes into a different
/// initial state. Chosen randomly; the values themselves don't matter as much
/// as that they differ and don't match anything in real data.
static CHECKSUM_BASE_OFFSETS: [u32; N_SUMS] = [
    0x5B1F36E9, 0xB8525960, 0x02AB50AA, 0x1DE66D2A, 0x79FF467A, 0x9BB9F8A3, 0x217E7CD2, 0x83E13D2C,
    0xF8D4474F, 0xE39EB970, 0x42C6AE16, 0x993216FA, 0x7B093B5D, 0x98DAFF3C, 0xF718902A, 0x0B1C9CDB,
    0xE58F764B, 0x187636BC, 0x5D7B3BB1, 0xE73DE7DE, 0x92BEC979, 0xCCA6C0B2, 0x304A0979, 0x85AA43D4,
    0x783125BB, 0x6CA8EAA2, 0xE407EAC6, 0x4B5CFC3E, 0x9FBF8C76, 0x15CA20BE, 0xF2CA9FD3, 0x959BD756,
];

/// One round of the checksum (the CHECKSUM_COMP macro). Wrapping multiply to
/// match C's modular 32-bit arithmetic.
#[inline]
fn checksum_comp(checksum: u32, value: u32) -> u32 {
    let tmp = checksum ^ value;
    tmp.wrapping_mul(FNV_PRIME) ^ (tmp >> 17)
}

/// Block checksum algorithm (pg_checksum_block). `words` is the page reinterpreted
/// as 64 rows of 32 little-endian u32 columns; each column is aggregated into its
/// own partial checksum, then the lanes are XOR-folded together.
fn pg_checksum_block(words: &[u32; N_BLOCKS * N_SUMS]) -> u32 {
    let mut sums = CHECKSUM_BASE_OFFSETS;

    // Main checksum calculation.
    for i in 0..N_BLOCKS {
        for j in 0..N_SUMS {
            sums[j] = checksum_comp(sums[j], words[i * N_SUMS + j]);
        }
    }

    // Two more rounds of zeroes for additional mixing.
    for _ in 0..2 {
        for s in &mut sums {
            *s = checksum_comp(*s, 0);
        }
    }

    // XOR fold partial checksums together.
    sums.iter().fold(0u32, |acc, &s| acc ^ s)
}

impl Page {
    /// Compute the checksum for a Postgres page.
    ///
    /// The pd_checksum field is excluded from the computation (transiently treated
    /// as zero). The result mixes in the block number (to detect transposed pages)
    /// and is reduced to a non-zero u16. This does NOT store the checksum on the
    /// page.
    ///
    /// The caller holds the buffer lock.
    pub fn checksum(&self, blkno: BlockNumber) -> u16 {
        // We only calculate the checksum for properly-initialized pages.
        debug_assert!(!self.is_new());

        // Read the page as 32-bit little-endian words, with pd_checksum zeroed. PG
        // mutates the page in place then restores it; we build the word array
        // instead (same result, no transient page mutation).
        let mut words = [0u32; N_BLOCKS * N_SUMS];
        let bytes = &self.as_bytes()[..BLCKSZ as usize];
        for (w, chunk) in words.iter_mut().zip(bytes.chunks_exact(4)) {
            *w = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        }
        // Zero out the pd_checksum field (a u16 at PD_CHECKSUM_OFFSET). It sits in
        // the low half of the word at PD_CHECKSUM_OFFSET/4 on little-endian.
        debug_assert!(PD_CHECKSUM_OFFSET.is_multiple_of(4));
        words[PD_CHECKSUM_OFFSET / 4] &= 0xFFFF_0000;

        let mut checksum = pg_checksum_block(&words);

        // Mix in the block number to detect transposed pages.
        checksum ^= blkno;

        // Reduce to a u16 with an offset of one, avoiding a checksum of zero.
        ((checksum % 65535) + 1) as u16
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn init_page() -> Box<Page> {
        let mut p = Page::boxed_zeroed();
        p.init(BLCKSZ as usize, 0);
        // Put some non-zero payload in so it's not a trivial page.
        for (i, b) in p.as_mut_bytes()[SizeOfPageHeaderData..SizeOfPageHeaderData + 64]
            .iter_mut()
            .enumerate()
        {
            *b = (i as u8).wrapping_mul(7).wrapping_add(3);
        }
        p
    }

    #[test]
    fn deterministic_same_input() {
        let p = init_page();
        assert_eq!(p.checksum(0), p.checksum(0));
        assert_eq!(p.checksum(42), p.checksum(42));
    }

    #[test]
    fn block_number_changes_checksum() {
        let p = init_page();
        // Different block numbers must (essentially always) give different results.
        assert_ne!(p.checksum(0), p.checksum(1));
        assert_ne!(p.checksum(100), p.checksum(200));
    }

    #[test]
    fn pd_checksum_field_excluded() {
        let mut p = init_page();
        let c0 = p.checksum(7);
        // Mutating the stored pd_checksum (bytes 8,9) must not change the result.
        p.as_mut_bytes()[8] = 0xAB;
        p.as_mut_bytes()[9] = 0xCD;
        let c1 = p.checksum(7);
        assert_eq!(c0, c1);
    }

    #[test]
    fn never_zero() {
        // Even for a (post-init, non-new) page across many blocks, never 0.
        let p = init_page();
        for blk in 0..1000u32 {
            assert_ne!(p.checksum(blk), 0);
        }
    }

    #[test]
    fn regression_anchor() {
        // Fixed page: PageInit'd zeroed page (no payload, special=0), blkno 0/1.
        // These values were cross-checked byte-for-byte against PostgreSQL's
        // checksum_impl.h reference implementation (a standalone C program). If
        // they change, on-disk format compatibility is broken.
        let mut p = Page::boxed_zeroed();
        p.init(BLCKSZ as usize, 0);
        assert_eq!(p.checksum(0), 0x6560); // 25952
        assert_eq!(p.checksum(1), 0x655F); // 25951
    }
}
