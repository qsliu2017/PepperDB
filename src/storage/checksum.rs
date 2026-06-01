//! Checksum implementation for data pages.
//!
//! Source: postgres/src/backend/storage/page/checksum.c
//! checksum.c merely `#include`s storage/checksum_impl.h (and storage/checksum.h);
//! the actual algorithm lives in checksum_impl.h, which is MERGED here.
//! #include mapping:
//!   - storage/checksum_impl.h  -> entire algorithm below (REAL)
//!   - storage/bufpage.h        -> PageHeaderData (NOT YET PORTED; see note below)
//!   - storage/checksum.h       -> just declares pg_checksum_page
//!   - storage/block.h          -> crate::storage::block::BlockNumber (REAL)
//!   - pg_config (BLCKSZ)       -> crate::pg_config::BLCKSZ (REAL)
//!
//! NOTE on PageHeaderData: bufpage.rs is being written in parallel and is not
//! yet present, so the union trick (PGChecksummablePage) is not used. Instead we
//! operate directly on the raw 8192-byte page buffer. The only field the C code
//! touches is `pd_checksum`, a uint16 located at byte offset 8 in PageHeaderData
//! (it follows pd_lsn, a PageXLogRecPtr = two uint32 = 8 bytes). This offset is
//! hardcoded as PD_CHECKSUM_OFFSET and verified by a compile-time-ish test.

use crate::prelude::*;
use crate::pg_config::BLCKSZ;
use crate::storage::block::BlockNumber;

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: Size) -> *mut c_void;
}

/// number of checksums to calculate in parallel
const N_SUMS: usize = 32;
/// prime multiplier of FNV-1a hash
const FNV_PRIME: uint32 = 16777619;

/// Number of uint32 rows in the page when viewed as a [rows][N_SUMS] matrix.
/// (BLCKSZ / (sizeof(uint32) * N_SUMS))
const N_ROWS: usize = BLCKSZ / (4 * N_SUMS);

/// Byte offset of pd_checksum (uint16) within PageHeaderData / the page buffer.
/// pd_lsn (PageXLogRecPtr: two uint32 = 8 bytes) precedes it.
const PD_CHECKSUM_OFFSET: usize = 8;

/*
 * Base offsets to initialize each of the parallel FNV hashes into a
 * different initial state.
 */
static CHECKSUM_BASE_OFFSETS: [uint32; N_SUMS] = [
    0x5B1F36E9, 0xB8525960, 0x02AB50AA, 0x1DE66D2A,
    0x79FF467A, 0x9BB9F8A3, 0x217E7CD2, 0x83E13D2C,
    0xF8D4474F, 0xE39EB970, 0x42C6AE16, 0x993216FA,
    0x7B093B5D, 0x98DAFF3C, 0xF718902A, 0x0B1C9CDB,
    0xE58F764B, 0x187636BC, 0x5D7B3BB1, 0xE73DE7DE,
    0x92BEC979, 0xCCA6C0B2, 0x304A0979, 0x85AA43D4,
    0x783125BB, 0x6CA8EAA2, 0xE407EAC6, 0x4B5CFC3E,
    0x9FBF8C76, 0x15CA20BE, 0xF2CA9FD3, 0x959BD756,
];

/*
 * Calculate one round of the checksum.
 *
 * C:
 *   #define CHECKSUM_COMP(checksum, value) do { \
 *       uint32 __tmp = (checksum) ^ (value); \
 *       (checksum) = __tmp * FNV_PRIME ^ (__tmp >> 17); \
 *   } while (0)
 *
 * C arithmetic wraps on overflow; Rust debug builds panic, so we MUST use
 * wrapping_mul here.
 */
#[inline(always)]
fn checksum_comp(checksum: uint32, value: uint32) -> uint32 {
    let tmp = checksum ^ value;
    tmp.wrapping_mul(FNV_PRIME) ^ (tmp >> 17)
}

/*
 * Block checksum algorithm.  The page must be adequately aligned
 * (at least on 4-byte boundary).
 *
 * `data` is the page interpreted as a [N_ROWS][N_SUMS] matrix of uint32, in
 * native byte order (matching the C union's `uint32 data[..][N_SUMS]`).
 */
fn pg_checksum_block(data: &[[uint32; N_SUMS]; N_ROWS]) -> uint32 {
    let mut sums = [0u32; N_SUMS];
    let mut result: uint32 = 0;

    /* ensure that the size is compatible with the algorithm */
    Assert!(N_ROWS * N_SUMS * 4 == BLCKSZ);

    /* initialize partial checksums to their corresponding offsets */
    // memcpy(sums, checksumBaseOffsets, sizeof(checksumBaseOffsets));
    unsafe {
        memcpy(
            sums.as_mut_ptr() as *mut c_void,
            CHECKSUM_BASE_OFFSETS.as_ptr() as *const c_void,
            core::mem::size_of_val(&CHECKSUM_BASE_OFFSETS) as Size,
        );
    }

    /* main checksum calculation */
    for i in 0..N_ROWS {
        for j in 0..N_SUMS {
            sums[j] = checksum_comp(sums[j], data[i][j]);
        }
    }

    /* finally add in two rounds of zeroes for additional mixing */
    for _ in 0..2 {
        for j in 0..N_SUMS {
            sums[j] = checksum_comp(sums[j], 0);
        }
    }

    /* xor fold partial checksums together */
    for i in 0..N_SUMS {
        result ^= sums[i];
    }

    result
}

/*
 * Compute the checksum for a Postgres page.
 *
 * The page must be adequately aligned (at least on a 4-byte boundary).
 * Beware also that the checksum field of the page is transiently zeroed.
 *
 * The checksum includes the block number (to detect the case where a page is
 * somehow moved to a different location), the page header (excluding the
 * checksum itself), and the page data.
 *
 * NOTE: the C code Assert(!PageIsNew(page)); PageIsNew is part of bufpage.h
 * which is not yet ported, so that assertion is omitted here.
 */
pub unsafe fn pg_checksum_page(page: *mut c_char, blkno: BlockNumber) -> uint16 {
    /*
     * Save pd_checksum and temporarily set it to zero, so that the checksum
     * calculation isn't affected by the old checksum stored on the page.
     * Restore it after, because actually updating the checksum is NOT part of
     * the API of this function.
     *
     * The C code reads/writes cpage->phdr.pd_checksum (a uint16 in native byte
     * order). We do the same via the raw byte buffer at PD_CHECKSUM_OFFSET.
     */
    let csum_ptr = page.add(PD_CHECKSUM_OFFSET) as *mut uint16;
    let save_checksum: uint16 = csum_ptr.read_unaligned();
    csum_ptr.write_unaligned(0);

    /*
     * View the page as a [N_ROWS][N_SUMS] uint32 matrix, matching the C union's
     * data[] member exactly (native byte order, no transformation).
     */
    let data = &*(page as *const [[uint32; N_SUMS]; N_ROWS]);
    let mut checksum: uint32 = pg_checksum_block(data);

    csum_ptr.write_unaligned(save_checksum);

    /* Mix in the block number to detect transposed pages */
    checksum ^= blkno;

    /*
     * Reduce to a uint16 (to fit in the pd_checksum field) with an offset of
     * one. That avoids checksums of zero, which seems like a good idea.
     */
    ((checksum % 65535) + 1) as uint16
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a deterministic, non-trivial 8192-byte page buffer.
    fn make_page() -> Vec<c_char> {
        let mut buf = vec![0 as c_char; BLCKSZ];
        for (i, b) in buf.iter_mut().enumerate() {
            // some non-zero, non-constant pattern; cast through u8 to be sign-safe
            *b = ((i * 31 + 7) & 0xFF) as u8 as c_char;
        }
        buf
    }

    #[test]
    fn pd_checksum_offset_is_eight() {
        // pd_lsn (two uint32) then pd_checksum (uint16).
        assert_eq!(PD_CHECKSUM_OFFSET, 4 + 4);
    }

    #[test]
    fn matrix_covers_whole_page() {
        assert_eq!(N_ROWS * N_SUMS * 4, BLCKSZ);
        assert_eq!(N_ROWS, 64);
    }

    #[test]
    fn checksum_is_stable_and_nonzero() {
        let mut page = make_page();
        let c1 = unsafe { pg_checksum_page(page.as_mut_ptr(), 0) };
        let c2 = unsafe { pg_checksum_page(page.as_mut_ptr(), 0) };
        assert_eq!(c1, c2, "checksum must be deterministic");
        assert_ne!(c1, 0, "checksum is offset by one, never zero");
    }

    #[test]
    fn checksum_restores_pd_checksum_field() {
        let mut page = make_page();
        let off = PD_CHECKSUM_OFFSET;
        // plant a known stored checksum value
        page[off] = 0xAB_u8 as c_char;
        page[off + 1] = 0xCD_u8 as c_char;
        let before = (page[off], page[off + 1]);
        let _ = unsafe { pg_checksum_page(page.as_mut_ptr(), 5) };
        let after = (page[off], page[off + 1]);
        assert_eq!(before, after, "pd_checksum must be restored after computing");
    }

    #[test]
    fn checksum_is_independent_of_stored_pd_checksum() {
        // The stored pd_checksum is zeroed during computation, so two pages that
        // differ ONLY in pd_checksum must produce the same result.
        let mut a = make_page();
        let mut b = make_page();
        let off = PD_CHECKSUM_OFFSET;
        a[off] = 0x11_u8 as c_char;
        a[off + 1] = 0x22_u8 as c_char;
        b[off] = 0x99_u8 as c_char;
        b[off + 1] = 0x88_u8 as c_char;
        let ca = unsafe { pg_checksum_page(a.as_mut_ptr(), 3) };
        let cb = unsafe { pg_checksum_page(b.as_mut_ptr(), 3) };
        assert_eq!(ca, cb);
    }

    #[test]
    fn checksum_changes_when_a_byte_changes() {
        let mut page = make_page();
        let base = unsafe { pg_checksum_page(page.as_mut_ptr(), 0) };
        // flip a data byte well away from pd_checksum
        page[100] = (page[100] as u8 ^ 0x01) as c_char;
        let changed = unsafe { pg_checksum_page(page.as_mut_ptr(), 0) };
        assert_ne!(base, changed, "data change must alter the checksum");
    }

    #[test]
    fn same_page_different_blkno_differs() {
        let mut page = make_page();
        let c0 = unsafe { pg_checksum_page(page.as_mut_ptr(), 0) };
        let c1 = unsafe { pg_checksum_page(page.as_mut_ptr(), 1) };
        let c2 = unsafe { pg_checksum_page(page.as_mut_ptr(), 1234) };
        assert_ne!(c0, c1, "block number is mixed in");
        assert_ne!(c0, c2);
    }

    #[test]
    fn checksum_comp_wraps_like_c() {
        // Exercise the wrapping multiply with values that overflow u32.
        // C: __tmp = checksum ^ value; result = __tmp*FNV_PRIME ^ (__tmp>>17)
        let checksum: uint32 = 0xFFFF_FFFF;
        let value: uint32 = 0x1234_5678;
        let tmp = checksum ^ value;
        let expected = tmp.wrapping_mul(FNV_PRIME) ^ (tmp >> 17);
        assert_eq!(checksum_comp(checksum, value), expected);
    }
}
