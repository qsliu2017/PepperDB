//! Page-level operations for PostgreSQL's 8KB buffer page format.
//! Constants, helpers, ItemId packing, page init/checksums used by both
//! access methods (heap, nbtree) and the storage manager (smgr).

pub const PAGE_SIZE: usize = 8192;

// -- Page header (28 bytes) ---------------------------------------------------

pub(crate) const PD_LOWER: usize = 12; // u16 at byte 12
pub(crate) const PD_UPPER: usize = 14; // u16 at byte 14
pub(crate) const PD_SPECIAL: usize = 16; // u16 at byte 16
pub(crate) const PD_PAGESIZE_VERSION: usize = 18; // u16 at byte 18
pub(crate) const HEADER_SIZE: usize = 28;

/// pd_pagesize_version: page size in high bits, version 4 in low byte
pub(crate) const PG_PAGE_SIZE_VERSION: u16 = (PAGE_SIZE as u16 & 0xFF00) | 4;

// -- ItemId bitfield ----------------------------------------------------------

pub(crate) const ITEM_ID_SIZE: usize = 4;

pub(crate) const LP_NORMAL: u8 = 1;

// -- MAXALIGN (8 bytes, matching PostgreSQL on 64-bit) ------------------------

pub(crate) fn maxalign(v: usize) -> usize {
    (v + 7) & !7
}

// -- Read/write helpers -------------------------------------------------------

pub(crate) fn read_u16(buf: &[u8], off: usize) -> u16 {
    u16::from_le_bytes([buf[off], buf[off + 1]])
}

pub(crate) fn write_u16(buf: &mut [u8], off: usize, val: u16) {
    buf[off..off + 2].copy_from_slice(&val.to_le_bytes());
}

pub(crate) fn read_u32(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]])
}

pub(crate) fn write_u32(buf: &mut [u8], off: usize, val: u32) {
    buf[off..off + 4].copy_from_slice(&val.to_le_bytes());
}

// -- ItemId packing -----------------------------------------------------------

pub(crate) fn pack_item_id(offset: u16, flags: u8, length: u16) -> u32 {
    (offset as u32 & 0x7FFF) | ((flags as u32 & 0x3) << 15) | ((length as u32 & 0x7FFF) << 17)
}

pub(crate) fn unpack_item_id(id: u32) -> (u16, u8, u16) {
    let offset = (id & 0x7FFF) as u16;
    let flags = ((id >> 15) & 0x3) as u8;
    let length = ((id >> 17) & 0x7FFF) as u16;
    (offset, flags, length)
}

// -- Page operations ----------------------------------------------------------

pub fn init_page(buf: &mut [u8; PAGE_SIZE]) {
    buf.fill(0);
    write_u16(buf, PD_LOWER, HEADER_SIZE as u16);
    write_u16(buf, PD_UPPER, PAGE_SIZE as u16);
    write_u16(buf, PD_SPECIAL, PAGE_SIZE as u16);
    write_u16(buf, PD_PAGESIZE_VERSION, PG_PAGE_SIZE_VERSION);
}

pub fn num_items(buf: &[u8; PAGE_SIZE]) -> u16 {
    let pd_lower = read_u16(buf, PD_LOWER) as usize;
    ((pd_lower - HEADER_SIZE) / ITEM_ID_SIZE) as u16
}

// -- Page LSN -----------------------------------------------------------------

/// Write LSN to pd_lsn (bytes 0-7) of page header.
pub fn set_page_lsn(buf: &mut [u8; PAGE_SIZE], lsn: u64) {
    buf[0..8].copy_from_slice(&lsn.to_le_bytes());
}

/// Read LSN from pd_lsn (bytes 0-7) of page header.
pub fn get_page_lsn(buf: &[u8; PAGE_SIZE]) -> u64 {
    u64::from_le_bytes(buf[0..8].try_into().unwrap())
}

// -- Page checksums (PostgreSQL checksum_impl.h) ------------------------------

const PD_CHECKSUM: usize = 8; // u16 at byte 8

/// FNV-1a shuffle constants from PostgreSQL's checksum_impl.h.
const FNV_PRIME: u32 = 0x01000193;
const FNV_OFFSET: u32 = 0x811C9DC5;

/// Number of u32 words in a page.
const N_SUMS: usize = 32;

/// Compute PG-compatible page checksum (FNV-1a variant mixed with block number).
pub fn compute_checksum(page: &[u8; PAGE_SIZE], blkno: u32) -> u16 {
    let mut sums = [0u32; N_SUMS];

    // Process page as u32 words, XOR-folding into N_SUMS accumulators
    let words = PAGE_SIZE / 4;
    for i in 0..words {
        let off = i * 4;
        let word = u32::from_le_bytes([page[off], page[off + 1], page[off + 2], page[off + 3]]);
        sums[i % N_SUMS] = sums[i % N_SUMS].wrapping_add(word);
    }

    // FNV-1a hash of the accumulators
    let mut result = FNV_OFFSET;
    for &s in &sums {
        let b0 = (s & 0xFF) as u8;
        let b1 = ((s >> 8) & 0xFF) as u8;
        let b2 = ((s >> 16) & 0xFF) as u8;
        let b3 = ((s >> 24) & 0xFF) as u8;
        result ^= b0 as u32;
        result = result.wrapping_mul(FNV_PRIME);
        result ^= b1 as u32;
        result = result.wrapping_mul(FNV_PRIME);
        result ^= b2 as u32;
        result = result.wrapping_mul(FNV_PRIME);
        result ^= b3 as u32;
        result = result.wrapping_mul(FNV_PRIME);
    }

    // Mix in the block number
    result ^= blkno;

    // Reduce to u16, avoiding zero (which means "no checksum")
    let checksum = ((result >> 16) ^ (result & 0xFFFF)) as u16;
    if checksum == 0 {
        1
    } else {
        checksum
    }
}

/// Write checksum into pd_checksum (bytes 8-9). Zeros the field first.
pub fn set_checksum(page: &mut [u8; PAGE_SIZE], blkno: u32) {
    // Zero the checksum field before computing
    write_u16(page, PD_CHECKSUM, 0);
    let cksum = compute_checksum(page, blkno);
    write_u16(page, PD_CHECKSUM, cksum);
}

/// Verify page checksum. All-zero pages (uninitialized) always pass.
pub fn verify_checksum(page: &[u8; PAGE_SIZE], blkno: u32) -> bool {
    if page.iter().all(|&b| b == 0) {
        return true;
    }
    let stored = read_u16(page, PD_CHECKSUM);
    // Zero the field, recompute, compare
    let mut tmp = *page;
    write_u16(&mut tmp, PD_CHECKSUM, 0);
    let computed = compute_checksum(&tmp, blkno);
    stored == computed
}
