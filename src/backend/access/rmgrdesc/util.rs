//! Shared byte-reading helpers for the rmgr descriptor routines. The `*_desc`
//! routines render a decoded record's `main_data` (the `#[repr(C)]` on-disk WAL
//! payload); these read fields out of that byte slice, native-endian, matching
//! the WAL assembler on the little-endian target. All reads return a default
//! (0) rather than panicking on a short / malformed record, so a description can
//! never crash recovery or pg_waldump.

/// Read a `u8` at `off` (0 if out of range).
#[inline]
pub(super) fn u8_at(b: &[u8], off: usize) -> u8 {
    b.get(off).copied().unwrap_or(0)
}

/// Read a native-endian `u16` at `off` (0 if out of range).
#[inline]
pub(super) fn u16_at(b: &[u8], off: usize) -> u16 {
    match b.get(off..off + 2) {
        Some(&[a, c]) => u16::from_ne_bytes([a, c]),
        _ => 0,
    }
}

/// Read a native-endian `u32` at `off` (0 if out of range).
#[inline]
pub(super) fn u32_at(b: &[u8], off: usize) -> u32 {
    match b.get(off..off + 4) {
        Some(&[b0, b1, b2, b3]) => u32::from_ne_bytes([b0, b1, b2, b3]),
        _ => 0,
    }
}

/// Read a native-endian `i32` at `off` (0 if out of range).
#[inline]
pub(super) fn i32_at(b: &[u8], off: usize) -> i32 {
    u32_at(b, off) as i32
}

/// Read a native-endian `u64` at `off` (0 if out of range).
#[inline]
pub(super) fn u64_at(b: &[u8], off: usize) -> u64 {
    match b.get(off..off + 8) {
        Some(&[b0, b1, b2, b3, b4, b5, b6, b7]) => {
            u64::from_ne_bytes([b0, b1, b2, b3, b4, b5, b6, b7])
        }
        _ => 0,
    }
}

/// Read a native-endian `i64` at `off` (0 if out of range).
#[inline]
pub(super) fn i64_at(b: &[u8], off: usize) -> i64 {
    u64_at(b, off) as i64
}

/// `LSN_FORMAT_ARGS`: render an `XLogRecPtr` as `%X/%X` (high 32 / low 32).
#[inline]
pub(super) fn lsn_str(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn as u32)
}
