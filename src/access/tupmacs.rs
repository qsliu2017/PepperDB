//! Translated from PostgreSQL src/include/access/tupmacs.h

use crate::c::{bits8, DOUBLEALIGN, INTALIGN, SHORTALIGN, TYPEALIGN};
use crate::postgres::Datum;
use crate::varatt::{VARATT_IS_SHORT, VARATT_NOT_PAD_BYTE, VARSIZE_ANY};

// TYPALIGN_* alignment-code chars (pg_type.h hand-translated #defines, not
// catalog-generated). Kept here as u8 for the alignment arithmetic below; the
// pg_type column form (i8) lives in crate::catalog::pg_type.
pub const TYPALIGN_CHAR: u8 = b'c';
pub const TYPALIGN_SHORT: u8 = b's';
pub const TYPALIGN_INT: u8 = b'i';
pub const TYPALIGN_DOUBLE: u8 = b'd';

/// Check a tuple's null bitmap. A 0 bit indicates null; 1 indicates non-null.
pub fn att_isnull(att: i32, bits: &[bits8]) -> bool {
    bits[(att >> 3) as usize] & (1 << (att & 0x07)) == 0
}

/// Fetch a byval/byref attribute value from a tuple data pointer, returning a
/// Datum. For byref, returns the pointer as a Datum; for byval, the value is
/// loaded and extended to Datum width.
///
/// The load uses `read_unaligned`: a tuple's data area is laid out at each
/// attribute's natural boundary, so byval fields are normally aligned (and this
/// compiles to a plain load), but the access must not *assume* alignment -- a
/// tuple may be overlaid on a buffer at an arbitrary address, and an aligned
/// read of a misaligned field is UB (and traps on strict-alignment targets).
///
/// SAFETY: `t` must point to at least `attlen` valid bytes for byval.
pub unsafe fn fetch_att(t: *const u8, attbyval: bool, attlen: i32) -> Datum {
    if attbyval {
        match attlen {
            1 => Datum((t.cast::<i8>().read_unaligned()) as usize),
            2 => Datum((t.cast::<i16>().read_unaligned()) as usize),
            4 => Datum((t.cast::<i32>().read_unaligned()) as usize),
            8 => t.cast::<Datum>().read_unaligned(),
            _ => panic!("unsupported byval length: {attlen}"),
        }
    } else {
        Datum(t as usize)
    }
}

/// Align `cur_offset` for a Datum of alignment `attalign` and length `attlen`.
/// Short varlenas (attlen == -1 and short header) are not aligned.
///
/// SAFETY: for a short varlena `attdatum` must point at a valid varlena header.
pub unsafe fn att_align_datum(
    cur_offset: usize,
    attalign: u8,
    attlen: i32,
    attdatum: Datum,
) -> usize {
    if attlen == -1 && VARATT_IS_SHORT(attdatum.0 as *const u8) {
        cur_offset
    } else {
        att_align_nominal(cur_offset, attalign)
    }
}

/// Same as att_align_datum but takes an alignment in bytes (attalignby).
///
/// SAFETY: see att_align_datum.
pub unsafe fn att_datum_alignby(
    cur_offset: usize,
    attalignby: usize,
    attlen: i32,
    attdatum: Datum,
) -> usize {
    if attlen == -1 && VARATT_IS_SHORT(attdatum.0 as *const u8) {
        cur_offset
    } else {
        TYPEALIGN(attalignby, cur_offset)
    }
}

/// Align `cur_offset` while walking a tuple; peeks at attptr to detect a short
/// varlena (1-byte header) vs a pad byte / 4-byte length word.
///
/// SAFETY: `attptr` must be readable when attlen == -1.
pub unsafe fn att_align_pointer(
    cur_offset: usize,
    attalign: u8,
    attlen: i32,
    attptr: *const u8,
) -> usize {
    if attlen == -1 && VARATT_NOT_PAD_BYTE(attptr) {
        cur_offset
    } else {
        att_align_nominal(cur_offset, attalign)
    }
}

/// Same as att_align_pointer but takes an alignment in bytes (attalignby).
///
/// SAFETY: see att_align_pointer.
pub unsafe fn att_pointer_alignby(
    cur_offset: usize,
    attalignby: usize,
    attlen: i32,
    attptr: *const u8,
) -> usize {
    if attlen == -1 && VARATT_NOT_PAD_BYTE(attptr) {
        cur_offset
    } else {
        TYPEALIGN(attalignby, cur_offset)
    }
}

/// Align `cur_offset` for `attalign`, ignoring packed (short) varlenas.
pub fn att_align_nominal(cur_offset: usize, attalign: u8) -> usize {
    if attalign == TYPALIGN_INT {
        INTALIGN(cur_offset)
    } else if attalign == TYPALIGN_CHAR {
        cur_offset
    } else if attalign == TYPALIGN_DOUBLE {
        DOUBLEALIGN(cur_offset)
    } else {
        debug_assert_eq!(attalign, TYPALIGN_SHORT);
        SHORTALIGN(cur_offset)
    }
}

/// Same as att_align_nominal but takes an alignment in bytes (attalignby).
pub fn att_nominal_alignby(cur_offset: usize, attalignby: usize) -> usize {
    TYPEALIGN(attalignby, cur_offset)
}

/// Increment `cur_offset` by the space needed for the Datum.
///
/// SAFETY: for variable-length attrs `attdatum` must point at a valid datum.
pub unsafe fn att_addlength_datum(cur_offset: usize, attlen: i32, attdatum: Datum) -> usize {
    att_addlength_pointer(cur_offset, attlen, attdatum.0 as *const u8)
}

/// Increment `cur_offset` by the space needed for the field at `attptr`.
///
/// SAFETY: for attlen == -1 or -2, `attptr` must point at a valid
/// varlena / null-terminated C string respectively.
pub unsafe fn att_addlength_pointer(cur_offset: usize, attlen: i32, attptr: *const u8) -> usize {
    if attlen > 0 {
        cur_offset + attlen as usize
    } else if attlen == -1 {
        cur_offset + VARSIZE_ANY(attptr)
    } else {
        debug_assert_eq!(attlen, -2);
        let mut len = 0usize;
        while attptr.add(len).read() != 0 {
            len += 1;
        }
        cur_offset + len + 1
    }
}

/// Store a byval Datum into a tuple data area. Only the byval case is handled.
///
/// Uses `write_unaligned` for the same reason as [`fetch_att`]: the data area is
/// normally aligned (so this is a plain store), but the write must not assume it.
///
/// SAFETY: `t` must point to at least `attlen` writable bytes.
pub unsafe fn store_att_byval(t: *mut u8, newdatum: Datum, attlen: i32) {
    match attlen {
        1 => t.write_unaligned(newdatum.0 as u8),
        2 => t.cast::<i16>().write_unaligned(newdatum.0 as i16),
        4 => t.cast::<i32>().write_unaligned(newdatum.0 as i32),
        8 => t.cast::<Datum>().write_unaligned(newdatum),
        _ => panic!("unsupported byval length: {attlen}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `fetch_att`/`store_att_byval` must round-trip every byval width at a
    /// deliberately misaligned address (the on-disk format can place a byval
    /// field off its natural boundary). An aligned read/write here would be UB
    /// and trap on strict-alignment targets; the unaligned accessors must not.
    /// Run under `MIRIFLAGS=-Zmiri-symbolic-alignment-check` to verify no
    /// alignment assumption survives.
    #[test]
    fn byval_roundtrip_at_misaligned_address() {
        let mut buf = [0u8; 32];
        for &off in &[1usize, 3, 5, 7] {
            // SAFETY: off+8 <= 32; the pointer stays inside `buf`.
            unsafe {
                let p = buf.as_mut_ptr().add(off);
                store_att_byval(p, Datum(0x0102_0304_0506_0708), 8);
                assert_eq!(fetch_att(p, true, 8), Datum(0x0102_0304_0506_0708));
                store_att_byval(p, Datum(0x1122_3344), 4);
                assert_eq!(fetch_att(p, true, 4), Datum(0x1122_3344));
                store_att_byval(p, Datum(0x5566), 2);
                assert_eq!(fetch_att(p, true, 2), Datum(0x5566));
                store_att_byval(p, Datum(0x77), 1);
                assert_eq!(fetch_att(p, true, 1), Datum(0x77));
            }
        }
    }
}
