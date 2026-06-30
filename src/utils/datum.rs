//! Translated from PostgreSQL src/include/utils/datum.h

use crate::postgres::Datum;

/// datumGetSize - find the "real" length of a datum.
pub fn datum_get_size(value: Datum, typ_by_val: bool, typ_len: i32) -> usize {
    let _ = (value, typ_by_val, typ_len);
    unimplemented!()
}

/// datumCopy - make a copy of a non-NULL datum.
pub fn datum_copy(value: Datum, typ_by_val: bool, typ_len: i32) -> Datum {
    let _ = (value, typ_by_val, typ_len);
    unimplemented!()
}

/// `datumCopy` with Rust ownership: deep-copy a non-NULL datum, returning the copy
/// plus the owned backing buffer for a by-reference (varlena) value. PG's
/// `datumCopy` palloc's the copy into the current memory context; this port has no
/// implicit context, so the caller holds the returned `Box<[u8]>` and the returned
/// `Datum` points into it (a stable heap allocation that travels with the Box).
///
/// - by-value (`typ_by_val`): the Datum is the value; no backing buffer.
/// - varlena (`typ_len == -1`): copy `VARSIZE_ANY` bytes into an owned buffer; the
///   Datum points at it.
/// - fixed-length by-ref (`typ_len > 0`): copy `typ_len` bytes likewise.
///
/// The returned `Datum` is `usize`-shaped (Send); the `Box<[u8]>` owns the bytes,
/// so the pair is self-contained and `Send`.
pub fn datum_copy_owned(value: Datum, typ_by_val: bool, typ_len: i32) -> (Datum, Option<Box<[u8]>>) {
    use crate::postgres::{DatumGetPointer, PointerGetDatum};

    if typ_by_val {
        return (value, None);
    }
    let ptr = DatumGetPointer(value).cast::<u8>();
    if ptr.is_null() {
        return (value, None);
    }
    let size = if typ_len == -1 {
        // SAFETY: a by-ref varlena datum points at a valid varlena header.
        unsafe { crate::varatt::VARSIZE_ANY(ptr) }
    } else {
        crate::assert!(typ_len > 0, "datum_copy_owned: invalid by-ref typlen");
        typ_len as usize
    };
    // SAFETY: `ptr` points at `size` readable bytes (varlena total size or the
    // fixed-length by-ref width).
    let bytes: Box<[u8]> = unsafe { std::slice::from_raw_parts(ptr, size) }.to_vec().into_boxed_slice();
    let datum = PointerGetDatum(bytes.as_ptr());
    (datum, Some(bytes))
}

/// datumTransfer - transfer a non-NULL datum into the current memory context.
pub fn datum_transfer(value: Datum, typ_by_val: bool, typ_len: i32) -> Datum {
    let _ = (value, typ_by_val, typ_len);
    unimplemented!()
}

/// datumIsEqual - logical equality of two datums of the same type.
pub fn datum_is_equal(value1: Datum, value2: Datum, typ_by_val: bool, typ_len: i32) -> bool {
    let _ = (value1, value2, typ_by_val, typ_len);
    unimplemented!()
}

/// datum_image_eq - byte-image equality.
pub fn datum_image_eq(value1: Datum, value2: Datum, typ_by_val: bool, typ_len: i32) -> bool {
    let _ = (value1, value2, typ_by_val, typ_len);
    unimplemented!()
}

/// datum_image_hash - hash based on bits rather than logical value.
pub fn datum_image_hash(value: Datum, typ_by_val: bool, typ_len: i32) -> u32 {
    let _ = (value, typ_by_val, typ_len);
    unimplemented!()
}

/// Estimate serialized size for transfer to parallel workers.
pub fn datum_estimate_space(value: Datum, isnull: bool, typ_by_val: bool, typ_len: i32) -> usize {
    let _ = (value, isnull, typ_by_val, typ_len);
    unimplemented!()
}

/// Serialize a datum into the buffer at start_address.
pub fn datum_serialize(
    value: Datum,
    isnull: bool,
    typ_by_val: bool,
    typ_len: i32,
    start_address: &mut [u8],
) {
    let _ = (value, isnull, typ_by_val, typ_len, start_address);
    unimplemented!()
}

/// Restore a datum; the isnull out-param folds into the Option.
pub fn datum_restore(start_address: &mut &[u8]) -> Option<Datum> {
    let _ = start_address;
    unimplemented!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::{DatumGetInt32, DatumGetPointer, Int32GetDatum};

    /// by-value datums are returned verbatim with no backing buffer.
    #[test]
    fn datum_copy_owned_by_value_is_verbatim() {
        let (copy, backing) = datum_copy_owned(Int32GetDatum(42), true, 4);
        assert_eq!(DatumGetInt32(copy), 42);
        assert!(backing.is_none(), "by-value needs no owned buffer");
    }

    /// A varlena (text) datum is deep-copied into an owned buffer that the copy
    /// points into, and survives the source buffer being dropped/overwritten.
    #[test]
    fn datum_copy_owned_varlena_round_trip_survives_source_drop() {
        // Build a 4B-header varlena holding "abc".
        let payload = b"abc";
        let make = |p: &[u8]| -> Vec<u8> {
            let total = p.len() + 4;
            let mut buf = Vec::with_capacity(total);
            buf.extend_from_slice(&((total as u32) << 2).to_le_bytes());
            buf.extend_from_slice(p);
            buf
        };
        let mut src = make(payload);
        let datum = crate::postgres::PointerGetDatum(src.as_ptr());

        let (copy, backing) = datum_copy_owned(datum, false, -1);
        let backing = backing.expect("varlena copy owns its bytes");

        // Scribble over and drop the source: the copy must be unaffected.
        src.fill(0xFF);
        drop(src);

        // The copy's payload still reads "abc".
        let p = DatumGetPointer(copy).cast::<u8>();
        // SAFETY: `copy` points into the owned `backing` buffer.
        let got = unsafe {
            let len = crate::varatt::VARSIZE_ANY_EXHDR(p);
            let data = crate::varatt::VARDATA_ANY(p);
            std::slice::from_raw_parts(data, len).to_vec()
        };
        assert_eq!(got, payload, "varlena payload deep-copied correctly");
        // The copy datum points into the owned backing (not the freed source).
        assert_eq!(DatumGetPointer(copy).cast::<u8>() as usize, backing.as_ptr() as usize);
    }
}
