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
