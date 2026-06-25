//! Translated from PostgreSQL src/include/access/detoast.h

use crate::c::varlena;
use crate::postgres::Datum;

// VARATT_EXTERNAL_GET_POINTER is an aligned-copy macro over a toast pointer;
// it depends on varatt layout (c.h) and is reconstructed in Phase 2.

/// Fetch an external stored attribute from the toast relation (no decompress).
pub fn detoast_external_attr(_attr: &varlena) -> &varlena {
    unimplemented!()
}

/// Fully detoast one attribute, fetching and/or decompressing as needed.
pub fn detoast_attr(_attr: &varlena) -> &varlena {
    unimplemented!()
}

/// Fetch only the specified portion of an attribute.
pub fn detoast_attr_slice(_attr: &varlena, _sliceoffset: i32, _slicelength: i32) -> &varlena {
    unimplemented!()
}

/// Return the raw (detoasted) size of a varlena datum.
pub fn toast_raw_datum_size(_value: Datum) -> usize {
    unimplemented!()
}

/// Return the storage size of a varlena datum.
pub fn toast_datum_size(_value: Datum) -> usize {
    unimplemented!()
}
