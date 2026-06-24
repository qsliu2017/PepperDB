//! Translated from PostgreSQL src/include/access/detoast.h

use crate::postgres::Datum;

// TODO(struct-forward): varlena lives in c.h; repoint to crate::c in Phase 2.
#[deprecated(note = "TODO(struct-forward): repoint to crate::c::varlena in Phase 2")]
pub struct varlena {
    _opaque: [u8; 0],
}

// VARATT_EXTERNAL_GET_POINTER is an aligned-copy macro over a toast pointer;
// it depends on varatt layout (c.h) and is reconstructed in Phase 2.

/// Fetch an external stored attribute from the toast relation (no decompress).
#[allow(deprecated)]
pub fn detoast_external_attr(_attr: &varlena) -> &varlena {
    unimplemented!()
}

/// Fully detoast one attribute, fetching and/or decompressing as needed.
#[allow(deprecated)]
pub fn detoast_attr(_attr: &varlena) -> &varlena {
    unimplemented!()
}

/// Fetch only the specified portion of an attribute.
#[allow(deprecated)]
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
