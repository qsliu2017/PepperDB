//! Translated from PostgreSQL src/include/access/tupdesc_details.h

use crate::postgres::Datum;

/// Value used when an attribute is not present in a tuple (column added later).
pub struct AttrMissing {
    /// true if non-NULL missing value exists
    pub am_present: bool,
    /// value when attribute is missing
    pub am_value: Datum,
}
