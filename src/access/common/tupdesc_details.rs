//! access/tupdesc_details.h - tuple descriptor definitions we can't include everywhere

use crate::c::*;
use crate::postgres::Datum;

/// Structure used to represent value to be used when the attribute is not
/// present at all in a tuple, i.e. when the column was created after the tuple
#[repr(C)]
pub struct AttrMissing {
    /// true if non-NULL missing value exists
    pub am_present: bool,
    /// value when attribute is missing
    pub am_value: Datum,
}
