//! Translated from PostgreSQL src/include/storage/off.h

use crate::pg_config::BLCKSZ;
use crate::storage::itemid::ItemIdData;

/// 1-based index into the linp (ItemIdData) array in each disk page header.
pub type OffsetNumber = u16;

pub const INVALID_OFFSET_NUMBER: OffsetNumber = 0;
pub const FIRST_OFFSET_NUMBER: OffsetNumber = 1;
pub const MAX_OFFSET_NUMBER: OffsetNumber =
    (BLCKSZ as usize / core::mem::size_of::<ItemIdData>()) as OffsetNumber;

/// True iff the offset number is valid.
pub const fn offset_number_is_valid(offset_number: OffsetNumber) -> bool {
    offset_number != INVALID_OFFSET_NUMBER && offset_number <= MAX_OFFSET_NUMBER
}

/// Increment the argument (move to next offset).
pub const fn offset_number_next(offset_number: OffsetNumber) -> OffsetNumber {
    1 + offset_number
}

/// Decrement the argument (move to previous offset).
pub const fn offset_number_prev(offset_number: OffsetNumber) -> OffsetNumber {
    offset_number.wrapping_sub(1)
}
