//! Translation of postgres/src/include/storage/off.h
//!
//! OffsetNumber definitions: the offset of a tuple within a disk page.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::uint16;
use crate::pg_config::BLCKSZ;

pub type OffsetNumber = uint16;

pub const InvalidOffsetNumber: OffsetNumber = 0;
pub const FirstOffsetNumber: OffsetNumber = 1;
/// MaxOffsetNumber = BLCKSZ / sizeof(ItemIdData); ItemIdData is a 32-bit (4-byte)
/// line pointer (storage/itemid.h), so this is BLCKSZ/4.
pub const MaxOffsetNumber: OffsetNumber = (BLCKSZ / 4) as OffsetNumber;

/// OffsetNumberIsValid.
#[inline]
pub fn OffsetNumberIsValid(offsetNumber: OffsetNumber) -> bool {
    offsetNumber != InvalidOffsetNumber && offsetNumber <= MaxOffsetNumber
}

/// OffsetNumberNext - increment (disambiguation helper, see off.h).
#[inline]
pub fn OffsetNumberNext(offsetNumber: OffsetNumber) -> OffsetNumber {
    offsetNumber.wrapping_add(1)
}
/// OffsetNumberPrev - decrement.
#[inline]
pub fn OffsetNumberPrev(offsetNumber: OffsetNumber) -> OffsetNumber {
    offsetNumber.wrapping_sub(1)
}
