//! access/attnum.h - POSTGRES attribute number definitions.

use crate::c::*;

/*
 * user defined attribute numbers start at 1.   -ay 2/95
 */
pub type AttrNumber = int16;

pub const InvalidAttrNumber: AttrNumber = 0;
pub const MaxAttrNumber: AttrNumber = 32767;

/* ----------------
 *		support macros
 * ----------------
 */

/// AttributeNumberIsValid
///		True iff the attribute number is valid.
#[inline]
pub fn AttributeNumberIsValid(attributeNumber: AttrNumber) -> bool {
    attributeNumber != InvalidAttrNumber
}

/// AttrNumberIsForUserDefinedAttr
///		True iff the attribute number corresponds to a user defined attribute.
#[inline]
pub fn AttrNumberIsForUserDefinedAttr(attributeNumber: AttrNumber) -> bool {
    attributeNumber > 0
}

/// AttrNumberGetAttrOffset
///		Returns the attribute offset for an attribute number.
///
/// Note:
///		Assumes the attribute number is for a user defined attribute.
#[inline]
pub fn AttrNumberGetAttrOffset(attNum: AttrNumber) -> AttrNumber {
    crate::AssertMacro!(AttrNumberIsForUserDefinedAttr(attNum));
    attNum - 1
}

/// AttrOffsetGetAttrNumber
///		Returns the attribute number for an attribute offset.
#[inline]
pub fn AttrOffsetGetAttrNumber(attributeOffset: AttrNumber) -> AttrNumber {
    1 + attributeOffset
}
