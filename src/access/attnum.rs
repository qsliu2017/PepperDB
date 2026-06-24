//! Translated from PostgreSQL src/include/access/attnum.h

/// User defined attribute numbers start at 1.
pub type AttrNumber = i16;

pub const INVALID_ATTR_NUMBER: AttrNumber = 0;
pub const MAX_ATTR_NUMBER: AttrNumber = 32767;

/// True iff the attribute number is valid.
pub const fn attribute_number_is_valid(attribute_number: AttrNumber) -> bool {
    attribute_number != INVALID_ATTR_NUMBER
}

/// True iff the attribute number corresponds to a user defined attribute.
pub const fn attr_number_is_for_user_defined_attr(attribute_number: AttrNumber) -> bool {
    attribute_number > 0
}

/// Returns the attribute offset for an attribute number (user attrs only).
pub const fn attr_number_get_attr_offset(att_num: AttrNumber) -> AttrNumber {
    att_num - 1
}

/// Returns the attribute number for an attribute offset.
pub const fn attr_offset_get_attr_number(attribute_offset: AttrNumber) -> AttrNumber {
    1 + attribute_offset
}
