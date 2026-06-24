//! Translated from PostgreSQL src/include/access/sysattr.h

/// Attribute numbers for the system-defined attributes.
pub const SELF_ITEM_POINTER_ATTRIBUTE_NUMBER: i16 = -1;
pub const MIN_TRANSACTION_ID_ATTRIBUTE_NUMBER: i16 = -2;
pub const MIN_COMMAND_ID_ATTRIBUTE_NUMBER: i16 = -3;
pub const MAX_TRANSACTION_ID_ATTRIBUTE_NUMBER: i16 = -4;
pub const MAX_COMMAND_ID_ATTRIBUTE_NUMBER: i16 = -5;
pub const TABLE_OID_ATTRIBUTE_NUMBER: i16 = -6;
pub const FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER: i16 = -7;
