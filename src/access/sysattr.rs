//! Translation of postgres/src/include/access/sysattr.h
//!
//! Attribute numbers for the system-defined columns that every heap relation
//! has (ctid, xmin, cmin, xmax, cmax, tableoid).  These are negative AttrNumbers
//! so they never collide with user columns (which are 1-based).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::nodes::primnodes::AttrNumber;

/// ctid - the SelfItemPointer of the tuple.
pub const SelfItemPointerAttributeNumber: AttrNumber = -1;
/// xmin - the inserting transaction id.
pub const MinTransactionIdAttributeNumber: AttrNumber = -2;
/// cmin - the inserting command id.
pub const MinCommandIdAttributeNumber: AttrNumber = -3;
/// xmax - the deleting/locking transaction id.
pub const MaxTransactionIdAttributeNumber: AttrNumber = -4;
/// cmax - the deleting command id.
pub const MaxCommandIdAttributeNumber: AttrNumber = -5;
/// tableoid - the OID of the table the tuple came from.
pub const TableOidAttributeNumber: AttrNumber = -6;
/// One less than the smallest system attribute number; bitmaps of attribute
/// numbers add this offset so system columns map to small positive indexes.
pub const FirstLowInvalidHeapAttributeNumber: AttrNumber = -7;

// NOTE: access/common/heaptuple.rs defines these same consts module-locally but
// typed as c_int (i32) so they match directly in its `attnum: c_int` switch arms;
// this canonical module types them as AttrNumber (i16) for executor/planner
// consumers that work in AttrNumber.  Both are intentional; consumers pick the
// type that avoids a cast at their use site.
