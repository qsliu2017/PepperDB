//! Translation of postgres/src/include/catalog/pg_amop.h
//!
//! The `FormData_pg_amop` struct: the fixed-layout part of a pg_amop catalog
//! row.  The amop table identifies the operators associated with each index
//! operator family and operator class (classes are subsets of families).  An
//! associated operator can be either a search operator or an ordering operator,
//! as identified by amoppurpose.
//!
//! This header has NO `#ifdef CATALOG_VARLEN` section, so every declared column
//! is part of the fixed struct.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::int16;
use crate::postgres_ext::Oid;
use core::ffi::c_char;

/*
 * FormData_pg_amop - the fixed part of a pg_amop row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_amop {
    /* oid */
    pub oid: Oid,
    /* the index opfamily this entry is for */
    pub amopfamily: Oid,
    /* operator's left input data type */
    pub amoplefttype: Oid,
    /* operator's right input data type */
    pub amoprighttype: Oid,
    /* operator strategy number */
    pub amopstrategy: int16,
    /* is operator for 's'earch or 'o'rdering? */
    pub amoppurpose: c_char,
    /* the operator's pg_operator OID */
    pub amopopr: Oid,
    /* the index access method this entry is for */
    pub amopmethod: Oid,
    /* ordering opfamily OID, or 0 if search op */
    pub amopsortfamily: Oid,
}

/*
 * Form_pg_amop corresponds to a pointer to a tuple with the format of the
 * pg_amop relation.
 */
pub type Form_pg_amop = *mut FormData_pg_amop;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 * ----------------------------------------------------------------
 */

/* allowed values of amoppurpose: */
pub const AMOP_SEARCH: c_char = b's' as c_char; /* operator is for search */
pub const AMOP_ORDER: c_char = b'o' as c_char; /* operator is for ordering */

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // amopfamily sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_amop, amopfamily), 4);
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_amop>()
                >= core::mem::offset_of!(FormData_pg_amop, amopsortfamily)
                    + core::mem::size_of::<Oid>()
        );
    }
}
