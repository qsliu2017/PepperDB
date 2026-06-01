//! Translation of postgres/src/include/catalog/pg_range.h
//!
//! The `FormData_pg_range` struct: the fixed-layout part of a pg_range catalog
//! row, defining the "range type" system catalog.  This header has NO
//! CATALOG_VARLEN section, so every declared column is part of the fixed C
//! struct and is included here.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::postgres_ext::Oid;

/* regproc is a C typedef for Oid (a registered-procedure OID). */
pub type regproc = Oid;

/*
 * FormData_pg_range - the fixed part of a pg_range row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_range {
    /* OID of owning range type */
    pub rngtypid: Oid,
    /* OID of range's element type (subtype) */
    pub rngsubtype: Oid,
    /* OID of the range's multirange type */
    pub rngmultitypid: Oid,
    /* collation for this range type, or 0 */
    pub rngcollation: Oid,
    /* subtype's btree opclass */
    pub rngsubopc: Oid,
    /* canonicalize range, or 0 */
    pub rngcanonical: regproc,
    /* subtype difference as a float8, or 0 */
    pub rngsubdiff: regproc,
}

/*
 * Form_pg_range corresponds to a pointer to a row with the format of the
 * pg_range relation.
 */
pub type Form_pg_range = *mut FormData_pg_range;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // rngsubtype sits right after the 4-byte rngtypid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_range, rngsubtype), 4);
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_range>()
                >= core::mem::offset_of!(FormData_pg_range, rngsubdiff)
                    + core::mem::size_of::<regproc>()
        );
    }
}
