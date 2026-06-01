//! Translation of postgres/src/include/catalog/pg_transform.h
//!
//! The `FormData_pg_transform` struct: the fixed-layout part of a pg_transform
//! catalog row.  The C header has no `#ifdef CATALOG_VARLEN` cutoff, so every
//! column is part of this in-memory struct.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::postgres_ext::Oid;

/* regproc is a C typedef for Oid (a registered-procedure OID). */
pub type regproc = Oid;

/*
 * FormData_pg_transform - the fixed part of a pg_transform row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_transform {
    /* oid */
    pub oid: Oid,
    /* OID of the data type this transform is for */
    pub trftype: Oid,
    /* OID of the language this transform is for */
    pub trflang: Oid,
    /* function to convert the type to the language's representation */
    pub trffromsql: regproc,
    /* function to convert the language's representation to the type */
    pub trftosql: regproc,
}

/*
 * Form_pg_transform corresponds to a pointer to a tuple with the format of the
 * pg_transform relation.
 */
pub type Form_pg_transform = *mut FormData_pg_transform;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 *
 * (pg_transform.h exposes no #define constants.)
 * ----------------------------------------------------------------
 */

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // trftype sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_transform, trftype), 4);
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_transform>()
                >= core::mem::offset_of!(FormData_pg_transform, trftosql)
                    + core::mem::size_of::<regproc>()
        );
    }
}
