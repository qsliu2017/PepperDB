//! Translation of postgres/src/include/catalog/pg_sequence.h
//!
//! The `FormData_pg_sequence` struct: the fixed-layout part of a pg_sequence
//! catalog row.  The C header has no `#ifdef CATALOG_VARLEN` section, so every
//! column is part of this in-memory struct.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::int64;
use crate::postgres_ext::Oid;

/*
 * FormData_pg_sequence - the fixed part of a pg_sequence row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_sequence {
    /* OID of pg_class entry for this sequence */
    pub seqrelid: Oid,
    /* OID of pg_type entry for the sequence's data type */
    pub seqtypid: Oid,
    /* start value of the sequence */
    pub seqstart: int64,
    /* increment value of the sequence */
    pub seqincrement: int64,
    /* maximum value of the sequence */
    pub seqmax: int64,
    /* minimum value of the sequence */
    pub seqmin: int64,
    /* number of values to cache in a session */
    pub seqcache: int64,
    /* whether the sequence cycles */
    pub seqcycle: bool,
}

/*
 * Form_pg_sequence corresponds to a pointer to a tuple with the format of the
 * pg_sequence relation.
 */
pub type Form_pg_sequence = *mut FormData_pg_sequence;

/*
 * No EXPOSE_TO_CLIENT_CODE #define constants in pg_sequence.h; the header only
 * declares the seqrelid unique index and the SEQRELID syscache.
 */

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // seqtypid sits right after the 4-byte seqrelid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_sequence, seqtypid), 4);
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_sequence>()
                >= core::mem::offset_of!(FormData_pg_sequence, seqcycle)
                    + core::mem::size_of::<bool>()
        );
    }
}
