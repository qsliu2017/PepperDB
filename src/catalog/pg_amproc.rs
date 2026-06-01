//! Translation of postgres/src/include/catalog/pg_amproc.h
//!
//! The `FormData_pg_amproc` struct: the fixed-layout part of a pg_amproc
//! catalog row.  pg_amproc identifies support procedures associated with index
//! operator families and classes; these procedures can't be listed in pg_amop
//! since they are not the implementation of any indexable operator.
//!
//! This header has NO `#ifdef CATALOG_VARLEN` section, so every declared column
//! is part of the fixed struct and all are included here.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::int16;
use crate::postgres_ext::Oid;

/* regproc is a C typedef for Oid (a registered-procedure OID). */
pub type regproc = Oid;

/*
 * FormData_pg_amproc - the fixed part of a pg_amproc row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_amproc {
    /* oid */
    pub oid: Oid,
    /* the index opfamily this entry is for */
    pub amprocfamily: Oid,
    /* procedure's left input data type */
    pub amproclefttype: Oid,
    /* procedure's right input data type */
    pub amprocrighttype: Oid,
    /* support procedure index */
    pub amprocnum: int16,
    /* OID of the proc */
    pub amproc: regproc,
}

/*
 * Form_pg_amproc corresponds to a pointer to a tuple with the format of the
 * pg_amproc relation.
 */
pub type Form_pg_amproc = *mut FormData_pg_amproc;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // amprocfamily sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_amproc, amprocfamily), 4);
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_amproc>()
                >= core::mem::offset_of!(FormData_pg_amproc, amproc)
                    + core::mem::size_of::<regproc>()
        );
    }
}
