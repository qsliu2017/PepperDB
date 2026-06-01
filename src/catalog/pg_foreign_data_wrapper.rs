//! Translation of postgres/src/include/catalog/pg_foreign_data_wrapper.h
//!
//! The `FormData_pg_foreign_data_wrapper` struct: the fixed-layout part of a
//! pg_foreign_data_wrapper catalog row.  As in the C header, the struct as
//! compiled into the backend stops at the field just before
//! `#ifdef CATALOG_VARLEN`; the trailing variable-length fields (fdwacl[],
//! fdwoptions[], guarded by CATALOG_VARLEN) are NOT part of this in-memory
//! struct - they live only in a real on-disk pg_foreign_data_wrapper tuple and
//! are reached via heap_getattr.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::NameData;
use crate::postgres_ext::Oid;

/*
 * FormData_pg_foreign_data_wrapper - the fixed part of a
 * pg_foreign_data_wrapper row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the fields in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_foreign_data_wrapper {
    /* oid */
    pub oid: Oid,
    /* foreign-data wrapper name */
    pub fdwname: NameData,
    /* FDW owner */
    pub fdwowner: Oid,
    /* handler function, or 0 if none */
    pub fdwhandler: Oid,
    /* option validation function, or 0 if none */
    pub fdwvalidator: Oid,
}

/*
 * Form_pg_foreign_data_wrapper corresponds to a pointer to a tuple with the
 * format of the pg_foreign_data_wrapper relation.
 */
pub type Form_pg_foreign_data_wrapper = *mut FormData_pg_foreign_data_wrapper;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // fdwname sits right after the 4-byte oid Oid.
        assert_eq!(
            core::mem::offset_of!(FormData_pg_foreign_data_wrapper, fdwname),
            4
        );
        // fdwowner follows the NAMEDATALEN-byte fdwname (offset 4 + 64 = 68).
        assert_eq!(
            core::mem::offset_of!(FormData_pg_foreign_data_wrapper, fdwowner),
            4 + core::mem::size_of::<NameData>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_foreign_data_wrapper>()
                >= core::mem::offset_of!(FormData_pg_foreign_data_wrapper, fdwvalidator)
                    + core::mem::size_of::<Oid>()
        );
    }
}
