//! Translation of postgres/src/include/catalog/pg_shdescription.h
//!
//! The `FormData_pg_shdescription` struct: the fixed-layout part of a
//! pg_shdescription ("shared description") catalog row.  The trailing
//! variable-length `description` (text) field is guarded by CATALOG_VARLEN in
//! the C header and is NOT part of this struct; it lives only in a real
//! on-disk tuple and is reached via heap_getattr.
//!
//! An object is identified by the OID of the row that primarily defines the
//! object (objoid), plus the OID of the table that that row appears in
//! (classoid).  This allows unique identification of objects without assuming
//! OIDs are unique across tables.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::postgres_ext::Oid;

/*
 * FormData_pg_shdescription - the fixed part of a pg_shdescription row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_shdescription {
    /* OID of object itself */
    pub objoid: Oid,
    /* OID of table containing object */
    pub classoid: Oid,
}

/*
 * Form_pg_shdescription corresponds to a pointer to a row with the format of
 * the pg_shdescription relation.
 */
pub type Form_pg_shdescription = *mut FormData_pg_shdescription;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // classoid sits right after the 4-byte objoid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_shdescription, classoid), 4);
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_shdescription>()
                >= core::mem::offset_of!(FormData_pg_shdescription, classoid)
                    + core::mem::size_of::<Oid>()
        );
    }
}
