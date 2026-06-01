//! Translation of postgres/src/include/catalog/pg_description.h
//!
//! The `FormData_pg_description` struct: the fixed-layout part of a
//! pg_description catalog row.  As in the C header, the struct as compiled into
//! the backend stops at the field just before `#ifdef CATALOG_VARLEN`; the
//! trailing variable-length field (`description` text, guarded by
//! CATALOG_VARLEN) is NOT part of this in-memory struct - it lives only in a
//! real on-disk pg_description tuple and is reached via heap_getattr.
//!
//! An object is identified by the OID of the row that primarily defines it
//! (objoid) plus the OID of the table that row appears in (classoid); for
//! attribute comments an objsubid giving the column number is also used.  There
//! is no leading `oid` column.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::int32;
use crate::postgres_ext::Oid;

/*
 * FormData_pg_description - the fixed part of a pg_description row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_description {
    /* OID of object itself (part of unique key) */
    pub objoid: Oid,
    /* OID of table containing object (part of unique key) */
    pub classoid: Oid,
    /* column number, or 0 if not used (part of unique key) */
    pub objsubid: int32,
}

/*
 * Form_pg_description corresponds to a pointer to a tuple with the format of
 * the pg_description relation.
 */
pub type Form_pg_description = *mut FormData_pg_description;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // classoid sits right after the 4-byte objoid Oid (the first key field).
        assert_eq!(core::mem::offset_of!(FormData_pg_description, classoid), 4);
        // The struct must at least span through its last fixed field, objsubid.
        assert!(
            core::mem::size_of::<FormData_pg_description>()
                >= core::mem::offset_of!(FormData_pg_description, objsubid)
                    + core::mem::size_of::<int32>()
        );
    }
}
