//! Translation of postgres/src/include/catalog/pg_shseclabel.h
//!
//! The `FormData_pg_shseclabel` struct: the fixed-layout part of a
//! pg_shseclabel ("shared security label") catalog row.  As in the C header,
//! the struct as compiled into the backend stops at the field just before
//! `#ifdef CATALOG_VARLEN`; the trailing variable-length fields (provider and
//! label, both text, guarded by CATALOG_VARLEN) are NOT part of this in-memory
//! struct - they live only in a real on-disk pg_shseclabel tuple and are
//! reached via heap_getattr.
//!
//! Note there is no leading `oid` column; the unique key is (objoid, classoid,
//! provider).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::postgres_ext::Oid;

/*
 * FormData_pg_shseclabel - the fixed part of a pg_shseclabel row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_shseclabel {
    /* OID of the shared object itself (part of unique key) */
    pub objoid: Oid,
    /* OID of table containing the shared object (part of unique key) */
    pub classoid: Oid,
}

/*
 * Form_pg_shseclabel corresponds to a pointer to a tuple with the format of the
 * pg_shseclabel relation.
 */
pub type Form_pg_shseclabel = *mut FormData_pg_shseclabel;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // classoid sits right after the 4-byte objoid Oid (the first key field).
        assert_eq!(core::mem::offset_of!(FormData_pg_shseclabel, classoid), 4);
        // The struct must at least span through its last fixed field, classoid.
        assert!(
            core::mem::size_of::<FormData_pg_shseclabel>()
                >= core::mem::offset_of!(FormData_pg_shseclabel, classoid)
                    + core::mem::size_of::<Oid>()
        );
    }
}
