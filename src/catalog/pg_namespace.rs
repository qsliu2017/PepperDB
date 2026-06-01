//! Translation of postgres/src/include/catalog/pg_namespace.h
//!
//! The `FormData_pg_namespace` struct: the fixed-layout, guaranteed-not-null
//! part of a pg_namespace catalog row.  This is exactly the portion of the row
//! that the C struct exposes in memory; the variable-length / nullable trailing
//! field (nspacl, guarded by CATALOG_VARLEN in the C header) is NOT part of this
//! struct - it lives only in a real on-disk pg_namespace tuple and is reached
//! via heap_getattr.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::NameData;
use crate::postgres_ext::Oid;

/*
 * FormData_pg_namespace - the fixed part of a pg_namespace row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_namespace {
    /* oid */
    pub oid: Oid,
    /* name of the namespace */
    pub nspname: NameData,
    /* owner (creator) of the namespace */
    pub nspowner: Oid,
}

/*
 * Form_pg_namespace corresponds to a pointer to a tuple with the format of the
 * pg_namespace relation.
 */
pub type Form_pg_namespace = *mut FormData_pg_namespace;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // nspname sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_namespace, nspname), 4);
        // nspowner follows the NAMEDATALEN-byte nspname (offset 4 + 64 = 68).
        assert_eq!(
            core::mem::offset_of!(FormData_pg_namespace, nspowner),
            4 + core::mem::size_of::<NameData>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_namespace>()
                >= core::mem::offset_of!(FormData_pg_namespace, nspowner)
                    + core::mem::size_of::<Oid>()
        );
    }
}
