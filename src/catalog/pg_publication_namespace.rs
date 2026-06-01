//! Translation of postgres/src/include/catalog/pg_publication_namespace.h
//!
//! The `FormData_pg_publication_namespace` struct: the fixed-layout part of a
//! pg_publication_namespace catalog row, which maps schemas (namespaces) to
//! publications.  This header has no CATALOG_VARLEN section, so the struct
//! contains all of the catalog's columns.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::postgres_ext::Oid;

/*
 * FormData_pg_publication_namespace - a full pg_publication_namespace row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_publication_namespace {
    /* oid */
    pub oid: Oid,
    /* Oid of the publication */
    pub pnpubid: Oid,
    /* Oid of the schema */
    pub pnnspid: Oid,
}

/*
 * Form_pg_publication_namespace corresponds to a pointer to a row with the
 * format of the pg_publication_namespace relation.
 */
pub type Form_pg_publication_namespace = *mut FormData_pg_publication_namespace;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // pnpubid sits right after the 4-byte oid Oid.
        assert_eq!(
            core::mem::offset_of!(FormData_pg_publication_namespace, pnpubid),
            4
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_publication_namespace>()
                >= core::mem::offset_of!(FormData_pg_publication_namespace, pnnspid)
                    + core::mem::size_of::<Oid>()
        );
    }
}
