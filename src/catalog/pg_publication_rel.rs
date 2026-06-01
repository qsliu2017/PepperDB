//! Translation of postgres/src/include/catalog/pg_publication_rel.h
//!
//! The `FormData_pg_publication_rel` struct: the fixed-layout part of a
//! pg_publication_rel catalog row (mappings between relations and publications).
//! As in the C header, the struct as compiled into the backend stops at the
//! field just before `#ifdef CATALOG_VARLEN`; the trailing variable-length
//! fields (prqual pg_node_tree, prattrs int2vector, guarded by CATALOG_VARLEN)
//! are NOT part of this in-memory struct - they live only in a real on-disk
//! pg_publication_rel tuple and are reached via heap_getattr.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::postgres_ext::Oid;

/*
 * FormData_pg_publication_rel - the fixed part of a pg_publication_rel row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_publication_rel {
    /* oid */
    pub oid: Oid,
    /* Oid of the publication */
    pub prpubid: Oid,
    /* Oid of the relation */
    pub prrelid: Oid,
}

/*
 * Form_pg_publication_rel corresponds to a pointer to a tuple with the format
 * of the pg_publication_rel relation.
 */
pub type Form_pg_publication_rel = *mut FormData_pg_publication_rel;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // prpubid sits right after the 4-byte oid Oid (the first key field).
        assert_eq!(core::mem::offset_of!(FormData_pg_publication_rel, prpubid), 4);
        // The struct must at least span through its last fixed field, prrelid.
        assert!(
            core::mem::size_of::<FormData_pg_publication_rel>()
                >= core::mem::offset_of!(FormData_pg_publication_rel, prrelid)
                    + core::mem::size_of::<Oid>()
        );
    }
}
