//! Translation of postgres/src/include/catalog/pg_largeobject_metadata.h
//!
//! The `FormData_pg_largeobject_metadata` struct: the fixed-layout part of a
//! pg_largeobject_metadata catalog row.  As in the C header, the struct as
//! compiled into the backend stops at the field just before
//! `#ifdef CATALOG_VARLEN`; the trailing variable-length field (lomacl[],
//! guarded by CATALOG_VARLEN) is NOT part of this in-memory struct - it lives
//! only in a real on-disk pg_largeobject_metadata tuple and is reached via
//! heap_getattr.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::postgres_ext::Oid;

/*
 * FormData_pg_largeobject_metadata - the fixed part of a
 * pg_largeobject_metadata row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_largeobject_metadata {
    /* oid */
    pub oid: Oid,
    /* OID of the largeobject owner */
    pub lomowner: Oid,
}

/*
 * Form_pg_largeobject_metadata corresponds to a pointer to a tuple with the
 * format of the pg_largeobject_metadata relation.
 */
pub type Form_pg_largeobject_metadata = *mut FormData_pg_largeobject_metadata;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 *
 * (none defined in this header)
 * ----------------------------------------------------------------
 */

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // lomowner sits right after the 4-byte oid Oid.
        assert_eq!(
            core::mem::offset_of!(FormData_pg_largeobject_metadata, lomowner),
            4
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_largeobject_metadata>()
                >= core::mem::offset_of!(FormData_pg_largeobject_metadata, lomowner)
                    + core::mem::size_of::<Oid>()
        );
    }
}
