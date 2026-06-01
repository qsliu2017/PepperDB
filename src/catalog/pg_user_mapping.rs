//! Translation of postgres/src/include/catalog/pg_user_mapping.h
//!
//! The `FormData_pg_user_mapping` struct: the fixed-layout part of a
//! pg_user_mapping catalog row.  As in the C header, the struct as compiled
//! into the backend stops at the field just before `#ifdef CATALOG_VARLEN`; the
//! trailing variable-length field (umoptions[], guarded by CATALOG_VARLEN) is
//! NOT part of this in-memory struct - it lives only in a real on-disk
//! pg_user_mapping tuple and is reached via heap_getattr.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::postgres_ext::Oid;

/*
 * FormData_pg_user_mapping - the fixed part of a pg_user_mapping row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_user_mapping {
    /* oid */
    pub oid: Oid,
    /* Id of the user, InvalidOid if PUBLIC is wanted */
    pub umuser: Oid,
    /* server of this mapping */
    pub umserver: Oid,
}

/*
 * Form_pg_user_mapping corresponds to a pointer to a tuple with the format of
 * the pg_user_mapping relation.
 */
pub type Form_pg_user_mapping = *mut FormData_pg_user_mapping;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 *
 * (pg_user_mapping.h defines no EXPOSE_TO_CLIENT_CODE constants.)
 * ----------------------------------------------------------------
 */

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // umuser sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_user_mapping, umuser), 4);
        // umserver follows the 4-byte umuser Oid (offset 4 + 4 = 8).
        assert_eq!(
            core::mem::offset_of!(FormData_pg_user_mapping, umserver),
            4 + core::mem::size_of::<Oid>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_user_mapping>()
                >= core::mem::offset_of!(FormData_pg_user_mapping, umserver)
                    + core::mem::size_of::<Oid>()
        );
    }
}
