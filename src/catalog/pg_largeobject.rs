//! Translation of postgres/src/include/catalog/pg_largeobject.h
//!
//! The `FormData_pg_largeobject` struct: the fixed-layout part of a
//! pg_largeobject catalog row.  The C header has no `#ifdef CATALOG_VARLEN`,
//! but the trailing `data bytea` column is a variable-length (varlena) type;
//! like the other CATALOG_VARLEN-style trailing fields it is NOT part of this
//! in-memory fixed struct.  It lives only in a real on-disk pg_largeobject
//! tuple and is reached via direct access (see inv_api.c) / heap_getattr.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::int32;
use crate::postgres_ext::Oid;

/*
 * FormData_pg_largeobject - the fixed part of a pg_largeobject row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_largeobject {
    /* Identifier of large object */
    pub loid: Oid,
    /* Page number (starting from 0) */
    pub pageno: int32,
}

/*
 * Form_pg_largeobject corresponds to a pointer to a tuple with the format of
 * the pg_largeobject relation.
 */
pub type Form_pg_largeobject = *mut FormData_pg_largeobject;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 *
 * pg_largeobject.h exposes no EXPOSE_TO_CLIENT_CODE #define constants.
 * ----------------------------------------------------------------
 */

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // pageno sits right after the 4-byte loid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_largeobject, pageno), 4);
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_largeobject>()
                >= core::mem::offset_of!(FormData_pg_largeobject, pageno)
                    + core::mem::size_of::<int32>()
        );
    }
}
