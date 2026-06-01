//! Translation of postgres/src/include/catalog/pg_replication_origin.h
//!
//! The `FormData_pg_replication_origin` struct: the fixed-layout part of a
//! pg_replication_origin catalog row.  As in the C header, the struct as
//! compiled into the backend stops just before the variable-length fields; the
//! trailing `roname` (a text column, the first of the variable-length fields,
//! though the header allows direct access to it) is NOT part of this in-memory
//! struct - it lives only in a real on-disk pg_replication_origin tuple and is
//! reached via heap_getattr.  The header's `#ifdef CATALOG_VARLEN` block is
//! empty.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::postgres_ext::Oid;

/*
 * FormData_pg_replication_origin - the fixed part of a pg_replication_origin
 * row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_replication_origin {
    /*
     * Locally known id that get included into WAL.
     *
     * This should never leave the system.
     *
     * Needs to fit into an uint16, so we don't waste too much space in WAL
     * records. For this reason we don't use a normal Oid column here, since we
     * need to handle allocation of new values manually.
     */
    pub roident: Oid,
}

/*
 * Form_pg_replication_origin corresponds to a pointer to a tuple with the
 * format of the pg_replication_origin relation.
 */
pub type Form_pg_replication_origin = *mut FormData_pg_replication_origin;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 *
 * (The pg_replication_origin header defines no such constants.)
 * ----------------------------------------------------------------
 */

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // roident is the sole fixed field and sits at the start of the struct.
        assert_eq!(
            core::mem::offset_of!(FormData_pg_replication_origin, roident),
            0
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_replication_origin>()
                >= core::mem::offset_of!(FormData_pg_replication_origin, roident)
                    + core::mem::size_of::<Oid>()
        );
    }
}
