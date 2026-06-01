//! Translation of postgres/src/include/catalog/pg_foreign_table.h
//!
//! The `FormData_pg_foreign_table` struct: the fixed-layout part of a
//! pg_foreign_table catalog row.  As in the C header, the struct as compiled
//! into the backend stops at the field just before `#ifdef CATALOG_VARLEN`; the
//! trailing variable-length field (ftoptions[], guarded by CATALOG_VARLEN) is
//! NOT part of this in-memory struct - it lives only in a real on-disk
//! pg_foreign_table tuple and is reached via heap_getattr.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::postgres_ext::Oid;

/*
 * FormData_pg_foreign_table - the fixed part of a pg_foreign_table row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_foreign_table {
    /* OID of foreign table */
    pub ftrelid: Oid,
    /* OID of foreign server */
    pub ftserver: Oid,
}

/*
 * Form_pg_foreign_table corresponds to a pointer to a tuple with the format of
 * the pg_foreign_table relation.
 */
pub type Form_pg_foreign_table = *mut FormData_pg_foreign_table;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 *
 * (none defined in pg_foreign_table.h)
 * ----------------------------------------------------------------
 */

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // ftserver sits right after the 4-byte oid ftrelid.
        assert_eq!(core::mem::offset_of!(FormData_pg_foreign_table, ftserver), 4);
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_foreign_table>()
                >= core::mem::offset_of!(FormData_pg_foreign_table, ftserver)
                    + core::mem::size_of::<Oid>()
        );
    }
}
