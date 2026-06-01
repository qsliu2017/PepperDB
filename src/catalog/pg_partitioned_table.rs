//! Translation of postgres/src/include/catalog/pg_partitioned_table.h
//!
//! The `FormData_pg_partitioned_table` struct: the fixed-layout, guaranteed-
//! not-null part of a pg_partitioned_table catalog row.  This is exactly the
//! portion of the row that the C struct exposes in memory.  The C header allows
//! direct access to `partattrs` (an int2vector) which begins the variable-
//! length region, but it - along with the trailing fields guarded by
//! CATALOG_VARLEN (partclass, partcollation, partexprs) - is NOT part of this
//! fixed struct.  Those live only in a real on-disk pg_partitioned_table tuple
//! and are reached via heap_getattr.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::int16;
use crate::postgres_ext::Oid;
use core::ffi::c_char;

/*
 * FormData_pg_partitioned_table - the fixed part of a pg_partitioned_table row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the fields in a struct
 * representing a table row.
 *
 * The fixed part ends at partdefid; the next field (partattrs) begins the
 * variable-length region (it is the int2vector preceding #ifdef CATALOG_VARLEN).
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_partitioned_table {
    /* partitioned table oid */
    pub partrelid: Oid,
    /* partitioning strategy */
    pub partstrat: c_char,
    /* number of partition key columns */
    pub partnatts: int16,
    /* default partition oid; 0 if there isn't one */
    pub partdefid: Oid,
}

/*
 * Form_pg_partitioned_table corresponds to a pointer to a tuple with the format
 * of the pg_partitioned_table relation.
 */
pub type Form_pg_partitioned_table = *mut FormData_pg_partitioned_table;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // partstrat (a 1-byte char) sits right after the 4-byte partrelid Oid.
        assert_eq!(
            core::mem::offset_of!(FormData_pg_partitioned_table, partstrat),
            4
        );
        // The struct must at least span through its last fixed field
        // (partdefid is a 4-byte Oid).
        assert!(
            core::mem::size_of::<FormData_pg_partitioned_table>()
                >= core::mem::offset_of!(FormData_pg_partitioned_table, partdefid)
                    + core::mem::size_of::<Oid>()
        );
    }
}
