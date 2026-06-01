//! Translation of postgres/src/include/catalog/pg_statistic_ext_data.h
//!
//! The `FormData_pg_statistic_ext_data` struct: the fixed-layout part of a
//! pg_statistic_ext_data catalog row.  As in the C header, the struct as
//! compiled into the backend stops at the field just before
//! `#ifdef CATALOG_VARLEN`; the trailing variable-length fields (stxdndistinct
//! pg_ndistinct, stxddependencies pg_dependencies, stxdmcv pg_mcv_list,
//! stxdexpr pg_statistic[1], guarded by CATALOG_VARLEN) are NOT part of this
//! in-memory struct - they live only in a real on-disk tuple and are reached
//! via heap_getattr.
//!
//! The unique key is (stxoid, stxdinherit); there is no leading `oid` column.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::postgres_ext::Oid;

/*
 * FormData_pg_statistic_ext_data - the fixed part of a pg_statistic_ext_data
 * row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_statistic_ext_data {
    /* statistics object this data is for (part of unique key) */
    pub stxoid: Oid,
    /* true if inheritance children are included (part of unique key) */
    pub stxdinherit: bool,
}

/*
 * Form_pg_statistic_ext_data corresponds to a pointer to a tuple with the
 * format of the pg_statistic_ext_data relation.
 */
pub type Form_pg_statistic_ext_data = *mut FormData_pg_statistic_ext_data;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // stxdinherit sits right after the 4-byte stxoid Oid (the first key field).
        assert_eq!(
            core::mem::offset_of!(FormData_pg_statistic_ext_data, stxdinherit),
            4
        );
        // The struct must at least span through its last fixed field, stxdinherit.
        assert!(
            core::mem::size_of::<FormData_pg_statistic_ext_data>()
                >= core::mem::offset_of!(FormData_pg_statistic_ext_data, stxdinherit)
                    + core::mem::size_of::<bool>()
        );
    }
}
