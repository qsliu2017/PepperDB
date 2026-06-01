//! Translation of postgres/src/include/catalog/pg_statistic_ext.h
//!
//! The `FormData_pg_statistic_ext` struct: the fixed-layout part of a
//! pg_statistic_ext catalog row (definitions of extended statistics objects
//! created by CREATE STATISTICS, not the statistical data itself).  As in the C
//! header, the struct as compiled into the backend stops at the field just
//! before `#ifdef CATALOG_VARLEN`.
//!
//! Note that `stxkeys` (an int2vector) is declared just before the #ifdef and is
//! marked as a variable-length field; per the porting convention a fixed field
//! that is itself a variable-length type (int2vector) is excluded from the
//! in-memory fixed part.  The trailing CATALOG_VARLEN fields (stxstattarget,
//! stxkind char[1], stxexprs pg_node_tree) are likewise NOT part of this struct;
//! they live only in a real on-disk tuple and are reached via heap_getattr.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::NameData;
use crate::postgres_ext::Oid;

/*
 * FormData_pg_statistic_ext - the fixed part of a pg_statistic_ext row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment defined
 * here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_statistic_ext {
    /* oid */
    pub oid: Oid,
    /* relation containing attributes */
    pub stxrelid: Oid,
    /* statistics object name (part of unique key) */
    pub stxname: NameData,
    /* OID of statistics object's namespace (part of unique key) */
    pub stxnamespace: Oid,
    /* statistics object's owner */
    pub stxowner: Oid,
}

/*
 * Form_pg_statistic_ext corresponds to a pointer to a tuple with the format of
 * the pg_statistic_ext relation.
 */
pub type Form_pg_statistic_ext = *mut FormData_pg_statistic_ext;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 *
 * STATS_EXT_* - single-character codes identifying the kind of extended
 * statistics requested (stored in the stxkind column).
 * ----------------------------------------------------------------
 */

/* n-distinct coefficients */
pub const STATS_EXT_NDISTINCT: char = 'd';
/* functional dependencies */
pub const STATS_EXT_DEPENDENCIES: char = 'f';
/* most common values lists */
pub const STATS_EXT_MCV: char = 'm';
/* expression statistics */
pub const STATS_EXT_EXPRESSIONS: char = 'e';

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // stxrelid sits right after the 4-byte oid Oid (the first key field).
        assert_eq!(core::mem::offset_of!(FormData_pg_statistic_ext, stxrelid), 4);
        // The struct must at least span through its last fixed field, stxowner.
        assert!(
            core::mem::size_of::<FormData_pg_statistic_ext>()
                >= core::mem::offset_of!(FormData_pg_statistic_ext, stxowner)
                    + core::mem::size_of::<Oid>()
        );
    }
}
