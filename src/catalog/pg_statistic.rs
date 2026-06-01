//! Translation of postgres/src/include/catalog/pg_statistic.h
//!
//! The `FormData_pg_statistic` struct: the fixed-layout part of a pg_statistic
//! catalog row.  As in the C header, the struct as compiled into the backend
//! stops at the field just before `#ifdef CATALOG_VARLEN`; the trailing
//! variable-length fields (stanumbers1..5 float4[], stavalues1..5 anyarray,
//! guarded by CATALOG_VARLEN) are NOT part of this in-memory struct - they live
//! only in a real on-disk pg_statistic tuple and are reached via heap_getattr.
//!
//! Note the unique key is (starelid, staattnum, stainherit); there is no leading
//! `oid` column.  Each statistical "slot" is split across five parallel arrays
//! flattened into individually numbered fixed fields here (stakind1..5,
//! staop1..5, stacoll1..5) to match the C struct layout exactly.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::{float4, int16, int32};
use crate::postgres_ext::Oid;

/*
 * FormData_pg_statistic - the fixed part of a pg_statistic row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_statistic {
    /* relation containing attribute (part of unique key) */
    pub starelid: Oid,
    /* attribute (column) stats are for (part of unique key) */
    pub staattnum: int16,
    /* true if inheritance children are included (part of unique key) */
    pub stainherit: bool,
    /* the fraction of the column's entries that are NULL */
    pub stanullfrac: float4,
    /* average width in bytes of non-null entries (post-TOASTing) */
    pub stawidth: int32,
    /* approximate number of distinct non-null values (see header notes) */
    pub stadistinct: float4,
    /* slot 1: kind code identifying kind of data */
    pub stakind1: int16,
    /* slot 2: kind code identifying kind of data */
    pub stakind2: int16,
    /* slot 3: kind code identifying kind of data */
    pub stakind3: int16,
    /* slot 4: kind code identifying kind of data */
    pub stakind4: int16,
    /* slot 5: kind code identifying kind of data */
    pub stakind5: int16,
    /* slot 1: OID of associated operator, if needed */
    pub staop1: Oid,
    /* slot 2: OID of associated operator, if needed */
    pub staop2: Oid,
    /* slot 3: OID of associated operator, if needed */
    pub staop3: Oid,
    /* slot 4: OID of associated operator, if needed */
    pub staop4: Oid,
    /* slot 5: OID of associated operator, if needed */
    pub staop5: Oid,
    /* slot 1: OID of relevant collation, or 0 if none */
    pub stacoll1: Oid,
    /* slot 2: OID of relevant collation, or 0 if none */
    pub stacoll2: Oid,
    /* slot 3: OID of relevant collation, or 0 if none */
    pub stacoll3: Oid,
    /* slot 4: OID of relevant collation, or 0 if none */
    pub stacoll4: Oid,
    /* slot 5: OID of relevant collation, or 0 if none */
    pub stacoll5: Oid,
}

/*
 * Form_pg_statistic corresponds to a pointer to a tuple with the format of the
 * pg_statistic relation.
 */
pub type Form_pg_statistic = *mut FormData_pg_statistic;

/* number of statistical slots in a pg_statistic row */
pub const STATISTIC_NUM_SLOTS: usize = 5;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 *
 * STATISTIC_KIND_* - integer codes identifying the kind of data stored in a
 * statistical slot (the stakindN columns).  See the header for the meaning of
 * each kind and the allocation policy for kind codes.
 * ----------------------------------------------------------------
 */

/* a "most common values" slot */
pub const STATISTIC_KIND_MCV: int16 = 1;
/* a "histogram" slot describing the distribution of scalar data */
pub const STATISTIC_KIND_HISTOGRAM: int16 = 2;
/* a "correlation" slot (physical vs. logical ordering of values) */
pub const STATISTIC_KIND_CORRELATION: int16 = 3;
/* a "most common elements" slot (for array-like types) */
pub const STATISTIC_KIND_MCELEM: int16 = 4;
/* a "distinct elements count histogram" slot (for array-type columns) */
pub const STATISTIC_KIND_DECHIST: int16 = 5;
/* a "length histogram" slot (for range-type columns) */
pub const STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM: int16 = 6;
/* a "bounds histogram" slot (for range-type columns) */
pub const STATISTIC_KIND_BOUNDS_HISTOGRAM: int16 = 7;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // staattnum sits right after the 4-byte starelid Oid (the first key field).
        assert_eq!(core::mem::offset_of!(FormData_pg_statistic, staattnum), 4);
        // The struct must at least span through its last fixed field, stacoll5.
        assert!(
            core::mem::size_of::<FormData_pg_statistic>()
                >= core::mem::offset_of!(FormData_pg_statistic, stacoll5)
                    + core::mem::size_of::<Oid>()
        );
    }
}
