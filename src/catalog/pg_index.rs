//! Translation of postgres/src/include/catalog/pg_index.h
//!
//! The `FormData_pg_index` struct: the fixed-layout, guaranteed-not-null part
//! of a pg_index catalog row.  This is exactly the portion of the row that the
//! C struct exposes in memory.  The C header allows direct access to `indkey`
//! (an int2vector) which begins the variable-length region, but it - along with
//! the trailing fields guarded by CATALOG_VARLEN (indcollation, indclass,
//! indoption, indexprs, indpred) - is NOT part of this fixed struct.  Those
//! live only in a real on-disk pg_index tuple and are reached via heap_getattr.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::int16;
use crate::postgres_ext::Oid;

/*
 * FormData_pg_index - the fixed part of a pg_index row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the fields in a struct
 * representing a table row.
 *
 * The fixed part ends at indisreplident; the next field (indkey) begins the
 * variable-length region (it is the int2vector preceding #ifdef CATALOG_VARLEN).
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_index {
    /* OID of the index */
    pub indexrelid: Oid,
    /* OID of the relation it indexes */
    pub indrelid: Oid,
    /* total number of columns in index */
    pub indnatts: int16,
    /* number of key columns in index */
    pub indnkeyatts: int16,
    /* is this a unique index? */
    pub indisunique: bool,
    /* null treatment in unique index */
    pub indnullsnotdistinct: bool,
    /* is this index for primary key? */
    pub indisprimary: bool,
    /* is this index for exclusion constraint? */
    pub indisexclusion: bool,
    /* is uniqueness enforced immediately? */
    pub indimmediate: bool,
    /* is this the index last clustered by? */
    pub indisclustered: bool,
    /* is this index valid for use by queries? */
    pub indisvalid: bool,
    /* must we wait for xmin to be old? */
    pub indcheckxmin: bool,
    /* is this index ready for inserts? */
    pub indisready: bool,
    /* is this index alive at all? */
    pub indislive: bool,
    /* is this index the identity for replication? */
    pub indisreplident: bool,
}

/*
 * Form_pg_index corresponds to a pointer to a tuple with the format of the
 * pg_index relation.
 */
pub type Form_pg_index = *mut FormData_pg_index;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 *
 * Index AMs that support ordered scans must support these two indoption bits.
 * Otherwise, the content of the per-column indoption fields is open for future
 * definition.
 * ----------------------------------------------------------------
 */

/* values are in reverse order */
pub const INDOPTION_DESC: int16 = 0x0001;
/* NULLs are first instead of last */
pub const INDOPTION_NULLS_FIRST: int16 = 0x0002;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // indrelid sits right after the 4-byte indexrelid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_index, indrelid), 4);
        // indnatts (int16) follows the two 4-byte Oids.
        assert_eq!(core::mem::offset_of!(FormData_pg_index, indnatts), 8);
        // The struct must at least span through its last fixed field
        // (indisreplident is a 1-byte bool).
        assert!(
            core::mem::size_of::<FormData_pg_index>()
                >= core::mem::offset_of!(FormData_pg_index, indisreplident) + 1
        );
    }
}
