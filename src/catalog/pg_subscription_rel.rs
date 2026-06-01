//! Translation of postgres/src/include/catalog/pg_subscription_rel.h
//!
//! The `FormData_pg_subscription_rel` struct: the fixed-layout part of a
//! pg_subscription_rel catalog row, recording the state of each replicated table
//! in each subscription.  As in the C header, the struct stops at the field just
//! before `#ifdef CATALOG_VARLEN`; the trailing `srsublsn` field is excluded.
//! Although srsublsn is a fixed-width type (XLogRecPtr), it is allowed to be
//! NULL, so the C header guards it under CATALOG_VARLEN to prevent direct C code
//! access just as for a varlena field - it is reached via heap_getattr instead.
//!
//! Note the unique key is (srrelid, srsubid); there is no leading `oid` column.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::postgres_ext::Oid;
use core::ffi::c_char;

/*
 * FormData_pg_subscription_rel - the fixed part of a pg_subscription_rel row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_subscription_rel {
    /* Oid of subscription (part of unique key) */
    pub srsubid: Oid,
    /* Oid of relation (part of unique key) */
    pub srrelid: Oid,
    /* state of the relation in subscription */
    pub srsubstate: c_char,
}

/*
 * Form_pg_subscription_rel corresponds to a pointer to a row with the format of
 * the pg_subscription_rel relation.
 */
pub type Form_pg_subscription_rel = *mut FormData_pg_subscription_rel;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 *
 * SUBREL_STATE_* - substate constants for the srsubstate column.
 * ----------------------------------------------------------------
 */

/* initializing (sublsn NULL) */
pub const SUBREL_STATE_INIT: c_char = b'i' as c_char;
/* data is being synchronized (sublsn NULL) */
pub const SUBREL_STATE_DATASYNC: c_char = b'd' as c_char;
/* tablesync copy phase is completed (sublsn NULL) */
pub const SUBREL_STATE_FINISHEDCOPY: c_char = b'f' as c_char;
/* synchronization finished in front of apply (sublsn set) */
pub const SUBREL_STATE_SYNCDONE: c_char = b's' as c_char;
/* ready (sublsn set) */
pub const SUBREL_STATE_READY: c_char = b'r' as c_char;

/* These are never stored in the catalog, we only use them for IPC. */
/* unknown state */
pub const SUBREL_STATE_UNKNOWN: c_char = b'\0' as c_char;
/* waiting for sync */
pub const SUBREL_STATE_SYNCWAIT: c_char = b'w' as c_char;
/* catching up with apply */
pub const SUBREL_STATE_CATCHUP: c_char = b'c' as c_char;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // srrelid sits right after the 4-byte srsubid Oid (the first key field).
        assert_eq!(core::mem::offset_of!(FormData_pg_subscription_rel, srrelid), 4);
        // The struct must at least span through its last fixed field, srsubstate.
        assert!(
            core::mem::size_of::<FormData_pg_subscription_rel>()
                >= core::mem::offset_of!(FormData_pg_subscription_rel, srsubstate)
                    + core::mem::size_of::<c_char>()
        );
    }
}
