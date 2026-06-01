//! Translation of postgres/src/include/catalog/pg_subscription.h
//!
//! The `FormData_pg_subscription` struct: the fixed-layout part of a
//! pg_subscription catalog row.  As in the C header, the struct as compiled
//! into the backend stops at the field just before `#ifdef CATALOG_VARLEN`; the
//! trailing variable-length fields (subconninfo, subslotname, subsynccommit,
//! subpublications[], suborigin, guarded by CATALOG_VARLEN) are NOT part of this
//! in-memory struct - they live only in a real on-disk pg_subscription tuple and
//! are reached via heap_getattr.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::{uint64, NameData};
use crate::postgres_ext::Oid;
use core::ffi::c_char;

/* XLogRecPtr is a C typedef for uint64 (a byte position in the WAL). */
pub type XLogRecPtr = uint64;

/*
 * FormData_pg_subscription - the fixed part of a pg_subscription row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_subscription {
    /* oid */
    pub oid: Oid,
    /* Database the subscription is in. */
    pub subdbid: Oid,
    /* All changes finished at this LSN are skipped */
    pub subskiplsn: XLogRecPtr,
    /* Name of the subscription */
    pub subname: NameData,
    /* Owner of the subscription */
    pub subowner: Oid,
    /* True if the subscription is enabled (the worker should be running) */
    pub subenabled: bool,
    /* True if the subscription wants the publisher to send data in binary */
    pub subbinary: bool,
    /* Stream in-progress transactions. See LOGICALREP_STREAM_xxx constants. */
    pub substream: c_char,
    /* Stream two-phase transactions */
    pub subtwophasestate: c_char,
    /* True if a worker error should cause the subscription to be disabled */
    pub subdisableonerr: bool,
    /* Must connection use a password? */
    pub subpasswordrequired: bool,
    /* True if replication should execute as the subscription owner */
    pub subrunasowner: bool,
    /* True if the associated replication slots in the upstream database are
     * enabled to be synchronized to the standbys. */
    pub subfailover: bool,
}

/*
 * Form_pg_subscription corresponds to a pointer to a row with the format of the
 * pg_subscription relation.
 */
pub type Form_pg_subscription = *mut FormData_pg_subscription;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 * ----------------------------------------------------------------
 */

/*
 * two_phase tri-state values. See comments atop worker.c to know more about
 * these states.
 */
pub const LOGICALREP_TWOPHASE_STATE_DISABLED: c_char = b'd' as c_char;
pub const LOGICALREP_TWOPHASE_STATE_PENDING: c_char = b'p' as c_char;
pub const LOGICALREP_TWOPHASE_STATE_ENABLED: c_char = b'e' as c_char;

/*
 * The subscription will request the publisher to only send changes that do not
 * have any origin.
 */
pub const LOGICALREP_ORIGIN_NONE: &str = "none";

/*
 * The subscription will request the publisher to send changes regardless
 * of their origin.
 */
pub const LOGICALREP_ORIGIN_ANY: &str = "any";

/* Disallow streaming in-progress transactions. */
pub const LOGICALREP_STREAM_OFF: c_char = b'f' as c_char;

/*
 * Streaming in-progress transactions are written to a temporary file and
 * applied only after the transaction is committed on upstream.
 */
pub const LOGICALREP_STREAM_ON: c_char = b't' as c_char;

/*
 * Streaming in-progress transactions are applied immediately via a parallel
 * apply worker.
 */
pub const LOGICALREP_STREAM_PARALLEL: c_char = b'p' as c_char;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // subdbid sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_subscription, subdbid), 4);
        // The struct must at least span through its last fixed field
        // (subfailover, a bool).
        assert!(
            core::mem::size_of::<FormData_pg_subscription>()
                >= core::mem::offset_of!(FormData_pg_subscription, subfailover)
                    + core::mem::size_of::<bool>()
        );
    }
}
