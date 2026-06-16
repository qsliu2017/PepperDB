//! Translation of postgres/src/include/catalog/pg_trigger.h
//!
//! The `FormData_pg_trigger` struct: the fixed-layout part of a pg_trigger
//! catalog row.  As in the C header, the struct as compiled into the backend
//! stops at the field just before `#ifdef CATALOG_VARLEN`; the trailing
//! variable-length fields (tgargs (bytea), tgqual (pg_node_tree), tgoldtable,
//! tgnewtable, guarded by CATALOG_VARLEN) are NOT part of this in-memory struct
//! - they live only in a real on-disk pg_trigger tuple and are reached via
//! heap_getattr.
//!
//! Note: the C header declares `tgattr` (an int2vector) just BEFORE the
//! CATALOG_VARLEN cutoff, with a comment noting that variable-length fields
//! "start here, but we allow direct access to tgattr".  int2vector is a
//! variable-length type, so it is excluded from this fixed-part struct; the
//! last fixed field is `tgnargs`.
//!
//! Note: when tgconstraint is nonzero, tgconstrrelid, tgconstrindid,
//! tgdeferrable, and tginitdeferred are largely redundant with the referenced
//! pg_constraint entry.  However, it is possible for a non-deferrable trigger
//! to be associated with a deferrable constraint.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::{int16, NameData};
use crate::postgres_ext::Oid;
use core::ffi::c_char;

/*
 * FormData_pg_trigger - the fixed part of a pg_trigger row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_trigger {
    /* oid */
    pub oid: Oid,
    /* relation trigger is attached to */
    pub tgrelid: Oid,
    /* OID of parent trigger, if any */
    pub tgparentid: Oid,
    /* trigger's name */
    pub tgname: NameData,
    /* OID of function to be called */
    pub tgfoid: Oid,
    /* BEFORE/AFTER/INSTEAD, UPDATE/DELETE/INSERT, ROW/STATEMENT; see below */
    pub tgtype: int16,
    /* trigger's firing configuration WRT session_replication_role */
    pub tgenabled: c_char,
    /* trigger is system-generated */
    pub tgisinternal: bool,
    /* constraint's FROM table, if any */
    pub tgconstrrelid: Oid,
    /* constraint's supporting index, if any */
    pub tgconstrindid: Oid,
    /* associated pg_constraint entry, if any */
    pub tgconstraint: Oid,
    /* constraint trigger is deferrable */
    pub tgdeferrable: bool,
    /* constraint trigger is deferred initially */
    pub tginitdeferred: bool,
    /* # of extra arguments in tgargs */
    pub tgnargs: int16,
}

/*
 * Form_pg_trigger corresponds to a pointer to a tuple with the format of the
 * pg_trigger relation.
 */
pub type Form_pg_trigger = *mut FormData_pg_trigger;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 * ----------------------------------------------------------------
 */

/* Bits within tgtype */
pub const TRIGGER_TYPE_ROW: int16 = 1 << 0;
pub const TRIGGER_TYPE_BEFORE: int16 = 1 << 1;
pub const TRIGGER_TYPE_INSERT: int16 = 1 << 2;
pub const TRIGGER_TYPE_DELETE: int16 = 1 << 3;
pub const TRIGGER_TYPE_UPDATE: int16 = 1 << 4;
pub const TRIGGER_TYPE_TRUNCATE: int16 = 1 << 5;
pub const TRIGGER_TYPE_INSTEAD: int16 = 1 << 6;

pub const TRIGGER_TYPE_LEVEL_MASK: int16 = TRIGGER_TYPE_ROW;
pub const TRIGGER_TYPE_STATEMENT: int16 = 0;

/* Note bits within TRIGGER_TYPE_TIMING_MASK aren't adjacent */
pub const TRIGGER_TYPE_TIMING_MASK: int16 = TRIGGER_TYPE_BEFORE | TRIGGER_TYPE_INSTEAD;
pub const TRIGGER_TYPE_AFTER: int16 = 0;

pub const TRIGGER_TYPE_EVENT_MASK: int16 =
    TRIGGER_TYPE_INSERT | TRIGGER_TYPE_DELETE | TRIGGER_TYPE_UPDATE | TRIGGER_TYPE_TRUNCATE;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // tgrelid (the first key field) sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_trigger, tgrelid), 4);
        // The struct must at least span through its last fixed field, tgnargs.
        assert!(
            core::mem::size_of::<FormData_pg_trigger>()
                >= core::mem::offset_of!(FormData_pg_trigger, tgnargs)
                    + core::mem::size_of::<int16>()
        );
    }
}

/* pg_trigger attribute numbers (1-based, matching the CATALOG(pg_trigger) column order). */
pub const Anum_pg_trigger_oid: i16 = 1;
pub const Anum_pg_trigger_tgrelid: i16 = 2;
pub const Anum_pg_trigger_tgparentid: i16 = 3;
pub const Anum_pg_trigger_tgname: i16 = 4;
pub const Anum_pg_trigger_tgfoid: i16 = 5;
pub const Anum_pg_trigger_tgtype: i16 = 6;
pub const Anum_pg_trigger_tgenabled: i16 = 7;
pub const Anum_pg_trigger_tgisinternal: i16 = 8;
pub const Anum_pg_trigger_tgconstrrelid: i16 = 9;
pub const Anum_pg_trigger_tgconstrindid: i16 = 10;
pub const Anum_pg_trigger_tgconstraint: i16 = 11;
pub const Anum_pg_trigger_tgdeferrable: i16 = 12;
pub const Anum_pg_trigger_tginitdeferred: i16 = 13;
pub const Anum_pg_trigger_tgnargs: i16 = 14;
pub const Anum_pg_trigger_tgattr: i16 = 15;
pub const Anum_pg_trigger_tgargs: i16 = 16;
pub const Anum_pg_trigger_tgqual: i16 = 17;
pub const Anum_pg_trigger_tgoldtable: i16 = 18;
pub const Anum_pg_trigger_tgnewtable: i16 = 19;
pub const Natts_pg_trigger: usize = 19;
