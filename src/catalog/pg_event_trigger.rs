//! Translation of postgres/src/include/catalog/pg_event_trigger.h
//!
//! The `FormData_pg_event_trigger` struct: the fixed-layout part of a
//! pg_event_trigger catalog row.  As in the C header, the struct as compiled
//! into the backend stops at the field just before `#ifdef CATALOG_VARLEN`; the
//! trailing variable-length field (evttags[], a text[] guarded by
//! CATALOG_VARLEN) is NOT part of this in-memory struct - it lives only in a
//! real on-disk pg_event_trigger tuple and is reached via heap_getattr.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::NameData;
use crate::postgres_ext::Oid;
use core::ffi::c_char;

/*
 * FormData_pg_event_trigger - the fixed part of a pg_event_trigger row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the fields in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_event_trigger {
    /* oid */
    pub oid: Oid,
    /* trigger's name */
    pub evtname: NameData,
    /* trigger's event */
    pub evtevent: NameData,
    /* trigger's owner */
    pub evtowner: Oid,
    /* OID of function to be called */
    pub evtfoid: Oid,
    /* trigger's firing configuration WRT session_replication_role */
    pub evtenabled: c_char,
}

/*
 * Form_pg_event_trigger corresponds to a pointer to a tuple with the format of
 * the pg_event_trigger relation.
 */
pub type Form_pg_event_trigger = *mut FormData_pg_event_trigger;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // evtname sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_event_trigger, evtname), 4);
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_event_trigger>()
                >= core::mem::offset_of!(FormData_pg_event_trigger, evtenabled)
                    + core::mem::size_of::<c_char>()
        );
    }
}
