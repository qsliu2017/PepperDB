//! Translation of postgres/src/include/catalog/pg_shdepend.h
//!
//! The `FormData_pg_shdepend` struct: the fixed-layout part of a pg_shdepend
//! catalog row.  The C header has no `#ifdef CATALOG_VARLEN` cutoff, so every
//! declared column is part of this in-memory struct.
//!
//! pg_shdepend records shared (cross-database) dependencies; only dependencies
//! on roles are explicitly stored.  There is no leading `oid` column - the row
//! is identified by the depender (dbid, classid, objid, objsubid).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::int32;
use crate::postgres_ext::Oid;
use core::ffi::c_char;

/*
 * FormData_pg_shdepend - the fixed part of a pg_shdepend row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_shdepend {
    /* OID of database containing object; 0 denotes a shared object */
    pub dbid: Oid,
    /* OID of table (pg_class) containing the dependent object */
    pub classid: Oid,
    /* OID of the dependent object itself */
    pub objid: Oid,
    /* column number, or 0 if not used */
    pub objsubid: int32,
    /* OID of table (pg_class) containing the referenced object */
    pub refclassid: Oid,
    /* OID of the referenced object itself */
    pub refobjid: Oid,
    /* dependency type; see codes in dependency.h (SharedDependencyType) */
    pub deptype: c_char,
}

/*
 * Form_pg_shdepend corresponds to a pointer to a row with the format of the
 * pg_shdepend relation.
 */
pub type Form_pg_shdepend = *mut FormData_pg_shdepend;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // classid sits right after the 4-byte dbid Oid (the first key field).
        assert_eq!(core::mem::offset_of!(FormData_pg_shdepend, classid), 4);
        // The struct must at least span through its last fixed field, deptype.
        assert!(
            core::mem::size_of::<FormData_pg_shdepend>()
                >= core::mem::offset_of!(FormData_pg_shdepend, deptype)
                    + core::mem::size_of::<c_char>()
        );
    }
}
