//! Translation of postgres/src/include/catalog/pg_init_privs.h
//!
//! The `FormData_pg_init_privs` struct: the fixed-layout part of a
//! pg_init_privs catalog row.  As in the C header, the struct as compiled into
//! the backend stops at the field just before `#ifdef CATALOG_VARLEN`; the
//! trailing variable-length field (initprivs aclitem[], guarded by
//! CATALOG_VARLEN) is NOT part of this in-memory struct - it lives only in a
//! real on-disk pg_init_privs tuple and is reached via heap_getattr.
//!
//! An object is identified by the OID of the row that primarily defines the
//! object (objoid) plus the OID of the table that row appears in (classoid).
//! For attribute privileges, objsubid gives the column number; it is zero for a
//! table itself and for all other kinds of objects.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::int32;
use crate::postgres_ext::Oid;
use core::ffi::c_char;

/*
 * FormData_pg_init_privs - the fixed part of a pg_init_privs row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_init_privs {
    /* OID of object itself */
    pub objoid: Oid,
    /* OID of table containing object */
    pub classoid: Oid,
    /* column number, or 0 if not used */
    pub objsubid: int32,
    /* from initdb or extension? */
    pub privtype: c_char,
}

/*
 * Form_pg_init_privs corresponds to a pointer to a tuple with the format of the
 * pg_init_privs relation.
 */
pub type Form_pg_init_privs = *mut FormData_pg_init_privs;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 *
 * InitPrivsType - it is important to know if the initial privileges are from
 * initdb or from an extension.  This enum provides that differentiation; the
 * two places that populate this table (initdb and recordExtensionInitPriv()
 * during CREATE EXTENSION) know to use the correct values.
 * ----------------------------------------------------------------
 */

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum InitPrivsType {
    INITPRIVS_INITDB = b'i' as isize,
    INITPRIVS_EXTENSION = b'e' as isize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // classoid sits right after the 4-byte objoid Oid (the first key field).
        assert_eq!(core::mem::offset_of!(FormData_pg_init_privs, classoid), 4);
        // The struct must at least span through its last fixed field, privtype.
        assert!(
            core::mem::size_of::<FormData_pg_init_privs>()
                > core::mem::offset_of!(FormData_pg_init_privs, privtype)
        );
    }
}
