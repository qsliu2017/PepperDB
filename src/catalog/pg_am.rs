//! Translation of postgres/src/include/catalog/pg_am.h
//!
//! The `FormData_pg_am` struct: the fixed-layout, guaranteed-not-null part of a
//! pg_am ("access method") catalog row.  This catalog has NO `#ifdef
//! CATALOG_VARLEN` section, so every column declared in the CATALOG(...) body is
//! part of this in-memory struct (oid, amname, amhandler, amtype).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::NameData;
use crate::postgres_ext::Oid;
use core::ffi::c_char;

/* regproc is a C typedef for Oid (a registered-procedure OID). */
pub type regproc = Oid;

/*
 * FormData_pg_am - the full fixed part of a pg_am row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the fields in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_am {
    /* oid */
    pub oid: Oid,
    /* access method name */
    pub amname: NameData,
    /* handler function (BKI_LOOKUP(pg_proc)) */
    pub amhandler: regproc,
    /* see AMTYPE_xxx constants below */
    pub amtype: c_char,
}

/*
 * Form_pg_am corresponds to a pointer to a tuple with the format of the pg_am
 * relation.
 */
pub type Form_pg_am = *mut FormData_pg_am;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 *
 * Allowed values for the amtype column.
 * ----------------------------------------------------------------
 */

pub const AMTYPE_INDEX: c_char = b'i' as c_char; /* index access method */
pub const AMTYPE_TABLE: c_char = b't' as c_char; /* table access method */

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // amname sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_am, amname), 4);
        // amhandler follows the NAMEDATALEN-byte amname (offset 4 + 64 = 68).
        assert_eq!(
            core::mem::offset_of!(FormData_pg_am, amhandler),
            4 + core::mem::size_of::<NameData>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_am>()
                >= core::mem::offset_of!(FormData_pg_am, amtype)
                    + core::mem::size_of::<c_char>()
        );
    }
}
