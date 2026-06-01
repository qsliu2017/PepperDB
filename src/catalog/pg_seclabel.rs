//! Translation of postgres/src/include/catalog/pg_seclabel.h
//!
//! The `FormData_pg_seclabel` struct: the fixed-layout part of a pg_seclabel
//! catalog row.  As in the C header, the struct as compiled into the backend
//! stops at the field just before `#ifdef CATALOG_VARLEN`; the trailing
//! variable-length fields (provider, label, guarded by CATALOG_VARLEN) are NOT
//! part of this in-memory struct - they live only in a real on-disk
//! pg_seclabel tuple and are reached via heap_getattr.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::int32;
use crate::postgres_ext::Oid;

/*
 * FormData_pg_seclabel - the fixed part of a pg_seclabel row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_seclabel {
    /* OID of the object itself */
    pub objoid: Oid,
    /* OID of table containing the object */
    pub classoid: Oid,
    /* column number, or 0 if not used */
    pub objsubid: int32,
}

/*
 * Form_pg_seclabel corresponds to a pointer to a row with the format of the
 * pg_seclabel relation.
 */
pub type Form_pg_seclabel = *mut FormData_pg_seclabel;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 *
 * (pg_seclabel.h exposes none.)
 * ----------------------------------------------------------------
 */

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // classoid sits right after the 4-byte objoid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_seclabel, classoid), 4);
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_seclabel>()
                >= core::mem::offset_of!(FormData_pg_seclabel, objsubid)
                    + core::mem::size_of::<int32>()
        );
    }
}
