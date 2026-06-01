//! Translation of postgres/src/include/catalog/pg_policy.h
//!
//! The `FormData_pg_policy` struct: the fixed-layout part of a pg_policy catalog
//! row.  As in the C header, the struct as compiled into the backend stops at
//! the field just before `#ifdef CATALOG_VARLEN`; the trailing variable-length
//! fields (polroles[], polqual, polwithcheck, guarded by CATALOG_VARLEN) are
//! NOT part of this in-memory struct - they live only in a real on-disk
//! pg_policy tuple and are reached via heap_getattr.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::NameData;
use crate::postgres_ext::Oid;
use core::ffi::c_char;

/*
 * FormData_pg_policy - the fixed part of a pg_policy row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * catalogs used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the fields in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_policy {
    /* oid */
    pub oid: Oid,
    /* Policy name. */
    pub polname: NameData,
    /* Oid of the relation with policy. */
    pub polrelid: Oid,
    /* One of ACL_*_CHR, or '*' for all */
    pub polcmd: c_char,
    /* restrictive or permissive policy */
    pub polpermissive: bool,
}

/*
 * Form_pg_policy corresponds to a pointer to a row with the format of the
 * pg_policy relation.
 */
pub type Form_pg_policy = *mut FormData_pg_policy;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // polname sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_policy, polname), 4);
        // polrelid follows the NAMEDATALEN-byte polname (offset 4 + 64 = 68).
        assert_eq!(
            core::mem::offset_of!(FormData_pg_policy, polrelid),
            4 + core::mem::size_of::<NameData>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_policy>()
                >= core::mem::offset_of!(FormData_pg_policy, polpermissive)
                    + core::mem::size_of::<bool>()
        );
    }
}
