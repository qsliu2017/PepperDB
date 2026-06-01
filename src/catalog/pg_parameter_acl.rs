//! Translation of postgres/src/include/catalog/pg_parameter_acl.h
//!
//! The `FormData_pg_parameter_acl` struct: the fixed-layout part of a
//! pg_parameter_acl ("configuration parameter ACL") catalog row.  As in the C
//! header, the struct as compiled into the backend stops at the field just
//! before `#ifdef CATALOG_VARLEN`; the trailing variable-length fields (parname
//! text, paracl aclitem[], guarded by CATALOG_VARLEN) are NOT part of this
//! in-memory struct - they live only in a real on-disk pg_parameter_acl tuple
//! and are reached via heap_getattr.  Thus the fixed part is just the `oid`
//! column.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::postgres_ext::Oid;

/*
 * FormData_pg_parameter_acl - the fixed part of a pg_parameter_acl row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_parameter_acl {
    /* oid */
    pub oid: Oid,
}

/*
 * Form_pg_parameter_acl corresponds to a pointer to a tuple with the format of
 * the pg_parameter_acl relation.
 */
pub type Form_pg_parameter_acl = *mut FormData_pg_parameter_acl;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // The sole fixed field, oid, sits at offset 0.
        assert_eq!(core::mem::offset_of!(FormData_pg_parameter_acl, oid), 0);
        // The struct must at least span through its last fixed field, oid.
        assert!(
            core::mem::size_of::<FormData_pg_parameter_acl>()
                >= core::mem::offset_of!(FormData_pg_parameter_acl, oid)
                    + core::mem::size_of::<Oid>()
        );
    }
}
