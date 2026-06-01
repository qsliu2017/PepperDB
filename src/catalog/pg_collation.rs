//! Translation of postgres/src/include/catalog/pg_collation.h
//!
//! The `FormData_pg_collation` struct: the fixed-layout, guaranteed-not-null
//! part of a pg_collation catalog row.  This is exactly the portion of the row
//! that the C struct exposes in memory; the variable-length / nullable trailing
//! fields (collcollate, collctype, colllocale, collicurules, collversion, all
//! `text` and guarded by CATALOG_VARLEN in the C header) are NOT part of this
//! struct - they live only in a real on-disk pg_collation tuple and are reached
//! via heap_getattr.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::{int32, NameData};
use crate::postgres_ext::Oid;
use core::ffi::c_char;

/*
 * FormData_pg_collation - the fixed part of a pg_collation row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_collation {
    /* oid */
    pub oid: Oid,
    /* collation name */
    pub collname: NameData,
    /* OID of namespace containing this collation */
    pub collnamespace: Oid,
    /* owner of collation */
    pub collowner: Oid,
    /* see COLLPROVIDER_* constants below */
    pub collprovider: c_char,
    /* if true, collation is deterministic */
    pub collisdeterministic: bool,
    /* encoding for this collation; -1 = "all" */
    pub collencoding: int32,
}

/*
 * Form_pg_collation corresponds to a pointer to a row with the format of the
 * pg_collation relation.
 */
pub type Form_pg_collation = *mut FormData_pg_collation;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 *
 * COLLPROVIDER_* - values of the collprovider column.
 * ----------------------------------------------------------------
 */
pub const COLLPROVIDER_DEFAULT: c_char = b'd' as c_char;
pub const COLLPROVIDER_BUILTIN: c_char = b'b' as c_char;
pub const COLLPROVIDER_ICU: c_char = b'i' as c_char;
pub const COLLPROVIDER_LIBC: c_char = b'c' as c_char;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // collname sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_collation, collname), 4);
        // collnamespace follows the NAMEDATALEN-byte collname (offset 4 + 64 = 68).
        assert_eq!(
            core::mem::offset_of!(FormData_pg_collation, collnamespace),
            4 + core::mem::size_of::<NameData>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_collation>()
                >= core::mem::offset_of!(FormData_pg_collation, collencoding)
                    + core::mem::size_of::<int32>()
        );
    }
}
