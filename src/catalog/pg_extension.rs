//! Translation of postgres/src/include/catalog/pg_extension.h
//!
//! The `FormData_pg_extension` struct: the fixed-layout part of a pg_extension
//! catalog row.  As in the C header, the struct as compiled into the backend
//! stops at the field just before `#ifdef CATALOG_VARLEN`; the trailing
//! variable-length / nullable fields (extversion text, extconfig[] Oid array,
//! extcondition[] text array, guarded by CATALOG_VARLEN) are NOT part of this
//! in-memory struct - they live only in a real on-disk pg_extension tuple and
//! are reached via heap_getattr.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::NameData;
use crate::postgres_ext::Oid;

/*
 * FormData_pg_extension - the fixed part of a pg_extension row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * catalogs used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the fields in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_extension {
    /* oid */
    pub oid: Oid,
    /* extension name */
    pub extname: NameData,
    /* extension owner */
    pub extowner: Oid,
    /* namespace of contained objects */
    pub extnamespace: Oid,
    /* if true, allow ALTER EXTENSION SET SCHEMA */
    pub extrelocatable: bool,
}

/*
 * Form_pg_extension corresponds to a pointer to a tuple with the format of the
 * pg_extension relation.
 */
pub type Form_pg_extension = *mut FormData_pg_extension;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // extname sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_extension, extname), 4);
        // extowner follows the NAMEDATALEN-byte extname (offset 4 + 64 = 68).
        assert_eq!(
            core::mem::offset_of!(FormData_pg_extension, extowner),
            4 + core::mem::size_of::<NameData>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_extension>()
                >= core::mem::offset_of!(FormData_pg_extension, extrelocatable)
                    + core::mem::size_of::<bool>()
        );
    }
}
