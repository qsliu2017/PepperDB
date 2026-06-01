//! Translation of postgres/src/include/catalog/pg_opfamily.h
//!
//! The `FormData_pg_opfamily` struct: the fixed-layout, guaranteed-not-null
//! part of a pg_opfamily catalog row (the "operator family" system catalog).
//! The C header has no CATALOG_VARLEN section, so every declared column is part
//! of the fixed struct.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::NameData;
use crate::postgres_ext::Oid;

/*
 * FormData_pg_opfamily - the fixed part of a pg_opfamily row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_opfamily {
    /* oid */
    pub oid: Oid,
    /* index access method opfamily is for */
    pub opfmethod: Oid,
    /* name of this opfamily */
    pub opfname: NameData,
    /* namespace of this opfamily */
    pub opfnamespace: Oid,
    /* opfamily owner */
    pub opfowner: Oid,
}

/*
 * Form_pg_opfamily corresponds to a pointer to a tuple with the format of the
 * pg_opfamily relation.
 */
pub type Form_pg_opfamily = *mut FormData_pg_opfamily;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // opfmethod sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_opfamily, opfmethod), 4);
        // opfname follows oid (4) + opfmethod Oid (4).
        assert_eq!(
            core::mem::offset_of!(FormData_pg_opfamily, opfname),
            4 + core::mem::size_of::<Oid>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_opfamily>()
                >= core::mem::offset_of!(FormData_pg_opfamily, opfowner)
                    + core::mem::size_of::<Oid>()
        );
    }
}
