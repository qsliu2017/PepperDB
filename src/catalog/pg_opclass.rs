//! Translation of postgres/src/include/catalog/pg_opclass.h
//!
//! The `FormData_pg_opclass` struct: the fixed-layout part of a pg_opclass
//! catalog row (the "operator class" system catalog).  The C header has no
//! `#ifdef CATALOG_VARLEN` section, so every declared column is part of the
//! fixed struct and is included here.
//!
//! The primary key for this table is <opcmethod, opcname, opcnamespace> --
//! one row per valid combination of opclass name and index access method type.
//! The row specifies the expected input data type for the opclass.  When
//! opckeytype is nonzero, it indicates the index stores data of that type
//! rather than the input column type.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::NameData;
use crate::postgres_ext::Oid;

/*
 * FormData_pg_opclass - the fixed part of a pg_opclass row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_opclass {
    /* oid */
    pub oid: Oid,
    /* index access method opclass is for */
    pub opcmethod: Oid,
    /* name of this opclass */
    pub opcname: NameData,
    /* namespace of this opclass */
    pub opcnamespace: Oid,
    /* opclass owner */
    pub opcowner: Oid,
    /* containing operator family */
    pub opcfamily: Oid,
    /* type of data indexed by opclass */
    pub opcintype: Oid,
    /* T if opclass is default for opcintype */
    pub opcdefault: bool,
    /* type of data in index, or InvalidOid if same as input column type */
    pub opckeytype: Oid,
}

/*
 * Form_pg_opclass corresponds to a pointer to a tuple with the format of the
 * pg_opclass relation.
 */
pub type Form_pg_opclass = *mut FormData_pg_opclass;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // opcmethod sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_opclass, opcmethod), 4);
        // opcname follows oid (4) + opcmethod (4) = offset 8.
        assert_eq!(core::mem::offset_of!(FormData_pg_opclass, opcname), 8);
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_opclass>()
                >= core::mem::offset_of!(FormData_pg_opclass, opckeytype)
                    + core::mem::size_of::<Oid>()
        );
    }
}
