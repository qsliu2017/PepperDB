//! Translation of postgres/src/include/catalog/pg_conversion.h
//!
//! The `FormData_pg_conversion` struct: the fixed-layout part of a
//! pg_conversion catalog row.  The C header has no `#ifdef CATALOG_VARLEN`
//! cutoff, so every column declared in the CATALOG(...) body is part of this
//! in-memory struct.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::{int32, NameData};
use crate::postgres_ext::Oid;

/* regproc is a C typedef for Oid (a registered-procedure OID). */
pub type regproc = Oid;

/*
 * FormData_pg_conversion - the fixed part of a pg_conversion row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_conversion {
    /* oid */
    pub oid: Oid,
    /* name of the conversion */
    pub conname: NameData,
    /* namespace that the conversion belongs to */
    pub connamespace: Oid,
    /* owner of the conversion */
    pub conowner: Oid,
    /* FOR encoding id */
    pub conforencoding: int32,
    /* TO encoding id */
    pub contoencoding: int32,
    /* OID of the conversion proc */
    pub conproc: regproc,
    /* true if this is a default conversion */
    pub condefault: bool,
}

/*
 * Form_pg_conversion corresponds to a pointer to a tuple with the format of
 * the pg_conversion relation.
 */
pub type Form_pg_conversion = *mut FormData_pg_conversion;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // conname sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_conversion, conname), 4);
        // connamespace follows the NAMEDATALEN-byte conname (offset 4 + 64 = 68).
        assert_eq!(
            core::mem::offset_of!(FormData_pg_conversion, connamespace),
            4 + core::mem::size_of::<NameData>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_conversion>()
                >= core::mem::offset_of!(FormData_pg_conversion, condefault)
                    + core::mem::size_of::<bool>()
        );
    }
}
