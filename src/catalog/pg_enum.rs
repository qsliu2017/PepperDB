//! Translation of postgres/src/include/catalog/pg_enum.h
//!
//! The `FormData_pg_enum` struct: the fixed-layout part of a pg_enum catalog
//! row, defining the "enum" system catalog (pg_enum) which records the label
//! values and sort positions of every enumerated-type value.
//!
//! The C header has NO `#ifdef CATALOG_VARLEN` section, so every declared
//! column is part of the fixed struct - including the trailing NameData
//! enumlabel, which is a fixed NAMEDATALEN-byte field rather than a varlen
//! attribute.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::{float4, NameData};
use crate::postgres_ext::Oid;

/*
 * FormData_pg_enum - a pg_enum row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_enum {
    /* oid */
    pub oid: Oid,
    /* OID of owning enum type */
    pub enumtypid: Oid,
    /* sort position of this enum value */
    pub enumsortorder: float4,
    /* text representation of enum value */
    pub enumlabel: NameData,
}

/*
 * Form_pg_enum corresponds to a pointer to a tuple with the format of the
 * pg_enum relation.
 */
pub type Form_pg_enum = *mut FormData_pg_enum;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 *
 * pg_enum.h exposes no #define constants to client code.
 * ----------------------------------------------------------------
 */

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // enumtypid sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_enum, enumtypid), 4);
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_enum>()
                >= core::mem::offset_of!(FormData_pg_enum, enumlabel)
                    + core::mem::size_of::<NameData>()
        );
    }
}
