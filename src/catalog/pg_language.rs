//! Translation of postgres/src/include/catalog/pg_language.h
//!
//! The `FormData_pg_language` struct: the fixed-layout part of a pg_language
//! catalog row.  As in the C header, the struct as compiled into the backend
//! stops at the field just before `#ifdef CATALOG_VARLEN`; the trailing
//! variable-length field (lanacl[], guarded by CATALOG_VARLEN) is NOT part of
//! this in-memory struct - it lives only in a real on-disk pg_language tuple and
//! is reached via heap_getattr.
//!
//! This header declares no EXPOSE_TO_CLIENT_CODE constants.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::NameData;
use crate::postgres_ext::Oid;

/*
 * FormData_pg_language - the fixed part of a pg_language row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_language {
    /* oid */
    pub oid: Oid,
    /* language name */
    pub lanname: NameData,
    /* language's owner */
    pub lanowner: Oid,
    /* is a procedural language */
    pub lanispl: bool,
    /* PL is trusted */
    pub lanpltrusted: bool,
    /* call handler, if it's a PL */
    pub lanplcallfoid: Oid,
    /* optional anonymous-block handler function */
    pub laninline: Oid,
    /* optional validation function */
    pub lanvalidator: Oid,
}

/*
 * Form_pg_language corresponds to a pointer to a tuple with the format of the
 * pg_language relation.
 */
pub type Form_pg_language = *mut FormData_pg_language;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // lanname sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_language, lanname), 4);
        // lanowner follows the NAMEDATALEN-byte lanname (offset 4 + 64 = 68).
        assert_eq!(
            core::mem::offset_of!(FormData_pg_language, lanowner),
            4 + core::mem::size_of::<NameData>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_language>()
                >= core::mem::offset_of!(FormData_pg_language, lanvalidator)
                    + core::mem::size_of::<Oid>()
        );
    }
}
