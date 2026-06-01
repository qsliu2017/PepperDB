//! Translation of postgres/src/include/catalog/pg_publication.h
//!
//! The `FormData_pg_publication` struct: the fixed-layout part of a
//! pg_publication catalog row.  The C CATALOG(pg_publication) struct has no
//! `#ifdef CATALOG_VARLEN` section, so every declared column is part of this
//! in-memory struct.  The EXPOSE_TO_CLIENT_CODE section of the header defines
//! the PublishGencolsType enum (values of the pubgencols column), translated
//! below as pub consts.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::NameData;
use crate::postgres_ext::Oid;
use core::ffi::c_char;

/*
 * FormData_pg_publication - the fixed part of a pg_publication row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_publication {
    /* oid */
    pub oid: Oid,
    /* name of the publication */
    pub pubname: NameData,
    /* publication owner */
    pub pubowner: Oid,
    /* encompass all tables in the database (except unlogged and temp ones) */
    pub puballtables: bool,
    /* true if inserts are published */
    pub pubinsert: bool,
    /* true if updates are published */
    pub pubupdate: bool,
    /* true if deletes are published */
    pub pubdelete: bool,
    /* true if truncates are published */
    pub pubtruncate: bool,
    /* true if partition changes are published using root schema */
    pub pubviaroot: bool,
    /* 'n'(none)/'s'(stored): how generated column data should be published */
    pub pubgencols: c_char,
}

/*
 * Form_pg_publication corresponds to a pointer to a tuple with the format of
 * the pg_publication relation.
 */
pub type Form_pg_publication = *mut FormData_pg_publication;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 *
 * PublishGencolsType - values of the pubgencols column.
 * ----------------------------------------------------------------
 */

/* Generated columns present should not be replicated. */
pub const PUBLISH_GENCOLS_NONE: c_char = b'n' as c_char;
/* Generated columns present should be replicated. */
pub const PUBLISH_GENCOLS_STORED: c_char = b's' as c_char;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // pubname sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_publication, pubname), 4);
        // pubowner follows the NAMEDATALEN-byte pubname (offset 4 + 64 = 68).
        assert_eq!(
            core::mem::offset_of!(FormData_pg_publication, pubowner),
            4 + core::mem::size_of::<NameData>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_publication>()
                >= core::mem::offset_of!(FormData_pg_publication, pubgencols)
                    + core::mem::size_of::<c_char>()
        );
    }
}
