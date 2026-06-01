//! Translation of postgres/src/include/catalog/pg_ts_parser.h
//!
//! The `FormData_pg_ts_parser` struct: the fixed-layout part of a pg_ts_parser
//! catalog row (the "text search parser" system catalog).  This header has no
//! `#ifdef CATALOG_VARLEN` section, so every declared column is part of the
//! in-memory struct.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::NameData;
use crate::postgres_ext::Oid;

/* regproc is a C typedef for Oid (a registered-procedure OID). */
pub type regproc = Oid;

/*
 * FormData_pg_ts_parser - the fixed part of a pg_ts_parser row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_ts_parser {
    /* oid */
    pub oid: Oid,
    /* parser's name */
    pub prsname: NameData,
    /* name space */
    pub prsnamespace: Oid,
    /* init parsing session */
    pub prsstart: regproc,
    /* return next token */
    pub prstoken: regproc,
    /* finalize parsing session */
    pub prsend: regproc,
    /* return data for headline creation */
    pub prsheadline: regproc,
    /* return descriptions of lexeme's types */
    pub prslextype: regproc,
}

/*
 * Form_pg_ts_parser corresponds to a pointer to a row with the format of the
 * pg_ts_parser relation.
 */
pub type Form_pg_ts_parser = *mut FormData_pg_ts_parser;

/* This header declares no EXPOSE_TO_CLIENT_CODE constants. */

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // prsname sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_ts_parser, prsname), 4);
        // prsnamespace follows the NAMEDATALEN-byte prsname (offset 4 + 64 = 68).
        assert_eq!(
            core::mem::offset_of!(FormData_pg_ts_parser, prsnamespace),
            4 + core::mem::size_of::<NameData>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_ts_parser>()
                >= core::mem::offset_of!(FormData_pg_ts_parser, prslextype)
                    + core::mem::size_of::<regproc>()
        );
    }
}
