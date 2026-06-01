//! Translation of postgres/src/include/catalog/pg_database.h
//!
//! The `FormData_pg_database` struct: the fixed-layout part of a pg_database
//! catalog row.  As in the C header, the struct as compiled into the backend
//! stops at the field just before `#ifdef CATALOG_VARLEN`; the trailing
//! variable-length fields (datcollate, datctype, datlocale, daticurules,
//! datcollversion, datacl[], guarded by CATALOG_VARLEN) are NOT part of this
//! in-memory struct - they live only in a real on-disk pg_database tuple and
//! are reached via heap_getattr.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::{int32, NameData, TransactionId};
use crate::postgres_ext::Oid;
use core::ffi::c_char;

/*
 * FormData_pg_database - the fixed part of a pg_database row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_database {
    /* oid */
    pub oid: Oid,
    /* database name */
    pub datname: NameData,
    /* owner of database */
    pub datdba: Oid,
    /* character encoding */
    pub encoding: int32,
    /* locale provider, see pg_collation.collprovider */
    pub datlocprovider: c_char,
    /* allowed as CREATE DATABASE template? */
    pub datistemplate: bool,
    /* new connections allowed? */
    pub datallowconn: bool,
    /* database has login event triggers? */
    pub dathasloginevt: bool,
    /*
     * Max connections allowed. Negative values have special meaning, see
     * DATCONNLIMIT_* defines below.
     */
    pub datconnlimit: int32,
    /* all Xids < this are frozen in this DB */
    pub datfrozenxid: TransactionId,
    /* all multixacts in the DB are >= this */
    pub datminmxid: TransactionId,
    /* default table space for this DB */
    pub dattablespace: Oid,
}

/*
 * Form_pg_database corresponds to a pointer to a tuple with the format of the
 * pg_database relation.
 */
pub type Form_pg_database = *mut FormData_pg_database;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 * ----------------------------------------------------------------
 */

/*
 * Special values for pg_database.datconnlimit. Normal values are >= 0.
 */
/* no limit */
pub const DATCONNLIMIT_UNLIMITED: int32 = -1;

/*
 * A database is set to invalid partway through being dropped.  Using
 * datconnlimit=-2 for this purpose isn't particularly clean, but is
 * backpatchable.
 */
pub const DATCONNLIMIT_INVALID_DB: int32 = -2;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // datname sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_database, datname), 4);
        // datdba follows the NAMEDATALEN-byte datname (offset 4 + 64 = 68).
        assert_eq!(
            core::mem::offset_of!(FormData_pg_database, datdba),
            4 + core::mem::size_of::<NameData>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_database>()
                >= core::mem::offset_of!(FormData_pg_database, dattablespace)
                    + core::mem::size_of::<Oid>()
        );
    }
}
