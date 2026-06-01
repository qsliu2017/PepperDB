//! Translation of postgres/src/include/catalog/pg_ts_config.h
//!
//! The `FormData_pg_ts_config` struct: the fixed-layout part of a pg_ts_config
//! ("text search configuration") catalog row.  This header has no
//! `#ifdef CATALOG_VARLEN` section, so every column declared in the CATALOG(...)
//! body is part of this in-memory struct.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::NameData;
use crate::postgres_ext::Oid;

/*
 * FormData_pg_ts_config - the fixed part of a pg_ts_config row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_ts_config {
    /* oid */
    pub oid: Oid,
    /* name of configuration */
    pub cfgname: NameData,
    /* name space */
    pub cfgnamespace: Oid,
    /* owner */
    pub cfgowner: Oid,
    /* OID of parser */
    pub cfgparser: Oid,
}

/*
 * Form_pg_ts_config corresponds to a pointer to a row with the format of the
 * pg_ts_config relation.
 */
pub type Form_pg_ts_config = *mut FormData_pg_ts_config;

/* This header declares no EXPOSE_TO_CLIENT_CODE #define constants. */

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // cfgname sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_ts_config, cfgname), 4);
        // cfgnamespace follows the NAMEDATALEN-byte cfgname (offset 4 + 64 = 68).
        assert_eq!(
            core::mem::offset_of!(FormData_pg_ts_config, cfgnamespace),
            4 + core::mem::size_of::<NameData>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_ts_config>()
                >= core::mem::offset_of!(FormData_pg_ts_config, cfgparser)
                    + core::mem::size_of::<Oid>()
        );
    }
}
