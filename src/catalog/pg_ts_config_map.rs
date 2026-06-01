//! Translation of postgres/src/include/catalog/pg_ts_config_map.h
//!
//! The `FormData_pg_ts_config_map` struct: a pg_ts_config_map catalog row,
//! defining text search token-to-dictionary mappings for a configuration.
//! This header has no `#ifdef CATALOG_VARLEN` section, so every field of the
//! CATALOG struct is part of the fixed in-memory layout.
//!
//! Note there is no leading `oid` column; the unique key is
//! (mapcfg, maptokentype, mapseqno).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::int32;
use crate::postgres_ext::Oid;

/*
 * FormData_pg_ts_config_map - a pg_ts_config_map row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_ts_config_map {
    /* OID of configuration owning this entry */
    pub mapcfg: Oid,
    /* token type from parser */
    pub maptokentype: int32,
    /* order in which to consult dictionaries */
    pub mapseqno: int32,
    /* dictionary to consult */
    pub mapdict: Oid,
}

/*
 * Form_pg_ts_config_map corresponds to a pointer to a tuple with the format of
 * the pg_ts_config_map relation.
 */
pub type Form_pg_ts_config_map = *mut FormData_pg_ts_config_map;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // maptokentype sits right after the 4-byte mapcfg Oid (the first key field).
        assert_eq!(core::mem::offset_of!(FormData_pg_ts_config_map, maptokentype), 4);
        // The struct must at least span through its last fixed field, mapdict.
        assert!(
            core::mem::size_of::<FormData_pg_ts_config_map>()
                >= core::mem::offset_of!(FormData_pg_ts_config_map, mapdict)
                    + core::mem::size_of::<Oid>()
        );
    }
}
