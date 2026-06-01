//! Translation of postgres/src/include/catalog/pg_rewrite.h
//!
//! The `FormData_pg_rewrite` struct: the fixed-layout, guaranteed-not-null part
//! of a pg_rewrite (rewrite rule) catalog row.  As in the C header, the struct
//! as compiled into the backend stops at the field just before
//! `#ifdef CATALOG_VARLEN`; the trailing variable-length fields (ev_qual,
//! ev_action, both pg_node_tree guarded by CATALOG_VARLEN) are NOT part of this
//! in-memory struct - they live only in a real on-disk pg_rewrite tuple and are
//! reached via heap_getattr.
//!
//! As of Postgres 7.3, the primary key for this table is <ev_class, rulename>
//! --- ie, rule names are only unique among the rules of a given table.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::NameData;
use crate::postgres_ext::Oid;
use core::ffi::c_char;

/*
 * FormData_pg_rewrite - the fixed part of a pg_rewrite row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_rewrite {
    /* oid */
    pub oid: Oid,
    /* name of the rule (unique among the rules of a given ev_class) */
    pub rulename: NameData,
    /* OID of the relation (pg_class) the rule is attached to */
    pub ev_class: Oid,
    /* event type that the rule is for (see CmdType, e.g. SELECT/UPDATE) */
    pub ev_type: c_char,
    /* rule firing configuration ('O'/'D'/'R'/'A'); see RULE_* in rewriteDefine.h */
    pub ev_enabled: c_char,
    /* is this an INSTEAD rule? */
    pub is_instead: bool,
}

/*
 * Form_pg_rewrite corresponds to a pointer to a tuple with the format of the
 * pg_rewrite relation.
 */
pub type Form_pg_rewrite = *mut FormData_pg_rewrite;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 *
 * The pg_rewrite.h catalog header defines no #define constants (no
 * EXPOSE_TO_CLIENT_CODE block); the ev_type / ev_enabled enumeration values
 * (RULE_FIRES_ON_ORIGIN, RULE_DISABLED, etc.) live in rewrite/rewriteDefine.h,
 * which is a separate (not-yet-ported) header.
 * ----------------------------------------------------------------
 */

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // rulename sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_rewrite, rulename), 4);
        // ev_class follows the NAMEDATALEN-byte rulename (offset 4 + 64 = 68).
        assert_eq!(
            core::mem::offset_of!(FormData_pg_rewrite, ev_class),
            4 + core::mem::size_of::<NameData>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_rewrite>()
                >= core::mem::offset_of!(FormData_pg_rewrite, is_instead)
                    + core::mem::size_of::<bool>()
        );
    }
}
