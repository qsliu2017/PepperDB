//! Translated from PostgreSQL src/include/utils/reltrigger.h
//
// Relation trigger descriptors. In-memory planner/executor state (not on-disk), so
// idiomatic Rust: owned strings/vecs, no #[repr(C)]. Fields mirror pg_trigger rows.

use crate::postgres_ext::Oid;

pub struct Trigger {
    pub tgoid: Oid,
    pub tgname: String,
    pub tgfoid: Oid,
    pub tgtype: i16,
    pub tgenabled: u8,
    pub tgisinternal: bool,
    pub tgisclone: bool,
    pub tgconstrrelid: Oid,
    pub tgconstrindid: Oid,
    pub tgconstraint: Oid,
    pub tgdeferrable: bool,
    pub tginitdeferred: bool,
    pub tgnargs: i16,
    pub tgnattr: i16,
    pub tgattr: Vec<i16>,
    pub tgargs: Vec<String>,
    pub tgqual: Option<String>,
    pub tgoldtable: Option<String>,
    pub tgnewtable: Option<String>,
}

pub struct TriggerDesc {
    pub triggers: Vec<Trigger>,

    // Per-type presence flags (skip-search optimization in C).
    pub trig_insert_before_row: bool,
    pub trig_insert_after_row: bool,
    pub trig_insert_instead_row: bool,
    pub trig_insert_before_statement: bool,
    pub trig_insert_after_statement: bool,
    pub trig_update_before_row: bool,
    pub trig_update_after_row: bool,
    pub trig_update_instead_row: bool,
    pub trig_update_before_statement: bool,
    pub trig_update_after_statement: bool,
    pub trig_delete_before_row: bool,
    pub trig_delete_after_row: bool,
    pub trig_delete_instead_row: bool,
    pub trig_delete_before_statement: bool,
    pub trig_delete_after_statement: bool,
    pub trig_truncate_before_statement: bool,
    pub trig_truncate_after_statement: bool,
    pub trig_insert_new_table: bool,
    pub trig_update_old_table: bool,
    pub trig_update_new_table: bool,
    pub trig_delete_old_table: bool,
}
