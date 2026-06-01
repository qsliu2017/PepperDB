//! utils/reltrigger.h - POSTGRES relation trigger definitions.
//!
//! These structs really belong to trigger.h, but are kept separate so they can
//! be cleanly included in rel.h and other places.

use std::ffi::c_char;
use std::ffi::c_int;

use crate::c::int16;
use crate::postgres_ext::Oid;

#[repr(C)]
pub struct Trigger {
    /// OID of trigger (pg_trigger row)
    pub tgoid: Oid,
    // Remaining fields are copied from pg_trigger, see pg_trigger.h
    pub tgname: *mut c_char,
    pub tgfoid: Oid,
    pub tgtype: int16,
    pub tgenabled: c_char,
    pub tgisinternal: bool,
    pub tgisclone: bool,
    pub tgconstrrelid: Oid,
    pub tgconstrindid: Oid,
    pub tgconstraint: Oid,
    pub tgdeferrable: bool,
    pub tginitdeferred: bool,
    pub tgnargs: int16,
    pub tgnattr: int16,
    pub tgattr: *mut int16,
    pub tgargs: *mut *mut c_char,
    pub tgqual: *mut c_char,
    pub tgoldtable: *mut c_char,
    pub tgnewtable: *mut c_char,
}

#[repr(C)]
pub struct TriggerDesc {
    /// array of Trigger structs
    pub triggers: *mut Trigger,
    /// number of array entries
    pub numtriggers: c_int,

    // These flags indicate whether the array contains at least one of each
    // type of trigger.  We use these to skip searching the array if not.
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
    // there are no row-level truncate triggers
    pub trig_truncate_before_statement: bool,
    pub trig_truncate_after_statement: bool,
    // Is there at least one trigger specifying each transition relation?
    pub trig_insert_new_table: bool,
    pub trig_update_old_table: bool,
    pub trig_update_new_table: bool,
    pub trig_delete_old_table: bool,
}
