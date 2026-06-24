//! Translated from PostgreSQL src/include/catalog/pg_trigger.h

use crate::c::{NameData, bytea, varlena};
use crate::postgres_ext::Oid;

pub const TriggerRelationId: Oid = Oid(2620);

#[repr(C)]
pub struct FormData_pg_trigger {
    pub oid: Oid,
    pub tgrelid: Oid,
    pub tgparentid: Oid,
    pub tgname: NameData,
    pub tgfoid: Oid,
    pub tgtype: i16,
    pub tgenabled: i8, // char
    pub tgisinternal: bool,
    pub tgconstrrelid: Oid,
    pub tgconstrindid: Oid,
    pub tgconstraint: Oid,
    pub tgdeferrable: bool,
    pub tginitdeferred: bool,
    pub tgnargs: i16,
    pub tgattr: varlena, // int2vector (first varlen field, direct-accessible)
    // CATALOG_VARLEN (not in fixed part)
    pub tgargs: bytea,
    pub tgqual: varlena, // pg_node_tree
    pub tgoldtable: NameData,
    pub tgnewtable: NameData,
}

pub type Form_pg_trigger = *mut FormData_pg_trigger; // TODO(ptr)

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_trigger_oid: i32 = 1;
pub const Anum_pg_trigger_tgrelid: i32 = 2;
pub const Anum_pg_trigger_tgparentid: i32 = 3;
pub const Anum_pg_trigger_tgname: i32 = 4;
pub const Anum_pg_trigger_tgfoid: i32 = 5;
pub const Anum_pg_trigger_tgtype: i32 = 6;
pub const Anum_pg_trigger_tgenabled: i32 = 7;
pub const Anum_pg_trigger_tgisinternal: i32 = 8;
pub const Anum_pg_trigger_tgconstrrelid: i32 = 9;
pub const Anum_pg_trigger_tgconstrindid: i32 = 10;
pub const Anum_pg_trigger_tgconstraint: i32 = 11;
pub const Anum_pg_trigger_tgdeferrable: i32 = 12;
pub const Anum_pg_trigger_tginitdeferred: i32 = 13;
pub const Anum_pg_trigger_tgnargs: i32 = 14;
pub const Anum_pg_trigger_tgattr: i32 = 15;
pub const Anum_pg_trigger_tgargs: i32 = 16;
pub const Anum_pg_trigger_tgqual: i32 = 17;
pub const Anum_pg_trigger_tgoldtable: i32 = 18;
pub const Anum_pg_trigger_tgnewtable: i32 = 19;
pub const Natts_pg_trigger: i32 = 19;

// DECLARE_TOAST(pg_trigger, 2336, 2337)
// DECLARE_INDEX(pg_trigger_tgconstraint_index, 2699, TriggerConstraintIndexId, ...)
// DECLARE_UNIQUE_INDEX(pg_trigger_tgrelid_tgname_index, 2701, TriggerRelidNameIndexId, ...)
// DECLARE_UNIQUE_INDEX_PKEY(pg_trigger_oid_index, 2702, TriggerOidIndexId, ...)
// DECLARE_ARRAY_FOREIGN_KEY((tgrelid, tgattr), pg_attribute, (attrelid, attnum))

// Bits within tgtype (EXPOSE_TO_CLIENT_CODE).
pub const TRIGGER_TYPE_ROW: i16 = 1 << 0;
pub const TRIGGER_TYPE_BEFORE: i16 = 1 << 1;
pub const TRIGGER_TYPE_INSERT: i16 = 1 << 2;
pub const TRIGGER_TYPE_DELETE: i16 = 1 << 3;
pub const TRIGGER_TYPE_UPDATE: i16 = 1 << 4;
pub const TRIGGER_TYPE_TRUNCATE: i16 = 1 << 5;
pub const TRIGGER_TYPE_INSTEAD: i16 = 1 << 6;

pub const TRIGGER_TYPE_LEVEL_MASK: i16 = TRIGGER_TYPE_ROW;
pub const TRIGGER_TYPE_STATEMENT: i16 = 0;

pub const TRIGGER_TYPE_TIMING_MASK: i16 = TRIGGER_TYPE_BEFORE | TRIGGER_TYPE_INSTEAD;
pub const TRIGGER_TYPE_AFTER: i16 = 0;

pub const TRIGGER_TYPE_EVENT_MASK: i16 =
    TRIGGER_TYPE_INSERT | TRIGGER_TYPE_DELETE | TRIGGER_TYPE_UPDATE | TRIGGER_TYPE_TRUNCATE;

pub const fn TRIGGER_CLEAR_TYPE(t: &mut i16) {
    *t = 0;
}
pub const fn TRIGGER_SETT_ROW(t: &mut i16) {
    *t |= TRIGGER_TYPE_ROW;
}
pub const fn TRIGGER_SETT_STATEMENT(t: &mut i16) {
    *t |= TRIGGER_TYPE_STATEMENT;
}
pub const fn TRIGGER_SETT_BEFORE(t: &mut i16) {
    *t |= TRIGGER_TYPE_BEFORE;
}
pub const fn TRIGGER_SETT_AFTER(t: &mut i16) {
    *t |= TRIGGER_TYPE_AFTER;
}
pub const fn TRIGGER_SETT_INSTEAD(t: &mut i16) {
    *t |= TRIGGER_TYPE_INSTEAD;
}
pub const fn TRIGGER_SETT_INSERT(t: &mut i16) {
    *t |= TRIGGER_TYPE_INSERT;
}
pub const fn TRIGGER_SETT_DELETE(t: &mut i16) {
    *t |= TRIGGER_TYPE_DELETE;
}
pub const fn TRIGGER_SETT_UPDATE(t: &mut i16) {
    *t |= TRIGGER_TYPE_UPDATE;
}
pub const fn TRIGGER_SETT_TRUNCATE(t: &mut i16) {
    *t |= TRIGGER_TYPE_TRUNCATE;
}

pub const fn TRIGGER_FOR_ROW(t: i16) -> bool {
    t & TRIGGER_TYPE_ROW != 0
}
pub const fn TRIGGER_FOR_BEFORE(t: i16) -> bool {
    t & TRIGGER_TYPE_TIMING_MASK == TRIGGER_TYPE_BEFORE
}
pub const fn TRIGGER_FOR_AFTER(t: i16) -> bool {
    t & TRIGGER_TYPE_TIMING_MASK == TRIGGER_TYPE_AFTER
}
pub const fn TRIGGER_FOR_INSTEAD(t: i16) -> bool {
    t & TRIGGER_TYPE_TIMING_MASK == TRIGGER_TYPE_INSTEAD
}
pub const fn TRIGGER_FOR_INSERT(t: i16) -> bool {
    t & TRIGGER_TYPE_INSERT != 0
}
pub const fn TRIGGER_FOR_DELETE(t: i16) -> bool {
    t & TRIGGER_TYPE_DELETE != 0
}
pub const fn TRIGGER_FOR_UPDATE(t: i16) -> bool {
    t & TRIGGER_TYPE_UPDATE != 0
}
pub const fn TRIGGER_FOR_TRUNCATE(t: i16) -> bool {
    t & TRIGGER_TYPE_TRUNCATE != 0
}

pub const fn TRIGGER_TYPE_MATCHES(t: i16, level: i16, timing: i16, event: i16) -> bool {
    t & (TRIGGER_TYPE_LEVEL_MASK | TRIGGER_TYPE_TIMING_MASK | event) == level | timing | event
}
