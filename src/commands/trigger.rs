//! Partial translation of postgres/src/include/commands/trigger.h
//!
//! The trigger-manager call interface: the `TriggerData` node a trigger function
//! receives via `fcinfo->context`, the `TRIGGER_EVENT_*` bits, and the
//! `TRIGGER_FIRED_*` / `CALLED_AS_TRIGGER` test macros.  Only what the builtin
//! trigger support functions (utils/adt/trigfuncs.c) need is translated; the
//! Trigger relcache entry and the trigger-firing machinery are future work.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::access::htup_details::HeapTuple;
use crate::c::uint32;
use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::execnodes::Relation;
use crate::nodes::nodes::NodeTag;
use crate::executor::tuptable::TupleTableSlot;
use crate::utils::fmgr::FunctionCallInfo;
use core::ffi::c_void;

/// The bitmask describing which trigger event fired (see TRIGGER_EVENT_*).
pub type TriggerEvent = uint32;

/// pg_trigger cache entry (catalog/relcache layer) - opaque stub for now.
/// TODO(pg-port): real `Trigger` from utils/reltrigger.h / relcache.
#[repr(C)]
pub struct Trigger {
    _opaque: [u8; 0],
}

/// Tuplestore for transition-table rows - opaque stub for now.
/// TODO(pg-port): real Tuplestorestate from utils/tuplestore.c.
#[repr(C)]
pub struct Tuplestorestate {
    _opaque: [u8; 0],
}

/*
 * TriggerData is the node type that is passed as fmgr "context" info when a
 * function is called by the trigger manager.
 */
#[repr(C)]
pub struct TriggerData {
    pub r#type: NodeTag,
    pub tg_event: TriggerEvent,
    pub tg_relation: Relation,
    pub tg_trigtuple: HeapTuple,
    pub tg_newtuple: HeapTuple,
    pub tg_trigger: *mut Trigger,
    pub tg_trigslot: *mut TupleTableSlot,
    pub tg_newslot: *mut TupleTableSlot,
    pub tg_oldtable: *mut Tuplestorestate,
    pub tg_newtable: *mut Tuplestorestate,
    pub tg_updatedcols: *const Bitmapset,
}

/* TriggerEvent bit flags */
pub const TRIGGER_EVENT_INSERT: TriggerEvent = 0x00000000;
pub const TRIGGER_EVENT_DELETE: TriggerEvent = 0x00000001;
pub const TRIGGER_EVENT_UPDATE: TriggerEvent = 0x00000002;
pub const TRIGGER_EVENT_TRUNCATE: TriggerEvent = 0x00000003;
pub const TRIGGER_EVENT_OPMASK: TriggerEvent = 0x00000003;

pub const TRIGGER_EVENT_ROW: TriggerEvent = 0x00000004;

pub const TRIGGER_EVENT_BEFORE: TriggerEvent = 0x00000008;
pub const TRIGGER_EVENT_AFTER: TriggerEvent = 0x00000000;
pub const TRIGGER_EVENT_INSTEAD: TriggerEvent = 0x00000010;
pub const TRIGGER_EVENT_TIMINGMASK: TriggerEvent = 0x00000018;

/* More TriggerEvent flags, used only within trigger.c (not for clients). */
pub const AFTER_TRIGGER_DEFERRABLE: TriggerEvent = 0x00000020;
pub const AFTER_TRIGGER_INITDEFERRED: TriggerEvent = 0x00000040;

#[inline]
pub fn TRIGGER_FIRED_BY_INSERT(event: TriggerEvent) -> bool {
    (event & TRIGGER_EVENT_OPMASK) == TRIGGER_EVENT_INSERT
}
#[inline]
pub fn TRIGGER_FIRED_BY_DELETE(event: TriggerEvent) -> bool {
    (event & TRIGGER_EVENT_OPMASK) == TRIGGER_EVENT_DELETE
}
#[inline]
pub fn TRIGGER_FIRED_BY_UPDATE(event: TriggerEvent) -> bool {
    (event & TRIGGER_EVENT_OPMASK) == TRIGGER_EVENT_UPDATE
}
#[inline]
pub fn TRIGGER_FIRED_BY_TRUNCATE(event: TriggerEvent) -> bool {
    (event & TRIGGER_EVENT_OPMASK) == TRIGGER_EVENT_TRUNCATE
}
#[inline]
pub fn TRIGGER_FIRED_FOR_ROW(event: TriggerEvent) -> bool {
    (event & TRIGGER_EVENT_ROW) != 0
}
#[inline]
pub fn TRIGGER_FIRED_FOR_STATEMENT(event: TriggerEvent) -> bool {
    (event & TRIGGER_EVENT_ROW) == 0
}
#[inline]
pub fn TRIGGER_FIRED_BEFORE(event: TriggerEvent) -> bool {
    (event & TRIGGER_EVENT_TIMINGMASK) == TRIGGER_EVENT_BEFORE
}
#[inline]
pub fn TRIGGER_FIRED_AFTER(event: TriggerEvent) -> bool {
    (event & TRIGGER_EVENT_TIMINGMASK) == TRIGGER_EVENT_AFTER
}
#[inline]
pub fn TRIGGER_FIRED_INSTEAD(event: TriggerEvent) -> bool {
    (event & TRIGGER_EVENT_TIMINGMASK) == TRIGGER_EVENT_INSTEAD
}

/*
 * Test whether the function is being called as a trigger, i.e. fcinfo->context
 * is a TriggerData node.
 *
 * # Safety
 * `fcinfo` is a valid FunctionCallInfo (or null).
 */
#[inline]
pub unsafe fn CALLED_AS_TRIGGER(fcinfo: FunctionCallInfo) -> bool {
    let ctx = (*fcinfo).context as *const NodeTag;
    !ctx.is_null() && *ctx == NodeTag::T_TriggerData
}

// Silence unused-import warnings for the opaque c_void helper type used by
// downstream trigger code (kept for the module's future expansion).
#[allow(dead_code)]
type _Unused = *mut c_void;
