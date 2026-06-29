//! Translated from PostgreSQL src/include/commands/trigger.h

use crate::access::tableam::{TM_FailureData, TM_Result};
use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::execnodes::{EPQState, EState, ResultRelInfo};
use crate::nodes::nodes::{CmdType, Node};
use crate::nodes::parsenodes::{ConstraintsSetStmt, CreateTrigStmt, RenameStmt};
use crate::nodes::bitmapset::Bitmapset;
use crate::access::htup::HeapTuple;
use crate::storage::itemptr::ItemPointerData;
use crate::storage::lock::LOCKMODE;
use crate::executor::tuptable::TupleTableSlot;
use crate::utils::reltrigger::{Trigger, TriggerDesc};
use crate::utils::tuplestore::Tuplestorestate;
use crate::postgres_ext::Oid;
use std::sync::Arc;
use crate::utils::rel::RelationData;

// ItemPointer: non-null pointer in C; nullable forms become Option at call sites.
type ItemPointer = *mut ItemPointerData; // TODO(ptr)

/// fcinfo->context is a TriggerData node. Translated as a runtime tag check.
// CALLED_AS_TRIGGER(fcinfo): context != NULL && IsA(context, TriggerData)
// Modeled as a fn once FunctionCallInfo carries a typed context; stub for now.
pub fn called_as_trigger(_context: Option<&Node>) -> bool {
    unimplemented!()
}

pub type TriggerEvent = u32;

/// TriggerData is the node passed as fmgr "context" when a trigger fn is called.
// NOTE(node): C has a leading `NodeTag type`; dropped (Node enum is its own
// discriminant, matching execnodes/plannodes). Reported, NOT added to nodes.rs.
pub struct TriggerData {
    pub event: TriggerEvent,
    pub relation: Arc<RelationData>,
    pub trigtuple: HeapTuple,
    pub newtuple: HeapTuple,
    pub trigger: *mut Trigger, // TODO(ptr)
    pub trigslot: *mut TupleTableSlot, // TODO(ptr)
    pub newslot: *mut TupleTableSlot,  // TODO(ptr)
    pub oldtable: *mut Tuplestorestate, // TODO(ptr)
    pub newtable: *mut Tuplestorestate, // TODO(ptr)
    pub updatedcols: *const Bitmapset, // TODO(ptr)
}

/// Opaque; private state defined in trigger.c, not ported.
pub struct AfterTriggersTableData;

/// State for capturing old/new tuples into transition tables for one operation.
pub struct TransitionCaptureState {
    pub delete_old_table: bool,
    pub update_old_table: bool,
    pub update_new_table: bool,
    pub insert_new_table: bool,
    pub original_insert_tuple: *mut TupleTableSlot, // TODO(ptr)
    pub insert_private: *mut AfterTriggersTableData, // TODO(ptr)
    pub update_private: *mut AfterTriggersTableData, // TODO(ptr)
    pub delete_private: *mut AfterTriggersTableData, // TODO(ptr)
}

// TriggerEvent bit flags. POOR per bitflags-port appendix 3.6: bits 0-1 are an
// INSERT/DELETE/UPDATE/TRUNCATE ordinal, bits 3-4 a timing value (can't be OR'd).
// Kept as raw i32-style consts + accessor fns rather than bitflags.
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

// More TriggerEvent flags, used only within trigger.c
pub const AFTER_TRIGGER_DEFERRABLE: TriggerEvent = 0x00000020;
pub const AFTER_TRIGGER_INITDEFERRED: TriggerEvent = 0x00000040;

pub const fn trigger_fired_by_insert(event: TriggerEvent) -> bool {
    (event & TRIGGER_EVENT_OPMASK) == TRIGGER_EVENT_INSERT
}
pub const fn trigger_fired_by_delete(event: TriggerEvent) -> bool {
    (event & TRIGGER_EVENT_OPMASK) == TRIGGER_EVENT_DELETE
}
pub const fn trigger_fired_by_update(event: TriggerEvent) -> bool {
    (event & TRIGGER_EVENT_OPMASK) == TRIGGER_EVENT_UPDATE
}
pub const fn trigger_fired_by_truncate(event: TriggerEvent) -> bool {
    (event & TRIGGER_EVENT_OPMASK) == TRIGGER_EVENT_TRUNCATE
}
pub const fn trigger_fired_for_row(event: TriggerEvent) -> bool {
    (event & TRIGGER_EVENT_ROW) != 0
}
pub const fn trigger_fired_for_statement(event: TriggerEvent) -> bool {
    !trigger_fired_for_row(event)
}
pub const fn trigger_fired_before(event: TriggerEvent) -> bool {
    (event & TRIGGER_EVENT_TIMINGMASK) == TRIGGER_EVENT_BEFORE
}
pub const fn trigger_fired_after(event: TriggerEvent) -> bool {
    (event & TRIGGER_EVENT_TIMINGMASK) == TRIGGER_EVENT_AFTER
}
pub const fn trigger_fired_instead(event: TriggerEvent) -> bool {
    (event & TRIGGER_EVENT_TIMINGMASK) == TRIGGER_EVENT_INSTEAD
}

// Replication role based firing.
pub const SESSION_REPLICATION_ROLE_ORIGIN: i32 = 0;
pub const SESSION_REPLICATION_ROLE_REPLICA: i32 = 1;
pub const SESSION_REPLICATION_ROLE_LOCAL: i32 = 2;
// PGDLLIMPORT int SessionReplicationRole -> global -> session/task state later.
pub static mut SessionReplicationRole: i32 = 0; // TODO(global): move to session state

// States at which a trigger can fire (pg_trigger.tgenabled).
pub const TRIGGER_FIRES_ON_ORIGIN: u8 = b'O';
pub const TRIGGER_FIRES_ALWAYS: u8 = b'A';
pub const TRIGGER_FIRES_ON_REPLICA: u8 = b'R';
pub const TRIGGER_DISABLED: u8 = b'D';

#[allow(clippy::too_many_arguments)]
pub fn CreateTrigger(
    _stmt: &CreateTrigStmt,
    _query_string: &str,
    _rel_oid: Oid,
    _ref_rel_oid: Oid,
    _constraint_oid: Oid,
    _index_oid: Oid,
    _funcoid: Oid,
    _parent_trigger_oid: Oid,
    _when_clause: Option<&Node>,
    _is_internal: bool,
    _in_partition: bool,
) -> ObjectAddress {
    unimplemented!()
}

#[allow(clippy::too_many_arguments)]
pub fn CreateTriggerFiringOn(
    _stmt: &CreateTrigStmt,
    _query_string: &str,
    _rel_oid: Oid,
    _ref_rel_oid: Oid,
    _constraint_oid: Oid,
    _index_oid: Oid,
    _funcoid: Oid,
    _parent_trigger_oid: Oid,
    _when_clause: Option<&Node>,
    _is_internal: bool,
    _in_partition: bool,
    _trigger_fires_when: u8,
) -> ObjectAddress {
    unimplemented!()
}

pub fn TriggerSetParentTrigger(
    _trig_rel: &RelationData,
    _child_trig_id: Oid,
    _parent_trig_id: Oid,
    _child_table_id: Oid,
) {
    unimplemented!()
}

pub fn RemoveTriggerById(_trig_oid: Oid) {
    unimplemented!()
}

// missing_ok -> Option (4): InvalidOid sentinel becomes None.
pub fn get_trigger_oid(_relid: Oid, _trigname: &str) -> Option<Oid> {
    unimplemented!()
}

pub fn renametrig(_stmt: &RenameStmt) -> ObjectAddress {
    unimplemented!()
}

#[allow(clippy::too_many_arguments)]
pub fn EnableDisableTrigger(
    _rel: &RelationData,
    _tgname: &str,
    _tgparent: Oid,
    _fires_when: u8,
    _skip_system: bool,
    _recurse: bool,
    _lockmode: LOCKMODE,
) {
    unimplemented!()
}

pub fn RelationBuildTriggers(_relation: &RelationData) {
    unimplemented!()
}

pub fn CopyTriggerDesc(_trigdesc: *mut TriggerDesc) -> *mut TriggerDesc {
    unimplemented!()
}

// Returns NULL if all triggers are inheritance-compatible -> Option.
pub fn FindTriggerIncompatibleWithInheritance(_trigdesc: *mut TriggerDesc) -> Option<&'static str> {
    unimplemented!()
}

pub fn MakeTransitionCaptureState(
    _trigdesc: *mut TriggerDesc,
    _relid: Oid,
    _cmd_type: CmdType,
) -> *mut TransitionCaptureState {
    unimplemented!()
}

pub fn FreeTriggerDesc(_trigdesc: *mut TriggerDesc) {
    unimplemented!()
}

pub fn ExecBSInsertTriggers(_estate: &mut EState<'_>, _relinfo: &mut ResultRelInfo) {
    unimplemented!()
}
pub fn ExecASInsertTriggers(
    _estate: &mut EState<'_>,
    _relinfo: &mut ResultRelInfo,
    _transition_capture: *mut TransitionCaptureState,
) {
    unimplemented!()
}
pub fn ExecBRInsertTriggers(
    _estate: &mut EState<'_>,
    _relinfo: &mut ResultRelInfo,
    _slot: &mut TupleTableSlot,
) -> bool {
    unimplemented!()
}
pub fn ExecARInsertTriggers(
    _estate: &mut EState<'_>,
    _relinfo: &mut ResultRelInfo,
    _slot: &mut TupleTableSlot,
    _recheck_indexes: Vec<Node>,
    _transition_capture: *mut TransitionCaptureState,
) {
    unimplemented!()
}
pub fn ExecIRInsertTriggers(
    _estate: &mut EState<'_>,
    _relinfo: &mut ResultRelInfo,
    _slot: &mut TupleTableSlot,
) -> bool {
    unimplemented!()
}
pub fn ExecBSDeleteTriggers(_estate: &mut EState<'_>, _relinfo: &mut ResultRelInfo) {
    unimplemented!()
}
pub fn ExecASDeleteTriggers(
    _estate: &mut EState<'_>,
    _relinfo: &mut ResultRelInfo,
    _transition_capture: *mut TransitionCaptureState,
) {
    unimplemented!()
}

/// ExecBRDeleteTriggers: tmresult/tmfd/epqslot are out-params; kept as out-refs
/// here (revisit with the .c body for tuple/Option folding). is_merge_delete in.
#[allow(clippy::too_many_arguments)]
pub fn ExecBRDeleteTriggers(
    _estate: &mut EState<'_>,
    _epqstate: &mut EPQState,
    _relinfo: &mut ResultRelInfo,
    _tupleid: ItemPointer,
    _fdw_trigtuple: HeapTuple,
    _epqslot: *mut *mut TupleTableSlot,
    _tmresult: *mut TM_Result,
    _tmfd: *mut TM_FailureData,
    _is_merge_delete: bool,
) -> bool {
    unimplemented!()
}
pub fn ExecARDeleteTriggers(
    _estate: &mut EState<'_>,
    _relinfo: &mut ResultRelInfo,
    _tupleid: ItemPointer,
    _fdw_trigtuple: HeapTuple,
    _transition_capture: *mut TransitionCaptureState,
    _is_crosspart_update: bool,
) {
    unimplemented!()
}
pub fn ExecIRDeleteTriggers(
    _estate: &mut EState<'_>,
    _relinfo: &mut ResultRelInfo,
    _trigtuple: HeapTuple,
) -> bool {
    unimplemented!()
}
pub fn ExecBSUpdateTriggers(_estate: &mut EState<'_>, _relinfo: &mut ResultRelInfo) {
    unimplemented!()
}
pub fn ExecASUpdateTriggers(
    _estate: &mut EState<'_>,
    _relinfo: &mut ResultRelInfo,
    _transition_capture: *mut TransitionCaptureState,
) {
    unimplemented!()
}
#[allow(clippy::too_many_arguments)]
pub fn ExecBRUpdateTriggers(
    _estate: &mut EState<'_>,
    _epqstate: &mut EPQState,
    _relinfo: &mut ResultRelInfo,
    _tupleid: ItemPointer,
    _fdw_trigtuple: HeapTuple,
    _newslot: &mut TupleTableSlot,
    _tmresult: *mut TM_Result,
    _tmfd: *mut TM_FailureData,
    _is_merge_update: bool,
) -> bool {
    unimplemented!()
}
#[allow(clippy::too_many_arguments)]
pub fn ExecARUpdateTriggers(
    _estate: &mut EState<'_>,
    _relinfo: &mut ResultRelInfo,
    _src_partinfo: &mut ResultRelInfo,
    _dst_partinfo: &mut ResultRelInfo,
    _tupleid: ItemPointer,
    _fdw_trigtuple: HeapTuple,
    _newslot: &mut TupleTableSlot,
    _recheck_indexes: Vec<Node>,
    _transition_capture: *mut TransitionCaptureState,
    _is_crosspart_update: bool,
) {
    unimplemented!()
}
pub fn ExecIRUpdateTriggers(
    _estate: &mut EState<'_>,
    _relinfo: &mut ResultRelInfo,
    _trigtuple: HeapTuple,
    _newslot: &mut TupleTableSlot,
) -> bool {
    unimplemented!()
}
pub fn ExecBSTruncateTriggers(_estate: &mut EState<'_>, _relinfo: &mut ResultRelInfo) {
    unimplemented!()
}
pub fn ExecASTruncateTriggers(_estate: &mut EState<'_>, _relinfo: &mut ResultRelInfo) {
    unimplemented!()
}

pub fn AfterTriggerBeginXact() {
    unimplemented!()
}
pub fn AfterTriggerBeginQuery() {
    unimplemented!()
}
pub fn AfterTriggerEndQuery(_estate: &mut EState<'_>) {
    unimplemented!()
}
pub fn AfterTriggerFireDeferred() {
    unimplemented!()
}
pub fn AfterTriggerEndXact(_is_commit: bool) {
    unimplemented!()
}
pub fn AfterTriggerBeginSubXact() {
    unimplemented!()
}
pub fn AfterTriggerEndSubXact(_is_commit: bool) {
    unimplemented!()
}
pub fn AfterTriggerSetState(_stmt: &ConstraintsSetStmt) {
    unimplemented!()
}
pub fn AfterTriggerPendingOnRel(_relid: Oid) -> bool {
    unimplemented!()
}

// in utils/adt/ri_triggers.c
pub fn RI_FKey_pk_upd_check_required(
    _trigger: *mut Trigger,
    _pk_rel: &RelationData,
    _oldslot: &mut TupleTableSlot,
    _newslot: &mut TupleTableSlot,
) -> bool {
    unimplemented!()
}
pub fn RI_FKey_fk_upd_check_required(
    _trigger: *mut Trigger,
    _fk_rel: &RelationData,
    _oldslot: &mut TupleTableSlot,
    _newslot: &mut TupleTableSlot,
) -> bool {
    unimplemented!()
}
pub fn RI_Initial_Check(_trigger: *mut Trigger, _fk_rel: &RelationData, _pk_rel: &RelationData) -> bool {
    unimplemented!()
}
pub fn RI_PartitionRemove_Check(_trigger: *mut Trigger, _fk_rel: &RelationData, _pk_rel: &RelationData) {
    unimplemented!()
}

// result values for RI_FKey_trigger_type
pub const RI_TRIGGER_PK: i32 = 1;
pub const RI_TRIGGER_FK: i32 = 2;
pub const RI_TRIGGER_NONE: i32 = 0;

pub fn RI_FKey_trigger_type(_tgfoid: Oid) -> i32 {
    unimplemented!()
}
