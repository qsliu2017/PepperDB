/*-------------------------------------------------------------------------
 *
 * nodeModifyTable.c
 *    routines to handle ModifyTable nodes.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/executor/nodeModifyTable.c
 *
 *-------------------------------------------------------------------------
 */
/* INTERFACE ROUTINES
 *      ExecInitModifyTable - initialize the ModifyTable node
 *      ExecModifyTable     - retrieve the next tuple from the node
 *      ExecEndModifyTable  - shut down the ModifyTable node
 *      ExecReScanModifyTable - rescan the ModifyTable node
 *
 *   NOTES
 *      The ModifyTable node receives input from its outerPlan, which is
 *      the data to insert for INSERT cases, the changed columns' new
 *      values plus row-locating info for UPDATE and MERGE cases, or just the
 *      row-locating info for DELETE cases.
 */

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_unsafe)]
#![allow(unreachable_code)]

use crate::prelude::*;

use std::ffi::c_int;

use crate::nodes::nodes::{CmdType, NodeTag, Node};
use crate::nodes::nodes::CmdType::*;
use crate::nodes::primnodes::{
    Expr, MergeAction, MergeMatchKind, NUM_MERGE_MATCH_KINDS,
};
use crate::nodes::primnodes::MergeMatchKind::*;
use crate::nodes::parsenodes::{WithCheckOption, RangeTblEntry};
use crate::nodes::parsenodes::WCOKind::*;
use crate::nodes::plannodes::{Plan, ModifyTable, PlanRowMark};
use crate::nodes::nodes::OnConflictAction;
use crate::nodes::nodes::OnConflictAction::*;
use crate::nodes::pg_list::List;
use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::lockoptions::{LockTupleMode, LockWaitPolicy};
use crate::nodes::lockoptions::LockTupleMode::*;
use crate::nodes::lockoptions::LockWaitPolicy::*;
use crate::nodes::execnodes::{
    EState, ModifyTableState, ResultRelInfo, EPQState, ExprState, ExprContext,
    ProjectionInfo, OnConflictSetState, MergeActionState,
    TupleTableSlot, TupleConversionMap, TransitionCaptureState,
    PartitionTupleRouting, HTAB,
};
use crate::nodes::execnodes::{
    EEO_FLAG_HAS_OLD, EEO_FLAG_HAS_NEW, EEO_FLAG_OLD_IS_NULL, EEO_FLAG_NEW_IS_NULL,
    MERGE_INSERT, MERGE_UPDATE, MERGE_DELETE,
};
use crate::access::htup_details::{HeapTuple, HeapTupleData, HeapTupleHeaderData,
    HeapTupleHeaderGetDatumLength};
use crate::access::common::tupdesc::{TupleDesc, TupleDescAttr, TupleDescCompactAttr};
use crate::access::common::tupconvert::execute_attr_map_slot;
use crate::access::common::attmap::{build_attrmap_by_name, AttrMap};
use crate::access::table::tableam::{
    TM_Result, TM_FailureData, TU_UpdateIndexes,
    table_slot_create, table_tuple_insert, table_tuple_delete,
    table_tuple_update, TUPLE_LOCK_FLAG_FIND_LAST_VERSION,
};
use crate::access::table::tableam::TM_Result::*;
use crate::access::table::tableam::TU_UpdateIndexes::*;
use crate::access::sysattr::{MinTransactionIdAttributeNumber, FirstLowInvalidHeapAttributeNumber};
use crate::access::attnum::AttributeNumberIsValid;
use crate::storage::itemptr::{ItemPointerData, ItemPointerCopy, ItemPointerSetInvalid, ItemPointerIsValid};
use crate::storage::lmgr::lmgr::{LockTuple, UnlockTuple, SpeculativeInsertionLockAcquire, SpeculativeInsertionLockRelease};
use crate::storage::lockdefs::InplaceUpdateTupleLock;
use crate::catalog::pg_class::{RELKIND_RELATION, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE, RELKIND_FOREIGN_TABLE, RELKIND_VIEW};
use crate::utils::rel::{Relation, RelationGetRelid, RelationGetDescr, RelationGetRelationName, RelationGetForm};
use crate::utils::adt::datum::datumCopy;
use crate::utils::builtins::format_type_be;
use crate::utils::hash::dynahash::{HASHCTL, hash_create, hash_search, HASH_ELEM, HASH_BLOBS, HASH_CONTEXT};
use crate::utils::hash::dynahash::HASHACTION::{HASH_FIND, HASH_ENTER};
use crate::utils::palloc::{palloc, palloc0, pfree, MemoryContextSwitchTo};
use crate::utils::mmgr::mcxt::CurrentMemoryContext;
use crate::nodes::nodeFuncs::exprType;
use crate::nodes::bitmapset::{bms_is_member, bms_add_member, bms_overlap};
use crate::{makeNode, castNode};
use crate::optimizer::util::var::pull_varattnos;
use crate::rewrite::rewriteManip::map_variable_attnos;
use crate::executor::execIndexing::{ExecInsertIndexTuples, ExecOpenIndices, ExecCheckIndexConstraints};
use crate::executor::execUtils::{
    ExecGetReturningSlot, ExecGetAllNullSlot, ExecGetChildToRootMap, ExecGetRootToChildMap,
    ExecAssignExprContext, ExecInitResultRelation, ResetExprContext, ResetPerTupleExprContext,
    GetPerTupleExprContext, GetPerTupleMemoryContext, ExecGetUpdatedCols,
};
use crate::executor::executor::{
    ExecBuildProjectionInfo, ExecBuildUpdateProjection,
    ExecInitResultTupleSlotTL, ExecInitResultTypeTL,
    ExecInitQual, ExecPrepareExpr, ExecEvalExpr, ExecProject, ExecQual,
    EvalPlanQual, EvalPlanQualInit, EvalPlanQualSetPlan, EvalPlanQualSlot,
    EvalPlanQualSetSlot, EvalPlanQualBegin, EvalPlanQualEnd,
    ExecInitNode, ExecEndNode,
    CheckValidResultRel, ExecConstraints, ExecPartitionCheck, ExecPartitionCheckEmitError,
    ExecWithCheckOptions, ExecUpdateLockMode, ExecFindRowMark, ExecBuildAuxRowMark,
    exec_rt_fetch, ExecProcNode,
};
use crate::executor::execProcnode::ExecInitNode as ExecInitNode2;
use crate::executor::execJunk::{ExecFindJunkAttributeInTlist, ExecGetJunkAttribute};
use crate::executor::execTuples::{
    MakeSingleTupleTableSlot, ExecDropSingleTupleTableSlot,
    ExecForceStoreHeapTuple, ExecStoreVirtualTuple, ExecStoreAllNullTuple,
};
use crate::executor::tuptable::{
    TupIsNull, TTS_EMPTY, ExecCopySlot, ExecClearTuple, ExecMaterializeSlot,
    slot_getallattrs, slot_getsysattr,
};
// ExecStoreVirtualTuple / ExecStoreAllNullTuple are non-inline C fns not yet ported;
// references in this file are inside #[allow(unreachable_code)] bodies so they compile.
use crate::access::common::tupdesc::CreateTupleDescCopy;
use crate::executor::instrument::{Instrumentation, InstrUpdateTupleCount};
use crate::miscadmin::{CHECK_FOR_INTERRUPTS, IsBootstrapProcessingMode};
use crate::utils::adt::ri_triggers::{RI_FKey_trigger_type, RI_TRIGGER_PK};
use crate::catalog::pg_attribute::ATTRIBUTE_GENERATED_STORED;

// FdwRoutine stubs for the vtable fn-ptr calls used in ExecInsert/ExecUpdate/ExecDelete.
// The FdwRoutine already has ExecForeignInsert / ExecForeignUpdate / ExecForeignDelete etc.
// added in execnodes.rs; we just need to name additional ones here.
use crate::nodes::execnodes::FdwRoutine;

/*
 * MTTargetRelLookup
 *
 * Hash table entry for looking up result rels by OID.
 */
#[repr(C)]
struct MTTargetRelLookup {
    relationOid: Oid,     /* hash key, must be first */
    relationIndex: c_int, /* rel's index in resultRelInfo[] array */
}

/*
 * Context struct for a ModifyTable operation, containing basic execution
 * state and some output variables populated by ExecUpdateAct() and
 * ExecDeleteAct() to report the result of their actions to callers.
 */
struct ModifyTableContext {
    /* Operation state */
    mtstate: *mut ModifyTableState,
    epqstate: *mut EPQState,
    estate: *mut EState,

    /*
     * Slot containing tuple obtained from ModifyTable's subplan.  Used to
     * access "junk" columns that are not going to be stored.
     */
    planSlot: *mut TupleTableSlot,

    /*
     * Information about the changes that were made concurrently to a tuple
     * being updated or deleted
     */
    tmfd: TM_FailureData,

    /*
     * The tuple deleted when doing a cross-partition UPDATE with a RETURNING
     * clause that refers to OLD columns (converted to the root's tuple
     * descriptor).
     */
    cpDeletedSlot: *mut TupleTableSlot,

    /*
     * The tuple projected by the INSERT's RETURNING clause, when doing a
     * cross-partition UPDATE
     */
    cpUpdateReturningSlot: *mut TupleTableSlot,
}

/*
 * Context struct containing output data specific to UPDATE operations.
 */
struct UpdateContext {
    crossPartUpdate: bool,        /* was it a cross-partition update? */
    updateIndexes: TU_UpdateIndexes, /* Which index updates are required? */

    /*
     * Lock mode to acquire on the latest tuple version before performing
     * EvalPlanQual on it
     */
    lockmode: LockTupleMode,
}

// ---- local stubs for unported subsystems ----

/* TODO(pg-port): real ExecBRInsertTriggers lives in commands/trigger.c */
unsafe fn ExecBRInsertTriggers(
    _estate: *mut EState,
    _resultRelInfo: *mut ResultRelInfo,
    _slot: *mut TupleTableSlot,
) -> bool {
    unimplemented!("TODO(pg-port): ExecBRInsertTriggers - commands/trigger.c")
}

/* TODO(pg-port): real ExecIRInsertTriggers lives in commands/trigger.c */
unsafe fn ExecIRInsertTriggers(
    _estate: *mut EState,
    _resultRelInfo: *mut ResultRelInfo,
    _slot: *mut TupleTableSlot,
) -> bool {
    unimplemented!("TODO(pg-port): ExecIRInsertTriggers - commands/trigger.c")
}

/* TODO(pg-port): real ExecARInsertTriggers lives in commands/trigger.c */
unsafe fn ExecARInsertTriggers(
    _estate: *mut EState,
    _resultRelInfo: *mut ResultRelInfo,
    _slot: *mut TupleTableSlot,
    _recheckIndexes: *mut List,
    _transition_capture: *mut TransitionCaptureState,
) {
    unimplemented!("TODO(pg-port): ExecARInsertTriggers - commands/trigger.c")
}

/* TODO(pg-port): real ExecBSInsertTriggers lives in commands/trigger.c */
unsafe fn ExecBSInsertTriggers(_estate: *mut EState, _resultRelInfo: *mut ResultRelInfo) {
    unimplemented!("TODO(pg-port): ExecBSInsertTriggers - commands/trigger.c")
}

/* TODO(pg-port): real ExecASInsertTriggers lives in commands/trigger.c */
unsafe fn ExecASInsertTriggers(
    _estate: *mut EState,
    _resultRelInfo: *mut ResultRelInfo,
    _transition_capture: *mut TransitionCaptureState,
) {
    unimplemented!("TODO(pg-port): ExecASInsertTriggers - commands/trigger.c")
}

/* TODO(pg-port): real ExecBRDeleteTriggers lives in commands/trigger.c */
unsafe fn ExecBRDeleteTriggers(
    _estate: *mut EState,
    _epqstate: *mut EPQState,
    _resultRelInfo: *mut ResultRelInfo,
    _tupleid: *mut ItemPointerData,
    _oldtuple: HeapTuple,
    _epqreturnslot: *mut *mut TupleTableSlot,
    _result: *mut TM_Result,
    _tmfd: *mut TM_FailureData,
    _is_merge: bool,
) -> bool {
    unimplemented!("TODO(pg-port): ExecBRDeleteTriggers - commands/trigger.c")
}

/* TODO(pg-port): real ExecIRDeleteTriggers lives in commands/trigger.c */
unsafe fn ExecIRDeleteTriggers(
    _estate: *mut EState,
    _resultRelInfo: *mut ResultRelInfo,
    _oldtuple: HeapTuple,
) -> bool {
    unimplemented!("TODO(pg-port): ExecIRDeleteTriggers - commands/trigger.c")
}

/* TODO(pg-port): real ExecARDeleteTriggers lives in commands/trigger.c */
unsafe fn ExecARDeleteTriggers(
    _estate: *mut EState,
    _resultRelInfo: *mut ResultRelInfo,
    _tupleid: *mut ItemPointerData,
    _oldtuple: HeapTuple,
    _transition_capture: *mut TransitionCaptureState,
    _changingPart: bool,
) {
    unimplemented!("TODO(pg-port): ExecARDeleteTriggers - commands/trigger.c")
}

/* TODO(pg-port): real ExecBSDeleteTriggers lives in commands/trigger.c */
unsafe fn ExecBSDeleteTriggers(_estate: *mut EState, _resultRelInfo: *mut ResultRelInfo) {
    unimplemented!("TODO(pg-port): ExecBSDeleteTriggers - commands/trigger.c")
}

/* TODO(pg-port): real ExecASDeleteTriggers lives in commands/trigger.c */
unsafe fn ExecASDeleteTriggers(
    _estate: *mut EState,
    _resultRelInfo: *mut ResultRelInfo,
    _transition_capture: *mut TransitionCaptureState,
) {
    unimplemented!("TODO(pg-port): ExecASDeleteTriggers - commands/trigger.c")
}

/* TODO(pg-port): real ExecBRUpdateTriggers lives in commands/trigger.c */
unsafe fn ExecBRUpdateTriggers(
    _estate: *mut EState,
    _epqstate: *mut EPQState,
    _resultRelInfo: *mut ResultRelInfo,
    _tupleid: *mut ItemPointerData,
    _oldtuple: HeapTuple,
    _slot: *mut TupleTableSlot,
    _result: *mut TM_Result,
    _tmfd: *mut TM_FailureData,
    _is_merge: bool,
) -> bool {
    unimplemented!("TODO(pg-port): ExecBRUpdateTriggers - commands/trigger.c")
}

/* TODO(pg-port): real ExecIRUpdateTriggers lives in commands/trigger.c */
unsafe fn ExecIRUpdateTriggers(
    _estate: *mut EState,
    _resultRelInfo: *mut ResultRelInfo,
    _oldtuple: HeapTuple,
    _slot: *mut TupleTableSlot,
) -> bool {
    unimplemented!("TODO(pg-port): ExecIRUpdateTriggers - commands/trigger.c")
}

/* TODO(pg-port): real ExecARUpdateTriggers lives in commands/trigger.c */
unsafe fn ExecARUpdateTriggers(
    _estate: *mut EState,
    _resultRelInfo: *mut ResultRelInfo,
    _src_partinfo: *mut ResultRelInfo,
    _dst_partinfo: *mut ResultRelInfo,
    _tupleid: *mut ItemPointerData,
    _oldtuple: HeapTuple,
    _slot: *mut TupleTableSlot,
    _recheckIndexes: *mut List,
    _transition_capture: *mut TransitionCaptureState,
    _is_crosspart: bool,
) {
    unimplemented!("TODO(pg-port): ExecARUpdateTriggers - commands/trigger.c")
}

/* TODO(pg-port): real ExecBSUpdateTriggers lives in commands/trigger.c */
unsafe fn ExecBSUpdateTriggers(_estate: *mut EState, _resultRelInfo: *mut ResultRelInfo) {
    unimplemented!("TODO(pg-port): ExecBSUpdateTriggers - commands/trigger.c")
}

/* TODO(pg-port): real ExecASUpdateTriggers lives in commands/trigger.c */
unsafe fn ExecASUpdateTriggers(
    _estate: *mut EState,
    _resultRelInfo: *mut ResultRelInfo,
    _transition_capture: *mut TransitionCaptureState,
) {
    unimplemented!("TODO(pg-port): ExecASUpdateTriggers - commands/trigger.c")
}

/* TODO(pg-port): real MakeTransitionCaptureState lives in commands/trigger.c */
unsafe fn MakeTransitionCaptureState(
    _trigdesc: *mut crate::nodes::execnodes::TriggerDesc,
    _relid: Oid,
    _cmdtype: CmdType,
) -> *mut TransitionCaptureState {
    unimplemented!("TODO(pg-port): MakeTransitionCaptureState - commands/trigger.c")
}

/* TODO(pg-port): real ExecSetupPartitionTupleRouting lives in executor/execPartition.c */
unsafe fn ExecSetupPartitionTupleRouting(
    _estate: *mut EState,
    _rel: Relation,
) -> *mut PartitionTupleRouting {
    unimplemented!("TODO(pg-port): ExecSetupPartitionTupleRouting - executor/execPartition.c")
}

/* TODO(pg-port): real ExecFindPartition lives in executor/execPartition.c */
unsafe fn ExecFindPartition(
    _mtstate: *mut ModifyTableState,
    _targetRelInfo: *mut ResultRelInfo,
    _proute: *mut PartitionTupleRouting,
    _slot: *mut TupleTableSlot,
    _estate: *mut EState,
) -> *mut ResultRelInfo {
    unimplemented!("TODO(pg-port): ExecFindPartition - executor/execPartition.c")
}

/* TODO(pg-port): real ExecCleanupTupleRouting lives in executor/execPartition.c */
unsafe fn ExecCleanupTupleRouting(
    _mtstate: *mut ModifyTableState,
    _proute: *mut PartitionTupleRouting,
) {
    unimplemented!("TODO(pg-port): ExecCleanupTupleRouting - executor/execPartition.c")
}

/* TODO(pg-port): real ExecInitPartitionInfo lives in executor/execPartition.c */
unsafe fn ExecInitMergeTupleSlots_partition(
    _mtstate: *mut ModifyTableState,
    _resultRelInfo: *mut ResultRelInfo,
) {
    unimplemented!("TODO(pg-port): ExecInitPartitionInfo - executor/execPartition.c")
}

/* TODO(pg-port): real ExecGetAncestorResultRels lives in executor/execUtils.c */
unsafe fn ExecGetAncestorResultRels(
    _estate: *mut EState,
    _resultRelInfo: *mut ResultRelInfo,
) -> *mut List {
    unimplemented!("TODO(pg-port): ExecGetAncestorResultRels - executor/execUtils.c")
}

/* TODO(pg-port): real build_column_default lives in catalog/catalog.c */
unsafe fn build_column_default(_rel: Relation, _attrno: c_int) -> *mut Node {
    unimplemented!("TODO(pg-port): build_column_default - catalog/catalog.c")
}

/* TODO(pg-port): real IsolationUsesXactSnapshot lives in access/transam/xact.c */
unsafe fn IsolationUsesXactSnapshot() -> bool {
    unimplemented!("TODO(pg-port): IsolationUsesXactSnapshot - access/transam/xact.c")
}

/* TODO(pg-port): real GetCurrentTransactionId lives in access/transam/xact.c */
unsafe fn GetCurrentTransactionId() -> TransactionId {
    unimplemented!("TODO(pg-port): GetCurrentTransactionId - access/transam/xact.c")
}

/* TODO(pg-port): real TransactionIdIsCurrentTransactionId lives in access/transam/xact.c */
unsafe fn TransactionIdIsCurrentTransactionId(_xid: TransactionId) -> bool {
    unimplemented!("TODO(pg-port): TransactionIdIsCurrentTransactionId - access/transam/xact.c")
}

/* TODO(pg-port): real table_tuple_lock lives in access/table/tableam.c */
unsafe fn table_tuple_lock(
    _rel: Relation,
    _tid: *mut ItemPointerData,
    _snapshot: crate::nodes::execnodes::Snapshot,
    _slot: *mut TupleTableSlot,
    _cid: CommandId,
    _mode: LockTupleMode,
    _wait_policy: LockWaitPolicy,
    _flags: c_int,
    _tmfd: *mut TM_FailureData,
) -> TM_Result {
    unimplemented!("TODO(pg-port): table_tuple_lock - access/table/tableam.c")
}

/* TODO(pg-port): real table_tuple_fetch_row_version lives in access/table/tableam.c */
unsafe fn table_tuple_fetch_row_version(
    _rel: Relation,
    _tid: *mut ItemPointerData,
    _snapshot: crate::nodes::execnodes::Snapshot,
    _slot: *mut TupleTableSlot,
) -> bool {
    unimplemented!("TODO(pg-port): table_tuple_fetch_row_version - access/table/tableam.c")
}

/* TODO(pg-port): real table_tuple_satisfies_snapshot lives in access/table/tableam.c */
unsafe fn table_tuple_satisfies_snapshot(
    _rel: Relation,
    _slot: *mut TupleTableSlot,
    _snapshot: crate::nodes::execnodes::Snapshot,
) -> bool {
    unimplemented!("TODO(pg-port): table_tuple_satisfies_snapshot - access/table/tableam.c")
}

/* TODO(pg-port): real table_tuple_insert_speculative lives in access/table/tableam.c */
unsafe fn table_tuple_insert_speculative(
    _rel: Relation,
    _slot: *mut TupleTableSlot,
    _cid: CommandId,
    _options: c_int,
    _bistate: *mut c_void,
    _specToken: uint32,
) {
    unimplemented!("TODO(pg-port): table_tuple_insert_speculative - access/table/tableam.c")
}

/* TODO(pg-port): real table_tuple_complete_speculative lives in access/table/tableam.c */
unsafe fn table_tuple_complete_speculative(
    _rel: Relation,
    _slot: *mut TupleTableSlot,
    _specToken: uint32,
    _succeeded: bool,
) {
    unimplemented!("TODO(pg-port): table_tuple_complete_speculative - access/table/tableam.c")
}

/* TODO(pg-port): real SnapshotAny is a global Snapshot in utils/snapmgr.c */
unsafe fn SnapshotAny_ptr() -> crate::nodes::execnodes::Snapshot {
    unimplemented!("TODO(pg-port): SnapshotAny - utils/snapmgr.c")
}

/* TODO(pg-port): real DatumGetHeapTupleHeader lives in include/access/htup.h */
unsafe fn DatumGetHeapTupleHeader(d: Datum) -> *mut HeapTupleHeaderData {
    d as *mut HeapTupleHeaderData
}

/* TODO(pg-port): real ItemPointerIndicatesMovedPartitions lives in storage/itemptr.h */
unsafe fn ItemPointerIndicatesMovedPartitions(_ptr: *const ItemPointerData) -> bool {
    false // TODO(pg-port): storage/itemptr.h
}

/* TODO(pg-port): list helpers used in this file; real versions in nodes/list.c */
unsafe fn list_nth(list: *mut List, n: c_int) -> *mut c_void {
    unimplemented!("TODO(pg-port): list_nth - nodes/list.c")
}

unsafe fn list_nth_node_list(list: *mut List, n: c_int) -> *mut List {
    list_nth(list, n) as *mut List
}

unsafe fn linitial(list: *mut List) -> *mut c_void {
    unimplemented!("TODO(pg-port): linitial - nodes/list.c")
}

unsafe fn linitial_int(list: *mut List) -> c_int {
    unimplemented!("TODO(pg-port): linitial_int - nodes/list.c")
}

unsafe fn lappend(list: *mut List, datum: *mut c_void) -> *mut List {
    unimplemented!("TODO(pg-port): lappend - nodes/list.c")
}

unsafe fn lappend_int(list: *mut List, datum: c_int) -> *mut List {
    unimplemented!("TODO(pg-port): lappend_int - nodes/list.c")
}

unsafe fn lcons(datum: *mut c_void, list: *mut List) -> *mut List {
    unimplemented!("TODO(pg-port): lcons - nodes/list.c")
}

unsafe fn list_free(list: *mut List) {
    unimplemented!("TODO(pg-port): list_free - nodes/list.c")
}

unsafe fn list_length(list: *mut List) -> c_int {
    unimplemented!("TODO(pg-port): list_length - nodes/list.c")
}

unsafe fn list_member_ptr(list: *mut List, datum: *mut c_void) -> bool {
    unimplemented!("TODO(pg-port): list_member_ptr - nodes/list.c")
}

unsafe fn lfirst(lc: *mut crate::nodes::pg_list::ListCell) -> *mut c_void {
    unimplemented!("TODO(pg-port): lfirst - nodes/list.c")
}

unsafe fn lfirst_int(lc: *mut crate::nodes::pg_list::ListCell) -> c_int {
    unimplemented!("TODO(pg-port): lfirst_int - nodes/list.c")
}

unsafe fn lfirst_node_WithCheckOption(
    lc: *mut crate::nodes::pg_list::ListCell,
) -> *mut WithCheckOption {
    unimplemented!("TODO(pg-port): lfirst_node(WithCheckOption,...) - nodes/list.c")
}

/* TODO(pg-port): foreach/forboth macros - approximated with raw pointer iteration */
/* We use raw iteration patterns inline where these would appear */

/* NIL is the null list pointer */
const NIL: *mut List = core::ptr::null_mut();

/* TODO(pg-port): InvalidOid */
const InvalidOid: Oid = 0;

/* TODO(pg-port): EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK, EXEC_FLAG_EXPLAIN_ONLY */
const EXEC_FLAG_BACKWARD: c_int = 0x0001;
const EXEC_FLAG_MARK: c_int = 0x0002;
const EXEC_FLAG_EXPLAIN_ONLY: c_int = 0x0008;

/* OnConflictAction variants now imported at top via nodes::nodes::OnConflictAction::* */

/* TTSOpsVirtual - real def lives in executor/execTuples.rs */
use crate::executor::execTuples::TTSOpsVirtual;

/* TODO(pg-port): InstrCountTuples2 / InstrCountFiltered1 as local stubs */
unsafe fn InstrCountTuples2(node: *mut crate::nodes::execnodes::PlanState, _n: f64) {
    // TODO(pg-port): real InstrCountTuples2 - executor/instrument.h
}

unsafe fn InstrCountFiltered1(node: *mut crate::nodes::execnodes::PlanState, _n: f64) {
    // TODO(pg-port): real InstrCountFiltered1 - executor/instrument.h
}

use crate::nodes::execnodes::PlanState;

unsafe fn outerPlanState(node: *mut PlanState) -> *mut PlanState {
    crate::nodes::execnodes::outerPlanState(node)
}

/// Write to the outer-plan-state slot (C lvalue: outerPlanState(node) = val).
unsafe fn outerPlanState_mut(node: *mut PlanState) -> *mut *mut PlanState {
    &raw mut (*node).lefttree
}

unsafe fn outerPlan(node: *mut Plan) -> *mut Plan {
    crate::nodes::plannodes::outerPlan(node)
}

/*
 * Verify that the tuples to be produced by INSERT match the
 * target relation's rowtype
 *
 * We do this to guard against stale plans.  If plan invalidation is
 * functioning properly then we should never get a failure here, but better
 * safe than sorry.  Note that this is called after we have obtained lock
 * on the target rel, so the rowtype can't change underneath us.
 *
 * The plan output is represented by its targetlist, because that makes
 * handling the dropped-column case easier.
 *
 * We used to use this for UPDATE as well, but now the equivalent checks
 * are done in ExecBuildUpdateProjection.
 */
unsafe fn ExecCheckPlanOutput(resultRel: Relation, targetList: *mut List) {
    let resultDesc: TupleDesc = RelationGetDescr(resultRel);
    let mut attno: c_int = 0;
    // TODO(pg-port): foreach(lc, targetList) - nodes/list.c
    // Stub: iterate over targetList entries
    let natts: c_int = (*resultDesc).natts;
    // We check attno != natts at end; full iteration needs list infrastructure.
    // For now, implement the structure faithfully and unimpl the loop body.
    #[allow(unreachable_code)]
    {
        // foreach(lc, targetList)
        unimplemented!("TODO(pg-port): ExecCheckPlanOutput - needs list iteration (nodes/list.c)");
        // TargetEntry iteration would go here; body below is translated faithfully:
        /*
        let tle = lfirst(lc) as *mut TargetEntry;
        assert!(!(*tle).resjunk);
        if attno >= natts {
            ereport!(ERROR, errmsg!("table row type and query-specified row type do not match")) /* C also: errdetail */;
        }
        let attr = TupleDescAttr(resultDesc, attno);
        attno += 1;
        if (*attr).attisdropped {
            if !IsA!((*tle).expr, Const) || !(*((*tle).expr as *mut Const)).constisnull {
                ereport!(ERROR, ...);
            }
        } else if (*attr).attgenerated != 0 {
            ...
        } else {
            if exprType((*tle).expr as *const Node) != (*attr).atttypid {
                ereport!(ERROR, ...);
            }
        }
        */
    }
    if attno != natts {
        ereport!(ERROR, errmsg!("table row type and query-specified row type do not match")) /* C also: errdetail */;
    }
}

/*
 * ExecProcessReturning --- evaluate a RETURNING list
 *
 * context: context for the ModifyTable operation
 * resultRelInfo: current result rel
 * cmdType: operation/merge action performed (INSERT, UPDATE, or DELETE)
 * oldSlot: slot holding old tuple deleted or updated
 * newSlot: slot holding new tuple inserted or updated
 * planSlot: slot holding tuple returned by top subplan node
 *
 * Note: If oldSlot and newSlot are NULL, the FDW should have already provided
 * econtext's scan tuple and its old & new tuples are not needed (FDW direct-
 * modify is disabled if the RETURNING list refers to any OLD/NEW values).
 *
 * Returns a slot holding the result tuple
 */
unsafe fn ExecProcessReturning(
    context: *mut ModifyTableContext,
    resultRelInfo: *mut ResultRelInfo,
    cmdType: CmdType,
    oldSlot: *mut TupleTableSlot,
    newSlot: *mut TupleTableSlot,
    planSlot: *mut TupleTableSlot,
) -> *mut TupleTableSlot {
    let estate: *mut EState = (*context).estate;
    let projectReturning: *mut ProjectionInfo = (*resultRelInfo).ri_projectReturning;
    let econtext: *mut ExprContext = (*projectReturning).pi_exprContext;

    /* Make tuple and any needed join variables available to ExecProject */
    match cmdType {
        CMD_INSERT | CMD_UPDATE => {
            /* return new tuple by default */
            if !newSlot.is_null() {
                (*econtext).ecxt_scantuple = newSlot;
            }
        }
        CMD_DELETE => {
            /* return old tuple by default */
            if !oldSlot.is_null() {
                (*econtext).ecxt_scantuple = oldSlot;
            }
        }
        _ => {
            elog!(ERROR, "unrecognized commandType: {}", cmdType as c_int);
        }
    }
    (*econtext).ecxt_outertuple = planSlot;

    /* Make old/new tuples available to ExecProject, if required */
    if !oldSlot.is_null() {
        (*econtext).ecxt_oldtuple = oldSlot;
    } else if (*projectReturning).pi_state.flags & EEO_FLAG_HAS_OLD != 0 {
        (*econtext).ecxt_oldtuple = ExecGetAllNullSlot(estate, resultRelInfo);
    } else {
        (*econtext).ecxt_oldtuple = core::ptr::null_mut(); /* No references to OLD columns */
    }

    if !newSlot.is_null() {
        (*econtext).ecxt_newtuple = newSlot;
    } else if (*projectReturning).pi_state.flags & EEO_FLAG_HAS_NEW != 0 {
        (*econtext).ecxt_newtuple = ExecGetAllNullSlot(estate, resultRelInfo);
    } else {
        (*econtext).ecxt_newtuple = core::ptr::null_mut(); /* No references to NEW columns */
    }

    /*
     * Tell ExecProject whether or not the OLD/NEW rows actually exist.  This
     * information is required to evaluate ReturningExpr nodes and also in
     * ExecEvalSysVar() and ExecEvalWholeRowVar().
     */
    if oldSlot.is_null() {
        (*projectReturning).pi_state.flags |= EEO_FLAG_OLD_IS_NULL;
    } else {
        (*projectReturning).pi_state.flags &= !EEO_FLAG_OLD_IS_NULL;
    }

    if newSlot.is_null() {
        (*projectReturning).pi_state.flags |= EEO_FLAG_NEW_IS_NULL;
    } else {
        (*projectReturning).pi_state.flags &= !EEO_FLAG_NEW_IS_NULL;
    }

    /* Compute the RETURNING expressions */
    ExecProject(projectReturning)
}

/*
 * ExecCheckTupleVisible -- verify tuple is visible
 *
 * It would not be consistent with guarantees of the higher isolation levels to
 * proceed with avoiding insertion (taking speculative insertion's alternative
 * path) on the basis of another tuple that is not visible to MVCC snapshot.
 * Check for the need to raise a serialization failure, and do so as necessary.
 */
unsafe fn ExecCheckTupleVisible(
    estate: *mut EState,
    rel: Relation,
    slot: *mut TupleTableSlot,
) {
    if !IsolationUsesXactSnapshot() {
        return;
    }

    if !table_tuple_satisfies_snapshot(rel, slot, (*estate).es_snapshot) {
        let mut isnull: bool = false;
        let xminDatum: Datum = slot_getsysattr(slot, MinTransactionIdAttributeNumber as c_int, &mut isnull);
        debug_assert!(!isnull);
        let xmin: TransactionId = crate::postgres::DatumGetTransactionId(xminDatum);

        /*
         * We should not raise a serialization failure if the conflict is
         * against a tuple inserted by our own transaction, even if it's not
         * visible to our snapshot.  (This would happen, for example, if
         * conflicting keys are proposed for insertion in a single command.)
         */
        if !TransactionIdIsCurrentTransactionId(xmin) {
            ereport!(ERROR, errmsg!("could not serialize access due to concurrent update"));
        }
    }
}

/*
 * ExecCheckTIDVisible -- convenience variant of ExecCheckTupleVisible()
 */
unsafe fn ExecCheckTIDVisible(
    estate: *mut EState,
    relinfo: *mut ResultRelInfo,
    tid: *mut ItemPointerData,
    tempSlot: *mut TupleTableSlot,
) {
    let rel: Relation = (*relinfo).ri_RelationDesc;

    /* Redundantly check isolation level */
    if !IsolationUsesXactSnapshot() {
        return;
    }

    if !table_tuple_fetch_row_version(rel, tid, SnapshotAny_ptr(), tempSlot) {
        elog!(ERROR, "failed to fetch conflicting tuple for ON CONFLICT");
    }
    ExecCheckTupleVisible(estate, rel, tempSlot);
    ExecClearTuple(tempSlot);
}

/*
 * Initialize generated columns handling for a tuple
 *
 * This fills the resultRelInfo's ri_GeneratedExprsI/ri_NumGeneratedNeededI or
 * ri_GeneratedExprsU/ri_NumGeneratedNeededU fields, depending on cmdtype.
 * This is used only for stored generated columns.
 *
 * If cmdType == CMD_UPDATE, the ri_extraUpdatedCols field is filled too.
 * This is used by both stored and virtual generated columns.
 *
 * Note: usually, a given query would need only one of ri_GeneratedExprsI and
 * ri_GeneratedExprsU per result rel; but MERGE can need both, and so can
 * cross-partition UPDATEs, since a partition might be the target of both
 * UPDATE and INSERT actions.
 */
pub unsafe fn ExecInitGenerated(
    resultRelInfo: *mut ResultRelInfo,
    estate: *mut EState,
    cmdtype: CmdType,
) {
    let rel: Relation = (*resultRelInfo).ri_RelationDesc;
    let tupdesc: TupleDesc = RelationGetDescr(rel);
    let natts: c_int = (*tupdesc).natts;
    let mut ri_NumGeneratedNeeded: c_int = 0;
    let updatedCols: *mut Bitmapset;
    let oldContext: MemoryContext;

    /* Nothing to do if no generated columns */
    if !((*tupdesc).constr != core::ptr::null_mut()
        && ((*(*tupdesc).constr).has_generated_stored || (*(*tupdesc).constr).has_generated_virtual))
    {
        return;
    }

    /*
     * In an UPDATE, we can skip computing any generated columns that do not
     * depend on any UPDATE target column.  But if there is a BEFORE ROW
     * UPDATE trigger, we cannot skip because the trigger might change more
     * columns.
     */
    if cmdtype == CMD_UPDATE
        && !((*rel).trigdesc != core::ptr::null_mut()
            && (*((*rel).trigdesc as *mut crate::nodes::execnodes::TriggerDesc)).trig_update_before_row)
    {
        updatedCols = ExecGetUpdatedCols(resultRelInfo, estate);
    } else {
        updatedCols = core::ptr::null_mut();
    }

    /*
     * Make sure these data structures are built in the per-query memory
     * context so they'll survive throughout the query.
     */
    oldContext = MemoryContextSwitchTo((*estate).es_query_cxt);

    let ri_GeneratedExprs: *mut *mut ExprState =
        palloc0(natts as usize * core::mem::size_of::<*mut ExprState>()) as *mut *mut ExprState;

    for i in 0..natts as usize {
        let attgenerated: c_char = (*TupleDescAttr(tupdesc, i as c_int)).attgenerated;

        if attgenerated != 0 {
            let expr: *mut Expr;

            /* Fetch the GENERATED AS expression tree */
            expr = build_column_default(rel, i as c_int + 1) as *mut Expr;
            if expr.is_null() {
                elog!(ERROR,
                    "no generation expression found for column number {} of table \"{}\"",
                    i + 1,
                    crate::utils::rel::RelationGetRelationName(rel) as usize);
            }

            /*
             * If it's an update with a known set of update target columns,
             * see if we can skip the computation.
             */
            if !updatedCols.is_null() {
                let mut attrs_used: *mut Bitmapset = core::ptr::null_mut();

                pull_varattnos(expr as *mut Node, 1, &mut attrs_used);

                if !bms_overlap(updatedCols, attrs_used) {
                    continue; /* need not update this column */
                }
            }

            /* No luck, so prepare the expression for execution */
            if attgenerated == ATTRIBUTE_GENERATED_STORED {
                *ri_GeneratedExprs.add(i) = ExecPrepareExpr(expr, estate);
                ri_NumGeneratedNeeded += 1;
            }

            /* If UPDATE, mark column in resultRelInfo->ri_extraUpdatedCols */
            if cmdtype == CMD_UPDATE {
                (*resultRelInfo).ri_extraUpdatedCols =
                    bms_add_member(
                        (*resultRelInfo).ri_extraUpdatedCols,
                        i as c_int + 1 - FirstLowInvalidHeapAttributeNumber as c_int,
                    );
            }
        }
    }

    if ri_NumGeneratedNeeded == 0 {
        /* didn't need it after all */
        pfree(ri_GeneratedExprs as *mut c_void);
        let ri_GeneratedExprs: *mut *mut ExprState = core::ptr::null_mut();

        /* Save in appropriate set of fields */
        if cmdtype == CMD_UPDATE {
            debug_assert!((*resultRelInfo).ri_GeneratedExprsU.is_null());
            (*resultRelInfo).ri_GeneratedExprsU = core::ptr::null_mut();
            (*resultRelInfo).ri_NumGeneratedNeededU = 0;
            (*resultRelInfo).ri_extraUpdatedCols_valid = true;
        } else {
            debug_assert!((*resultRelInfo).ri_GeneratedExprsI.is_null());
            (*resultRelInfo).ri_GeneratedExprsI = core::ptr::null_mut();
            (*resultRelInfo).ri_NumGeneratedNeededI = 0;
        }

        MemoryContextSwitchTo(oldContext);
        return;
    }

    /* Save in appropriate set of fields */
    if cmdtype == CMD_UPDATE {
        /* Don't call twice */
        debug_assert!((*resultRelInfo).ri_GeneratedExprsU.is_null());

        (*resultRelInfo).ri_GeneratedExprsU = ri_GeneratedExprs;
        (*resultRelInfo).ri_NumGeneratedNeededU = ri_NumGeneratedNeeded;

        (*resultRelInfo).ri_extraUpdatedCols_valid = true;
    } else {
        /* Don't call twice */
        debug_assert!((*resultRelInfo).ri_GeneratedExprsI.is_null());

        (*resultRelInfo).ri_GeneratedExprsI = ri_GeneratedExprs;
        (*resultRelInfo).ri_NumGeneratedNeededI = ri_NumGeneratedNeeded;
    }

    MemoryContextSwitchTo(oldContext);
}

/*
 * Compute stored generated columns for a tuple
 */
pub unsafe fn ExecComputeStoredGenerated(
    resultRelInfo: *mut ResultRelInfo,
    estate: *mut EState,
    slot: *mut TupleTableSlot,
    cmdtype: CmdType,
) {
    let rel: Relation = (*resultRelInfo).ri_RelationDesc;
    let tupdesc: TupleDesc = RelationGetDescr(rel);
    let natts: c_int = (*tupdesc).natts;
    let econtext: *mut ExprContext = GetPerTupleExprContext(estate);
    let ri_GeneratedExprs: *mut *mut ExprState;
    let oldContext: MemoryContext;

    /* We should not be called unless this is true */
    debug_assert!(
        (*tupdesc).constr != core::ptr::null_mut()
            && (*(*tupdesc).constr).has_generated_stored
    );

    /*
     * Initialize the expressions if we didn't already, and check whether we
     * can exit early because nothing needs to be computed.
     */
    if cmdtype == CMD_UPDATE {
        if (*resultRelInfo).ri_GeneratedExprsU.is_null() {
            ExecInitGenerated(resultRelInfo, estate, cmdtype);
        }
        if (*resultRelInfo).ri_NumGeneratedNeededU == 0 {
            return;
        }
        ri_GeneratedExprs = (*resultRelInfo).ri_GeneratedExprsU;
    } else {
        if (*resultRelInfo).ri_GeneratedExprsI.is_null() {
            ExecInitGenerated(resultRelInfo, estate, cmdtype);
        }
        /* Early exit is impossible given the prior Assert */
        debug_assert!((*resultRelInfo).ri_NumGeneratedNeededI > 0);
        ri_GeneratedExprs = (*resultRelInfo).ri_GeneratedExprsI;
    }

    oldContext = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

    let values: *mut Datum =
        palloc(core::mem::size_of::<Datum>() * natts as usize) as *mut Datum;
    let nulls: *mut bool =
        palloc(core::mem::size_of::<bool>() * natts as usize) as *mut bool;

    slot_getallattrs(slot);
    core::ptr::copy_nonoverlapping(
        (*slot).tts_isnull,
        nulls,
        natts as usize,
    );

    for i in 0..natts as usize {
        let attr: *mut crate::access::common::tupdesc::CompactAttribute =
            TupleDescCompactAttr(tupdesc, i as c_int);

        if !(*ri_GeneratedExprs.add(i)).is_null() {
            let mut isnull: bool = false;

            debug_assert!(
                (*TupleDescAttr(tupdesc, i as c_int)).attgenerated
                    == ATTRIBUTE_GENERATED_STORED
            );

            (*econtext).ecxt_scantuple = slot;

            let mut val: Datum =
                ExecEvalExpr(*ri_GeneratedExprs.add(i), econtext, &mut isnull);

            /*
             * We must make a copy of val as we have no guarantees about where
             * memory for a pass-by-reference Datum is located.
             */
            if !isnull {
                val = datumCopy(val, (*attr).attbyval, (*attr).attlen as c_int);
            }

            *values.add(i) = val;
            *nulls.add(i) = isnull;
        } else {
            if !*nulls.add(i) {
                *values.add(i) = datumCopy(
                    *(*slot).tts_values.add(i),
                    (*attr).attbyval,
                    (*attr).attlen as c_int,
                );
            }
        }
    }

    ExecClearTuple(slot);
    core::ptr::copy_nonoverlapping(values, (*slot).tts_values, natts as usize);
    core::ptr::copy_nonoverlapping(nulls, (*slot).tts_isnull, natts as usize);
    ExecStoreVirtualTuple(slot);
    ExecMaterializeSlot(slot);

    MemoryContextSwitchTo(oldContext);
}

/*
 * ExecInitInsertProjection
 *      Do one-time initialization of projection data for INSERT tuples.
 *
 * INSERT queries may need a projection to filter out junk attrs in the tlist.
 *
 * This is also a convenient place to verify that the
 * output of an INSERT matches the target table.
 */
unsafe fn ExecInitInsertProjection(
    mtstate: *mut ModifyTableState,
    resultRelInfo: *mut ResultRelInfo,
) {
    let node: *mut ModifyTable = (*mtstate).ps.plan as *mut ModifyTable;
    let subplan: *mut Plan = outerPlan(node as *mut Plan);
    let estate: *mut EState = (*mtstate).ps.state;
    let mut insertTargetList: *mut List = NIL;
    let mut need_projection: bool = false;
    /* Extract non-junk columns of the subplan's result tlist. */
    /* TODO(pg-port): foreach(l, subplan->targetlist) - needs list iteration */
    // The full body relies on list iteration; stub the structure here.
    #[allow(unreachable_code)]
    {
        unimplemented!("TODO(pg-port): ExecInitInsertProjection - needs list iteration (nodes/list.c)");
    }

    /*
     * The junk-free list must produce a tuple suitable for the result
     * relation.
     */
    ExecCheckPlanOutput((*resultRelInfo).ri_RelationDesc, insertTargetList);

    /* We'll need a slot matching the table's format. */
    (*resultRelInfo).ri_newTupleSlot =
        table_slot_create((*resultRelInfo).ri_RelationDesc, &mut (*estate).es_tupleTable);

    /* Build ProjectionInfo if needed (it probably isn't). */
    if need_projection {
        let relDesc: TupleDesc = RelationGetDescr((*resultRelInfo).ri_RelationDesc);

        /* need an expression context to do the projection */
        if (*mtstate).ps.ps_ExprContext.is_null() {
            ExecAssignExprContext(estate, &mut (*mtstate).ps);
        }

        (*resultRelInfo).ri_projectNew =
            ExecBuildProjectionInfo(
                insertTargetList,
                (*mtstate).ps.ps_ExprContext,
                (*resultRelInfo).ri_newTupleSlot,
                &mut (*mtstate).ps,
                relDesc,
            );
    }

    (*resultRelInfo).ri_projectNewInfoValid = true;
}

/*
 * ExecInitUpdateProjection
 *      Do one-time initialization of projection data for UPDATE tuples.
 *
 * UPDATE always needs a projection, because (1) there's always some junk
 * attrs, and (2) we may need to merge values of not-updated columns from
 * the old tuple into the final tuple.  In UPDATE, the tuple arriving from
 * the subplan contains only new values for the changed columns, plus row
 * identity info in the junk attrs.
 */
unsafe fn ExecInitUpdateProjection(
    mtstate: *mut ModifyTableState,
    resultRelInfo: *mut ResultRelInfo,
) {
    let node: *mut ModifyTable = (*mtstate).ps.plan as *mut ModifyTable;
    let subplan: *mut Plan = outerPlan(node as *mut Plan);
    let estate: *mut EState = (*mtstate).ps.state;
    let relDesc: TupleDesc = RelationGetDescr((*resultRelInfo).ri_RelationDesc);
    let whichrel: c_int;
    let updateColnos: *mut List;

    /*
     * Usually, mt_lastResultIndex matches the target rel.  If it happens not
     * to, we can get the index the hard way with an integer division.
     */
    whichrel = (*mtstate).mt_lastResultIndex;
    let whichrel = if resultRelInfo != (*mtstate).resultRelInfo.add(whichrel as usize) {
        (resultRelInfo as usize - (*mtstate).resultRelInfo as usize)
            / core::mem::size_of::<ResultRelInfo>()
    } else {
        whichrel as usize
    };
    debug_assert!(whichrel < (*mtstate).mt_nrels as usize);

    updateColnos = list_nth((*mtstate).mt_updateColnosLists, whichrel as c_int) as *mut List;

    /*
     * For UPDATE, we use the old tuple to fill up missing values in the tuple
     * produced by the subplan to get the new tuple.  We need two slots, both
     * matching the table's desired format.
     */
    (*resultRelInfo).ri_oldTupleSlot =
        table_slot_create((*resultRelInfo).ri_RelationDesc, &mut (*estate).es_tupleTable);
    (*resultRelInfo).ri_newTupleSlot =
        table_slot_create((*resultRelInfo).ri_RelationDesc, &mut (*estate).es_tupleTable);

    /* need an expression context to do the projection */
    if (*mtstate).ps.ps_ExprContext.is_null() {
        ExecAssignExprContext(estate, &mut (*mtstate).ps);
    }

    (*resultRelInfo).ri_projectNew =
        ExecBuildUpdateProjection(
            (*subplan).targetlist,
            false, /* subplan did the evaluation */
            updateColnos,
            relDesc,
            (*mtstate).ps.ps_ExprContext,
            (*resultRelInfo).ri_newTupleSlot,
            &mut (*mtstate).ps,
        );

    (*resultRelInfo).ri_projectNewInfoValid = true;
}

/*
 * ExecGetInsertNewTuple
 *      This prepares a "new" tuple ready to be inserted into given result
 *      relation, by removing any junk columns of the plan's output tuple
 *      and (if necessary) coercing the tuple to the right tuple format.
 */
unsafe fn ExecGetInsertNewTuple(
    relinfo: *mut ResultRelInfo,
    planSlot: *mut TupleTableSlot,
) -> *mut TupleTableSlot {
    let newProj: *mut ProjectionInfo = (*relinfo).ri_projectNew;
    let econtext: *mut ExprContext;

    /*
     * If there's no projection to be done, just make sure the slot is of the
     * right type for the target rel.  If the planSlot is the right type we
     * can use it as-is, else copy the data into ri_newTupleSlot.
     */
    if newProj.is_null() {
        if (*(*relinfo).ri_newTupleSlot).tts_ops != (*planSlot).tts_ops {
            ExecCopySlot((*relinfo).ri_newTupleSlot, planSlot);
            return (*relinfo).ri_newTupleSlot;
        } else {
            return planSlot;
        }
    }

    /*
     * Else project; since the projection output slot is ri_newTupleSlot, this
     * will also fix any slot-type problem.
     *
     * Note: currently, this is dead code, because INSERT cases don't receive
     * any junk columns so there's never a projection to be done.
     */
    econtext = (*newProj).pi_exprContext;
    (*econtext).ecxt_outertuple = planSlot;
    ExecProject(newProj)
}

/*
 * ExecGetUpdateNewTuple
 *      This prepares a "new" tuple by combining an UPDATE subplan's output
 *      tuple (which contains values of changed columns) with unchanged
 *      columns taken from the old tuple.
 *
 * The subplan tuple might also contain junk columns, which are ignored.
 * Note that the projection also ensures we have a slot of the right type.
 */
pub unsafe fn ExecGetUpdateNewTuple(
    relinfo: *mut ResultRelInfo,
    planSlot: *mut TupleTableSlot,
    oldSlot: *mut TupleTableSlot,
) -> *mut TupleTableSlot {
    let newProj: *mut ProjectionInfo = (*relinfo).ri_projectNew;
    let econtext: *mut ExprContext;

    /* Use a few extra Asserts to protect against outside callers */
    debug_assert!((*relinfo).ri_projectNewInfoValid);
    debug_assert!(!planSlot.is_null() && !TTS_EMPTY(planSlot));
    debug_assert!(!oldSlot.is_null() && !TTS_EMPTY(oldSlot));

    econtext = (*newProj).pi_exprContext;
    (*econtext).ecxt_outertuple = planSlot;
    (*econtext).ecxt_scantuple = oldSlot;
    ExecProject(newProj)
}

/* ----------------------------------------------------------------
 *      ExecInsert
 *
 *      For INSERT, we have to insert the tuple into the target relation
 *      (or partition thereof) and insert appropriate tuples into the index
 *      relations.
 *
 *      slot contains the new tuple value to be stored.
 *
 *      Returns RETURNING result if any, otherwise NULL.
 *      *inserted_tuple is the tuple that's effectively inserted;
 *      *insert_destrel is the relation where it was inserted.
 *      These are only set on success.
 *
 *      This may change the currently active tuple conversion map in
 *      mtstate->mt_transition_capture, so the callers must take care to
 *      save the previous value to avoid losing track of it.
 * ----------------------------------------------------------------
 */
unsafe fn ExecInsert(
    context: *mut ModifyTableContext,
    resultRelInfo: *mut ResultRelInfo,
    mut slot: *mut TupleTableSlot,
    canSetTag: bool,
    inserted_tuple: *mut *mut TupleTableSlot,
    insert_destrel: *mut *mut ResultRelInfo,
) -> *mut TupleTableSlot {
    let mtstate: *mut ModifyTableState = (*context).mtstate;
    let estate: *mut EState = (*context).estate;
    let resultRelationDesc: Relation;
    let mut recheckIndexes: *mut List = NIL;
    let planSlot: *mut TupleTableSlot = (*context).planSlot;
    let mut result: *mut TupleTableSlot = core::ptr::null_mut();
    let ar_insert_trig_tcs: *mut TransitionCaptureState;
    let node: *mut ModifyTable = (*mtstate).ps.plan as *mut ModifyTable;
    let onconflict: OnConflictAction = (*node).onConflictAction;
    let proute: *mut PartitionTupleRouting = (*mtstate).mt_partition_tuple_routing;
    let oldContext: MemoryContext;

    /*
     * If the input result relation is a partitioned table, find the leaf
     * partition to insert the tuple into.
     */
    if !proute.is_null() {
        let mut partRelInfo: *mut ResultRelInfo = core::ptr::null_mut();

        slot = ExecPrepareTupleRouting(mtstate, estate, proute,
                                       resultRelInfo, slot,
                                       &mut partRelInfo);
        let resultRelInfo = partRelInfo;
        return ExecInsert_inner(context, resultRelInfo, slot, canSetTag,
                                inserted_tuple, insert_destrel, proute, onconflict, planSlot);
    }

    ExecInsert_inner(context, resultRelInfo, slot, canSetTag,
                     inserted_tuple, insert_destrel, proute, onconflict, planSlot)
}

/* Inner body of ExecInsert after optional partition routing. */
unsafe fn ExecInsert_inner(
    context: *mut ModifyTableContext,
    resultRelInfo: *mut ResultRelInfo,
    mut slot: *mut TupleTableSlot,
    canSetTag: bool,
    inserted_tuple: *mut *mut TupleTableSlot,
    insert_destrel: *mut *mut ResultRelInfo,
    proute: *mut PartitionTupleRouting,
    onconflict: OnConflictAction,
    planSlot: *mut TupleTableSlot,
) -> *mut TupleTableSlot {
    let mtstate: *mut ModifyTableState = (*context).mtstate;
    let estate: *mut EState = (*context).estate;
    let mut recheckIndexes: *mut List = NIL;
    let mut result: *mut TupleTableSlot = core::ptr::null_mut();
    let mut ar_insert_trig_tcs: *mut TransitionCaptureState;
    let node: *mut ModifyTable = (*mtstate).ps.plan as *mut ModifyTable;

    ExecMaterializeSlot(slot);

    let resultRelationDesc: Relation = (*resultRelInfo).ri_RelationDesc;

    /*
     * Open the table's indexes, if we have not done so already, so that we
     * can add new index entries for the inserted tuple.
     */
    if (*(*resultRelationDesc).rd_rel).relhasindex
        && (*resultRelInfo).ri_IndexRelationDescs.is_null()
    {
        ExecOpenIndices(resultRelInfo, onconflict != ONCONFLICT_NONE);
    }

    /*
     * BEFORE ROW INSERT Triggers.
     *
     * Note: We fire BEFORE ROW TRIGGERS for every attempted insertion in an
     * INSERT ... ON CONFLICT statement.  We cannot check for constraint
     * violations before firing these triggers, because they can change the
     * values to insert.  Also, they can run arbitrary user-defined code with
     * side-effects that we can't cancel by just not inserting the tuple.
     */
    if !(*resultRelInfo).ri_TrigDesc.is_null()
        && (*(*resultRelInfo).ri_TrigDesc).trig_insert_before_row
    {
        /* Flush any pending inserts, so rows are visible to the triggers */
        if (*estate).es_insert_pending_result_relations != NIL {
            ExecPendingInserts(estate);
        }

        if !ExecBRInsertTriggers(estate, resultRelInfo, slot) {
            return core::ptr::null_mut(); /* "do nothing" */
        }
    }

    /* INSTEAD OF ROW INSERT Triggers */
    if !(*resultRelInfo).ri_TrigDesc.is_null()
        && (*(*resultRelInfo).ri_TrigDesc).trig_insert_instead_row
    {
        if !ExecIRInsertTriggers(estate, resultRelInfo, slot) {
            return core::ptr::null_mut(); /* "do nothing" */
        }
    } else if !(*resultRelInfo).ri_FdwRoutine.is_null() {
        /*
         * GENERATED expressions might reference the tableoid column, so
         * (re-)initialize tts_tableOid before evaluating them.
         */
        (*slot).tts_tableOid = RelationGetRelid((*resultRelInfo).ri_RelationDesc);

        /*
         * Compute stored generated columns
         */
        if !(*(*resultRelationDesc).rd_att).constr.is_null()
            && (*(*(*resultRelationDesc).rd_att).constr).has_generated_stored
        {
            ExecComputeStoredGenerated(resultRelInfo, estate, slot, CMD_INSERT);
        }

        /*
         * If the FDW supports batching, and batching is requested, accumulate
         * rows and insert them in batches. Otherwise use the per-row inserts.
         */
        if (*resultRelInfo).ri_BatchSize > 1 {
            let mut flushed: bool = false;

            /*
             * When we've reached the desired batch size, perform the
             * insertion.
             */
            if (*resultRelInfo).ri_NumSlots == (*resultRelInfo).ri_BatchSize {
                ExecBatchInsert(mtstate, resultRelInfo,
                                (*resultRelInfo).ri_Slots,
                                (*resultRelInfo).ri_PlanSlots,
                                (*resultRelInfo).ri_NumSlots,
                                estate, canSetTag);
                flushed = true;
            }

            let oldContext = MemoryContextSwitchTo((*estate).es_query_cxt);

            if (*resultRelInfo).ri_Slots.is_null() {
                (*resultRelInfo).ri_Slots = palloc(
                    core::mem::size_of::<*mut TupleTableSlot>()
                        * (*resultRelInfo).ri_BatchSize as usize,
                ) as *mut *mut TupleTableSlot;
                (*resultRelInfo).ri_PlanSlots = palloc(
                    core::mem::size_of::<*mut TupleTableSlot>()
                        * (*resultRelInfo).ri_BatchSize as usize,
                ) as *mut *mut TupleTableSlot;
            }

            /*
             * Initialize the batch slots. We don't know how many slots will
             * be needed, so we initialize them as the batch grows, and we
             * keep them across batches.
             */
            if (*resultRelInfo).ri_NumSlots >= (*resultRelInfo).ri_NumSlotsInitialized {
                let tdesc: TupleDesc = CreateTupleDescCopy((*slot).tts_tupleDescriptor);
                let plan_tdesc: TupleDesc =
                    CreateTupleDescCopy((*planSlot).tts_tupleDescriptor);

                *(*resultRelInfo).ri_Slots.add((*resultRelInfo).ri_NumSlots as usize) =
                    MakeSingleTupleTableSlot(tdesc, (*slot).tts_ops);

                *(*resultRelInfo).ri_PlanSlots.add((*resultRelInfo).ri_NumSlots as usize) =
                    MakeSingleTupleTableSlot(plan_tdesc, (*planSlot).tts_ops);

                /* remember how many batch slots we initialized */
                (*resultRelInfo).ri_NumSlotsInitialized += 1;
            }

            ExecCopySlot(
                *(*resultRelInfo).ri_Slots.add((*resultRelInfo).ri_NumSlots as usize),
                slot,
            );

            ExecCopySlot(
                *(*resultRelInfo).ri_PlanSlots.add((*resultRelInfo).ri_NumSlots as usize),
                planSlot,
            );

            /*
             * If these are the first tuples stored in the buffers, add the
             * target rel and the mtstate to the
             * es_insert_pending_result_relations and
             * es_insert_pending_modifytables lists respectively
             */
            if (*resultRelInfo).ri_NumSlots == 0 && !flushed {
                debug_assert!(!list_member_ptr(
                    (*estate).es_insert_pending_result_relations,
                    resultRelInfo as *mut c_void,
                ));
                (*estate).es_insert_pending_result_relations =
                    lappend(
                        (*estate).es_insert_pending_result_relations,
                        resultRelInfo as *mut c_void,
                    );
                (*estate).es_insert_pending_modifytables =
                    lappend(
                        (*estate).es_insert_pending_modifytables,
                        mtstate as *mut c_void,
                    );
            }
            debug_assert!(list_member_ptr(
                (*estate).es_insert_pending_result_relations,
                resultRelInfo as *mut c_void,
            ));

            (*resultRelInfo).ri_NumSlots += 1;

            MemoryContextSwitchTo(oldContext);

            return core::ptr::null_mut();
        }

        /*
         * insert into foreign table: let the FDW do it
         */
        let fdw = (*resultRelInfo).ri_FdwRoutine;
        slot = ((*fdw).ExecForeignInsert.unwrap())(
            estate,
            resultRelInfo,
            slot,
            planSlot,
        );

        if slot.is_null() {
            /* "do nothing" */
            return core::ptr::null_mut();
        }

        /*
         * AFTER ROW Triggers or RETURNING expressions might reference the
         * tableoid column, so (re-)initialize tts_tableOid before evaluating
         * them.
         */
        (*slot).tts_tableOid = RelationGetRelid((*resultRelInfo).ri_RelationDesc);
    } else {
        let wco_kind: crate::nodes::parsenodes::WCOKind;

        /*
         * Constraints and GENERATED expressions might reference the tableoid
         * column, so (re-)initialize tts_tableOid before evaluating them.
         */
        (*slot).tts_tableOid = RelationGetRelid(resultRelationDesc);

        /*
         * Compute stored generated columns
         */
        if !(*(*resultRelationDesc).rd_att).constr.is_null()
            && (*(*(*resultRelationDesc).rd_att).constr).has_generated_stored
        {
            ExecComputeStoredGenerated(resultRelInfo, estate, slot, CMD_INSERT);
        }

        /*
         * Check any RLS WITH CHECK policies.
         */
        if (*mtstate).operation == CMD_UPDATE {
            wco_kind = WCO_RLS_UPDATE_CHECK;
        } else if (*mtstate).operation == CMD_MERGE {
            wco_kind = if (*(*(*mtstate).mt_merge_action).mas_action).commandType == CMD_UPDATE {
                WCO_RLS_UPDATE_CHECK
            } else {
                WCO_RLS_INSERT_CHECK
            };
        } else {
            wco_kind = WCO_RLS_INSERT_CHECK;
        }

        /*
         * ExecWithCheckOptions() will skip any WCOs which are not of the kind
         * we are looking for at this point.
         */
        if (*resultRelInfo).ri_WithCheckOptions != NIL {
            ExecWithCheckOptions(wco_kind, resultRelInfo, slot, estate);
        }

        /*
         * Check the constraints of the tuple.
         */
        if !(*(*resultRelationDesc).rd_att).constr.is_null() {
            ExecConstraints(resultRelInfo, slot, estate);
        }

        /*
         * Also check the tuple against the partition constraint, if there is
         * one; except that if we got here via tuple-routing, we don't need to
         * if there's no BR trigger defined on the partition.
         */
        if (*(*resultRelationDesc).rd_rel).relispartition
            && ((*resultRelInfo).ri_RootResultRelInfo.is_null()
                || (!(*resultRelInfo).ri_TrigDesc.is_null()
                    && (*(*resultRelInfo).ri_TrigDesc).trig_insert_before_row))
        {
            ExecPartitionCheck(resultRelInfo, slot, estate, true);
        }

        if onconflict != ONCONFLICT_NONE && (*resultRelInfo).ri_NumIndices > 0 {
            /* Perform a speculative insertion. */
            let mut specToken: uint32;
            let mut conflictTid: ItemPointerData = core::mem::zeroed();
            let mut invalidItemPtr: ItemPointerData = core::mem::zeroed();
            let mut specConflict: bool;
            let arbiterIndexes: *mut List = (*resultRelInfo).ri_onConflictArbiterIndexes;

            ItemPointerSetInvalid(&mut invalidItemPtr);

            /*
             * Do a non-conclusive check for conflicts first.
             *
             * We loop back here if we find a conflict below.
             */
            'vlock: loop {
                CHECK_FOR_INTERRUPTS();
                specConflict = false;
                if !ExecCheckIndexConstraints(
                    resultRelInfo,
                    slot,
                    estate,
                    &mut conflictTid,
                    &mut invalidItemPtr,
                    arbiterIndexes,
                ) {
                    /* committed conflict tuple found */
                    if onconflict == ONCONFLICT_UPDATE {
                        /*
                         * In case of ON CONFLICT DO UPDATE, execute the UPDATE
                         * part.
                         */
                        let mut returning: *mut TupleTableSlot = core::ptr::null_mut();

                        if ExecOnConflictUpdate(
                            context,
                            resultRelInfo,
                            &mut conflictTid,
                            slot,
                            canSetTag,
                            &mut returning,
                        ) {
                            InstrCountTuples2(&mut (*mtstate).ps, 1.0);
                            return returning;
                        } else {
                            continue 'vlock;
                        }
                    } else {
                        /*
                         * In case of ON CONFLICT DO NOTHING, do nothing.
                         */
                        debug_assert!(onconflict == ONCONFLICT_NOTHING);
                        ExecCheckTIDVisible(
                            estate,
                            resultRelInfo,
                            &mut conflictTid,
                            ExecGetReturningSlot(estate, resultRelInfo),
                        );
                        InstrCountTuples2(&mut (*mtstate).ps, 1.0);
                        return core::ptr::null_mut();
                    }
                }

                /*
                 * Before we start insertion proper, acquire our "speculative
                 * insertion lock".
                 */
                specToken = SpeculativeInsertionLockAcquire(GetCurrentTransactionId());

                /* insert the tuple, with the speculative token */
                table_tuple_insert_speculative(
                    resultRelationDesc,
                    slot,
                    (*estate).es_output_cid,
                    0,
                    core::ptr::null_mut(),
                    specToken,
                );

                /* insert index entries for tuple */
                recheckIndexes = ExecInsertIndexTuples(
                    resultRelInfo,
                    slot,
                    estate,
                    false,
                    true,
                    &mut specConflict,
                    arbiterIndexes,
                    false,
                );

                /* adjust the tuple's state accordingly */
                table_tuple_complete_speculative(
                    resultRelationDesc,
                    slot,
                    specToken,
                    !specConflict,
                );

                /*
                 * Wake up anyone waiting for our decision.
                 */
                SpeculativeInsertionLockRelease(GetCurrentTransactionId());

                /*
                 * If there was a conflict, start from the beginning.
                 */
                if specConflict {
                    list_free(recheckIndexes);
                    continue 'vlock;
                }

                /* Since there was no insertion conflict, we're done */
                break 'vlock;
            }
        } else {
            /* insert the tuple normally */
            table_tuple_insert(
                resultRelationDesc,
                slot,
                (*estate).es_output_cid,
                0,
                core::ptr::null_mut(),
            );

            /* insert index entries for tuple */
            if (*resultRelInfo).ri_NumIndices > 0 {
                recheckIndexes = ExecInsertIndexTuples(
                    resultRelInfo,
                    slot,
                    estate,
                    false,
                    false,
                    core::ptr::null_mut(),
                    NIL,
                    false,
                );
            }
        }
    }

    if canSetTag {
        (*estate).es_processed += 1;
    }

    /*
     * If this insert is the result of a partition key update that moved the
     * tuple to a new partition, put this row into the transition NEW TABLE,
     * if there is one.
     */
    ar_insert_trig_tcs = (*mtstate).mt_transition_capture;
    if (*mtstate).operation == CMD_UPDATE
        && !(*mtstate).mt_transition_capture.is_null()
        && (*(*mtstate).mt_transition_capture).tcs_update_new_table
    {
        ExecARUpdateTriggers(
            estate,
            resultRelInfo,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
            core::ptr::null_mut(),
            core::ptr::null_mut(),
            slot,
            core::ptr::null_mut(),
            (*mtstate).mt_transition_capture,
            false,
        );

        /*
         * We've already captured the NEW TABLE row, so make sure any AR
         * INSERT trigger fired below doesn't capture it again.
         */
        ar_insert_trig_tcs = core::ptr::null_mut();
    }

    /* AFTER ROW INSERT Triggers */
    ExecARInsertTriggers(estate, resultRelInfo, slot, recheckIndexes, ar_insert_trig_tcs);

    list_free(recheckIndexes);

    /*
     * Check any WITH CHECK OPTION constraints from parent views.
     */
    if (*resultRelInfo).ri_WithCheckOptions != NIL {
        ExecWithCheckOptions(WCO_VIEW_CHECK, resultRelInfo, slot, estate);
    }

    /* Process RETURNING if present */
    if !(*resultRelInfo).ri_projectReturning.is_null() {
        let mut oldSlot: *mut TupleTableSlot = core::ptr::null_mut();

        /*
         * If this is part of a cross-partition UPDATE, and the RETURNING list
         * refers to any OLD columns, ExecDelete() will have saved the tuple
         * deleted from the original partition.
         */
        if !(*context).cpDeletedSlot.is_null() {
            let tupconv_map: *mut TupleConversionMap;

            oldSlot = (*context).cpDeletedSlot;
            tupconv_map = ExecGetRootToChildMap(resultRelInfo, estate);
            if !tupconv_map.is_null() {
                oldSlot = execute_attr_map_slot(
                    (*tupconv_map).attrMap,
                    oldSlot,
                    ExecGetReturningSlot(estate, resultRelInfo),
                );
                (*oldSlot).tts_tableOid = (*(*context).cpDeletedSlot).tts_tableOid;
                ItemPointerCopy(
                    &(*(*context).cpDeletedSlot).tts_tid,
                    &mut (*oldSlot).tts_tid,
                );
            }
        }

        result = ExecProcessReturning(
            context,
            resultRelInfo,
            CMD_INSERT,
            oldSlot,
            slot,
            (*context).planSlot,
        );

        /*
         * For a cross-partition UPDATE, release the old tuple.
         */
        if !(*context).cpDeletedSlot.is_null() {
            ExecMaterializeSlot(result);
            ExecClearTuple(oldSlot);
            if (*context).cpDeletedSlot != oldSlot {
                ExecClearTuple((*context).cpDeletedSlot);
            }
            (*context).cpDeletedSlot = core::ptr::null_mut();
        }
    }

    if !inserted_tuple.is_null() {
        *inserted_tuple = slot;
    }
    if !insert_destrel.is_null() {
        *insert_destrel = resultRelInfo;
    }

    result
}

/* ----------------------------------------------------------------
 *      ExecBatchInsert
 *
 *      Insert multiple tuples in an efficient way.
 *      Currently, this handles inserting into a foreign table without
 *      RETURNING clause.
 * ----------------------------------------------------------------
 */
unsafe fn ExecBatchInsert(
    mtstate: *mut ModifyTableState,
    resultRelInfo: *mut ResultRelInfo,
    slots: *mut *mut TupleTableSlot,
    planSlots: *mut *mut TupleTableSlot,
    numSlots: c_int,
    estate: *mut EState,
    canSetTag: bool,
) {
    let mut numInserted: c_int = numSlots;
    let mut slot: *mut TupleTableSlot = core::ptr::null_mut();
    let rslots: *mut *mut TupleTableSlot;

    /*
     * insert into foreign table: let the FDW do it
     */
    let fdw = (*resultRelInfo).ri_FdwRoutine;
    rslots = ((*fdw).ExecForeignBatchInsert.unwrap())(
        estate,
        resultRelInfo,
        slots,
        planSlots,
        &mut numInserted,
    );

    for i in 0..numInserted as usize {
        slot = *rslots.add(i);

        /*
         * AFTER ROW Triggers might reference the tableoid column, so
         * (re-)initialize tts_tableOid before evaluating them.
         */
        (*slot).tts_tableOid = RelationGetRelid((*resultRelInfo).ri_RelationDesc);

        /* AFTER ROW INSERT Triggers */
        ExecARInsertTriggers(
            estate,
            resultRelInfo,
            slot,
            NIL,
            (*mtstate).mt_transition_capture,
        );

        /*
         * Check any WITH CHECK OPTION constraints from parent views.  See the
         * comment in ExecInsert.
         */
        if (*resultRelInfo).ri_WithCheckOptions != NIL {
            ExecWithCheckOptions(WCO_VIEW_CHECK, resultRelInfo, slot, estate);
        }
    }

    if canSetTag && numInserted > 0 {
        (*estate).es_processed += numInserted as uint64;
    }

    /* Clean up all the slots, ready for the next batch */
    for i in 0..numSlots as usize {
        ExecClearTuple(*slots.add(i));
        ExecClearTuple(*planSlots.add(i));
    }
    (*resultRelInfo).ri_NumSlots = 0;
}

/*
 * ExecPendingInserts -- flushes all pending inserts to the foreign tables
 */
unsafe fn ExecPendingInserts(estate: *mut EState) {
    /* TODO(pg-port): forboth(l1,...,l2,...) iteration - needs list infrastructure */
    // Stub: this iterates es_insert_pending_result_relations and
    // es_insert_pending_modifytables in parallel.
    // For each pair, calls ExecBatchInsert.
    unimplemented!("TODO(pg-port): ExecPendingInserts - needs list iteration (nodes/list.c)");
    #[allow(unreachable_code)]
    {
        list_free((*estate).es_insert_pending_result_relations);
        list_free((*estate).es_insert_pending_modifytables);
        (*estate).es_insert_pending_result_relations = NIL;
        (*estate).es_insert_pending_modifytables = NIL;
    }
}

/*
 * ExecDeletePrologue -- subroutine for ExecDelete
 *
 * Prepare executor state for DELETE.  Actually, the only thing we have to do
 * here is execute BEFORE ROW triggers.  We return false if one of them makes
 * the delete a no-op; otherwise, return true.
 */
unsafe fn ExecDeletePrologue(
    context: *mut ModifyTableContext,
    resultRelInfo: *mut ResultRelInfo,
    tupleid: *mut ItemPointerData,
    oldtuple: HeapTuple,
    epqreturnslot: *mut *mut TupleTableSlot,
    result: *mut TM_Result,
) -> bool {
    if !result.is_null() {
        *result = TM_Ok;
    }

    /* BEFORE ROW DELETE triggers */
    if !(*resultRelInfo).ri_TrigDesc.is_null()
        && (*(*resultRelInfo).ri_TrigDesc).trig_delete_before_row
    {
        /* Flush any pending inserts, so rows are visible to the triggers */
        if (*(*context).estate).es_insert_pending_result_relations != NIL {
            ExecPendingInserts((*context).estate);
        }

        return ExecBRDeleteTriggers(
            (*context).estate,
            (*context).epqstate,
            resultRelInfo,
            tupleid,
            oldtuple,
            epqreturnslot,
            result,
            &mut (*context).tmfd,
            (*(*context).mtstate).operation == CMD_MERGE,
        );
    }

    true
}

/*
 * ExecDeleteAct -- subroutine for ExecDelete
 *
 * Actually delete the tuple from a plain table.
 *
 * Caller is in charge of doing EvalPlanQual as necessary
 */
unsafe fn ExecDeleteAct(
    context: *mut ModifyTableContext,
    resultRelInfo: *mut ResultRelInfo,
    tupleid: *mut ItemPointerData,
    changingPart: bool,
) -> TM_Result {
    let estate: *mut EState = (*context).estate;

    table_tuple_delete(
        (*resultRelInfo).ri_RelationDesc,
        tupleid,
        (*estate).es_output_cid,
        (*estate).es_snapshot as *mut _,
        (*estate).es_crosscheck_snapshot as *mut _,
        true, /* wait for commit */
        &mut (*context).tmfd,
        changingPart,
    )
}

/*
 * ExecDeleteEpilogue -- subroutine for ExecDelete
 *
 * Closing steps of tuple deletion; this invokes AFTER FOR EACH ROW triggers,
 * including the UPDATE triggers if the deletion is being done as part of a
 * cross-partition tuple move.
 */
unsafe fn ExecDeleteEpilogue(
    context: *mut ModifyTableContext,
    resultRelInfo: *mut ResultRelInfo,
    tupleid: *mut ItemPointerData,
    oldtuple: HeapTuple,
    changingPart: bool,
) {
    let mtstate: *mut ModifyTableState = (*context).mtstate;
    let estate: *mut EState = (*context).estate;
    let mut ar_delete_trig_tcs: *mut TransitionCaptureState;

    /*
     * If this delete is the result of a partition key update that moved the
     * tuple to a new partition, put this row into the transition OLD TABLE,
     * if there is one.
     */
    ar_delete_trig_tcs = (*mtstate).mt_transition_capture;
    if (*mtstate).operation == CMD_UPDATE
        && !(*mtstate).mt_transition_capture.is_null()
        && (*(*mtstate).mt_transition_capture).tcs_update_old_table
    {
        ExecARUpdateTriggers(
            estate,
            resultRelInfo,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
            tupleid,
            oldtuple,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
            (*mtstate).mt_transition_capture,
            false,
        );

        /*
         * We've already captured the OLD TABLE row, so make sure any AR
         * DELETE trigger fired below doesn't capture it again.
         */
        ar_delete_trig_tcs = core::ptr::null_mut();
    }

    /* AFTER ROW DELETE Triggers */
    ExecARDeleteTriggers(
        estate,
        resultRelInfo,
        tupleid,
        oldtuple,
        ar_delete_trig_tcs,
        changingPart,
    );
}

/* ----------------------------------------------------------------
 *      ExecDelete
 *
 *      DELETE is like UPDATE, except that we delete the tuple and no
 *      index modifications are needed.
 *
 *      Returns RETURNING result if any, otherwise NULL.
 * ----------------------------------------------------------------
 */
unsafe fn ExecDelete(
    context: *mut ModifyTableContext,
    resultRelInfo: *mut ResultRelInfo,
    tupleid: *mut ItemPointerData,
    oldtuple: HeapTuple,
    processReturning: bool,
    changingPart: bool,
    canSetTag: bool,
    tmresult: *mut TM_Result,
    tupleDeleted: *mut bool,
    epqreturnslot: *mut *mut TupleTableSlot,
) -> *mut TupleTableSlot {
    let estate: *mut EState = (*context).estate;
    let resultRelationDesc: Relation = (*resultRelInfo).ri_RelationDesc;
    let mut slot: *mut TupleTableSlot = core::ptr::null_mut();
    let mut result: TM_Result;
    let saveOld: bool;

    if !tupleDeleted.is_null() {
        *tupleDeleted = false;
    }

    /*
     * Prepare for the delete.  This includes BEFORE ROW triggers, so we're
     * done if it says we are.
     */
    if !ExecDeletePrologue(context, resultRelInfo, tupleid, oldtuple, epqreturnslot, tmresult) {
        return core::ptr::null_mut();
    }

    /* INSTEAD OF ROW DELETE Triggers */
    if !(*resultRelInfo).ri_TrigDesc.is_null()
        && (*(*resultRelInfo).ri_TrigDesc).trig_delete_instead_row
    {
        debug_assert!(!oldtuple.is_null());
        let dodelete: bool = ExecIRDeleteTriggers(estate, resultRelInfo, oldtuple);

        if !dodelete {
            /* "do nothing" */
            return core::ptr::null_mut();
        }
    } else if !(*resultRelInfo).ri_FdwRoutine.is_null() {
        /*
         * delete from foreign table: let the FDW do it
         */
        slot = ExecGetReturningSlot(estate, resultRelInfo);
        let fdw = (*resultRelInfo).ri_FdwRoutine;
        slot = ((*fdw).ExecForeignDelete.unwrap())(
            estate,
            resultRelInfo,
            slot,
            (*context).planSlot,
        );

        if slot.is_null() {
            /* "do nothing" */
            return core::ptr::null_mut();
        }

        /*
         * RETURNING expressions might reference the tableoid column, so
         * (re)initialize tts_tableOid before evaluating them.
         */
        if TTS_EMPTY(slot) {
            ExecStoreAllNullTuple(slot);
        }

        (*slot).tts_tableOid = RelationGetRelid(resultRelationDesc);
    } else {
        /*
         * delete the tuple
         *
         * Note: if context->estate->es_crosscheck_snapshot isn't
         * InvalidSnapshot, we check that the row to be deleted is visible to
         * that snapshot, and throw a can't-serialize error if not.
         */
        'ldelete: loop {
            result = ExecDeleteAct(context, resultRelInfo, tupleid, changingPart);

            if !tmresult.is_null() {
                *tmresult = result;
            }

            match result {
                TM_SelfModified => {
                    /*
                     * The target tuple was already updated or deleted by the
                     * current command, or by a later command in the current
                     * transaction.
                     */
                    if (*context).tmfd.cmax != (*estate).es_output_cid {
                        ereport!(ERROR, errmsg!("tuple to be deleted was already modified by an operation triggered by the current command")) /* C also: errhint */;
                    }

                    /* Else, already deleted by self; nothing to do */
                    return core::ptr::null_mut();
                }

                TM_Ok => {
                    break 'ldelete;
                }

                TM_Updated => {
                    let inputslot: *mut TupleTableSlot;
                    let epqslot: *mut TupleTableSlot;

                    if IsolationUsesXactSnapshot() {
                        ereport!(ERROR, errmsg!("could not serialize access due to concurrent update"));
                    }

                    /*
                     * Already know that we're going to need to do EPQ, so
                     * fetch tuple directly into the right slot.
                     */
                    EvalPlanQualBegin((*context).epqstate);
                    let inputslot = EvalPlanQualSlot(
                        (*context).epqstate,
                        resultRelationDesc,
                        (*resultRelInfo).ri_RangeTableIndex,
                    );

                    result = table_tuple_lock(
                        resultRelationDesc,
                        tupleid,
                        (*estate).es_snapshot as *mut _,
                        inputslot,
                        (*estate).es_output_cid,
                        LockTupleExclusive,
                        LockWaitBlock,
                        TUPLE_LOCK_FLAG_FIND_LAST_VERSION,
                        &mut (*context).tmfd,
                    );

                    match result {
                        TM_Ok => {
                            debug_assert!((*context).tmfd.traversed);
                            let epqslot = EvalPlanQual(
                                (*context).epqstate,
                                resultRelationDesc,
                                (*resultRelInfo).ri_RangeTableIndex,
                                inputslot,
                            );
                            if TupIsNull(epqslot) {
                                /* Tuple not passing quals anymore, exiting... */
                                return core::ptr::null_mut();
                            }

                            /*
                             * If requested, skip delete and pass back the
                             * updated row.
                             */
                            if !epqreturnslot.is_null() {
                                *epqreturnslot = epqslot;
                                return core::ptr::null_mut();
                            } else {
                                continue 'ldelete;
                            }
                        }

                        TM_SelfModified => {
                            if (*context).tmfd.cmax != (*estate).es_output_cid {
                                ereport!(ERROR, errmsg!("tuple to be deleted was already modified by an operation triggered by the current command")) /* C also: errhint */;
                            }
                            return core::ptr::null_mut();
                        }

                        TM_Deleted => {
                            /* tuple already deleted; nothing to do */
                            return core::ptr::null_mut();
                        }

                        _ => {
                            /*
                             * TM_Invisible should be impossible because we're
                             * waiting for updated row versions, and would
                             * already have errored out if the first version
                             * is invisible.
                             *
                             * TM_Updated should be impossible, because we're
                             * locking the latest version via
                             * TUPLE_LOCK_FLAG_FIND_LAST_VERSION.
                             */
                            elog!(ERROR, "unexpected table_tuple_lock status: {}", result as c_int);
                            return core::ptr::null_mut();
                        }
                    }
                }

                TM_Deleted => {
                    if IsolationUsesXactSnapshot() {
                        ereport!(ERROR, errmsg!("could not serialize access due to concurrent delete"));
                    }
                    /* tuple already deleted; nothing to do */
                    return core::ptr::null_mut();
                }

                _ => {
                    elog!(ERROR, "unrecognized table_tuple_delete status: {}", result as c_int);
                    return core::ptr::null_mut();
                }
            }
        }

        /*
         * Note: Normally one would think that we have to delete index tuples
         * associated with the heap tuple now...
         *
         * ... but in POSTGRES, we have no need to do this because VACUUM will
         * take care of it later.
         */
    }

    if canSetTag {
        (*estate).es_processed += 1;
    }

    /* Tell caller that the delete actually happened. */
    if !tupleDeleted.is_null() {
        *tupleDeleted = true;
    }

    ExecDeleteEpilogue(context, resultRelInfo, tupleid, oldtuple, changingPart);

    /*
     * Process RETURNING if present and if requested.
     *
     * If this is part of a cross-partition UPDATE, and the RETURNING list
     * refers to any OLD column values, save the old tuple here for later
     * processing of the RETURNING list by ExecInsert().
     */
    saveOld = changingPart
        && !(*resultRelInfo).ri_projectReturning.is_null()
        && (*(*resultRelInfo).ri_projectReturning).pi_state.flags & EEO_FLAG_HAS_OLD != 0;

    if !(*resultRelInfo).ri_projectReturning.is_null() && (processReturning || saveOld) {
        let rslot: *mut TupleTableSlot;

        if !(*resultRelInfo).ri_FdwRoutine.is_null() {
            /* FDW must have provided a slot containing the deleted row */
            debug_assert!(!TupIsNull(slot));
        } else {
            slot = ExecGetReturningSlot(estate, resultRelInfo);
            if !oldtuple.is_null() {
                ExecForceStoreHeapTuple(oldtuple, slot, false);
            } else {
                if !table_tuple_fetch_row_version(
                    resultRelationDesc,
                    tupleid,
                    SnapshotAny_ptr(),
                    slot,
                ) {
                    elog!(ERROR, "failed to fetch deleted tuple for DELETE RETURNING");
                }
            }
        }

        /*
         * If required, save the old tuple for later processing of the
         * RETURNING list by ExecInsert().
         */
        if saveOld {
            let tupconv_map: *mut TupleConversionMap;

            /*
             * Convert the tuple into the root partition's format/slot, if
             * needed.
             */
            tupconv_map = ExecGetChildToRootMap(resultRelInfo);
            if !tupconv_map.is_null() {
                let rootRelInfo: *mut ResultRelInfo = (*(*context).mtstate).rootResultRelInfo;
                let oldSlot: *mut TupleTableSlot = slot;

                slot = execute_attr_map_slot(
                    (*tupconv_map).attrMap,
                    slot,
                    ExecGetReturningSlot(estate, rootRelInfo),
                );

                (*slot).tts_tableOid = (*oldSlot).tts_tableOid;
                ItemPointerCopy(&(*oldSlot).tts_tid, &mut (*slot).tts_tid);
            }

            (*context).cpDeletedSlot = slot;

            return core::ptr::null_mut();
        }

        let rslot = ExecProcessReturning(
            context,
            resultRelInfo,
            CMD_DELETE,
            slot,
            core::ptr::null_mut(),
            (*context).planSlot,
        );

        /*
         * Before releasing the target tuple again, make sure rslot has a
         * local copy of any pass-by-reference values.
         */
        ExecMaterializeSlot(rslot);

        ExecClearTuple(slot);

        return rslot;
    }

    core::ptr::null_mut()
}

/*
 * ExecCrossPartitionUpdate --- Move an updated tuple to another partition.
 *
 * This works by first deleting the old tuple from the current partition,
 * followed by inserting the new tuple into the root parent table, that is,
 * mtstate->rootResultRelInfo.  It will be re-routed from there to the
 * correct partition.
 *
 * Returns true if the tuple has been successfully moved, or if it's found
 * that the tuple was concurrently deleted so there's nothing more to do
 * for the caller.
 *
 * False is returned if the tuple we're trying to move is found to have been
 * concurrently updated.
 */
unsafe fn ExecCrossPartitionUpdate(
    context: *mut ModifyTableContext,
    resultRelInfo: *mut ResultRelInfo,
    tupleid: *mut ItemPointerData,
    oldtuple: HeapTuple,
    mut slot: *mut TupleTableSlot,
    canSetTag: bool,
    updateCxt: *mut UpdateContext,
    tmresult: *mut TM_Result,
    retry_slot: *mut *mut TupleTableSlot,
    inserted_tuple: *mut *mut TupleTableSlot,
    insert_destrel: *mut *mut ResultRelInfo,
) -> bool {
    let mtstate: *mut ModifyTableState = (*context).mtstate;
    let estate: *mut EState = (*mtstate).ps.state;
    let tupconv_map: *mut TupleConversionMap;
    let mut tuple_deleted: bool = false;
    let mut epqslot: *mut TupleTableSlot = core::ptr::null_mut();

    (*context).cpDeletedSlot = core::ptr::null_mut();
    (*context).cpUpdateReturningSlot = core::ptr::null_mut();
    *retry_slot = core::ptr::null_mut();

    /*
     * Disallow an INSERT ON CONFLICT DO UPDATE that causes the original row
     * to migrate to a different partition.
     */
    if (*((*mtstate).ps.plan as *mut ModifyTable)).onConflictAction == ONCONFLICT_UPDATE {
        ereport!(ERROR, errmsg!("invalid ON UPDATE specification")) /* C also: errdetail */;
    }

    /*
     * When an UPDATE is run directly on a leaf partition, simply fail with a
     * partition constraint violation error.
     */
    if resultRelInfo == (*mtstate).rootResultRelInfo {
        ExecPartitionCheckEmitError(resultRelInfo, slot, estate);
    }

    /* Initialize tuple routing info if not already done. */
    if (*mtstate).mt_partition_tuple_routing.is_null() {
        let rootRel: Relation = (*(*mtstate).rootResultRelInfo).ri_RelationDesc;
        let oldcxt: MemoryContext;

        /* Things built here have to last for the query duration. */
        oldcxt = MemoryContextSwitchTo((*estate).es_query_cxt);

        (*mtstate).mt_partition_tuple_routing =
            ExecSetupPartitionTupleRouting(estate, rootRel);

        /*
         * Before a partition's tuple can be re-routed, it must first be
         * converted to the root's format.
         */
        debug_assert!((*mtstate).mt_root_tuple_slot.is_null());
        (*mtstate).mt_root_tuple_slot = table_slot_create(rootRel, core::ptr::null_mut());

        MemoryContextSwitchTo(oldcxt);
    }

    /*
     * Row movement, part 1.  Delete the tuple, but skip RETURNING processing.
     * We want to return rows from INSERT.
     */
    ExecDelete(
        context,
        resultRelInfo,
        tupleid,
        oldtuple,
        false, /* processReturning */
        true,  /* changingPart */
        false, /* canSetTag */
        tmresult,
        &mut tuple_deleted,
        &mut epqslot,
    );

    /*
     * For some reason if DELETE didn't happen (e.g. trigger prevented it, or
     * it was already deleted by self, or it was concurrently deleted by
     * another transaction), then we should skip the insert as well.
     */
    if !tuple_deleted {
        if (*mtstate).operation == CMD_MERGE {
            return *tmresult == TM_Ok;
        } else if TupIsNull(epqslot) {
            return true;
        } else {
            /* Fetch the most recent version of old tuple. */
            let oldSlot: *mut TupleTableSlot;

            /* ... but first, make sure ri_oldTupleSlot is initialized. */
            if !(*resultRelInfo).ri_projectNewInfoValid {
                // unlikely path
                ExecInitUpdateProjection(mtstate, resultRelInfo);
            }
            let oldSlot = (*resultRelInfo).ri_oldTupleSlot;
            if !table_tuple_fetch_row_version(
                (*resultRelInfo).ri_RelationDesc,
                tupleid,
                SnapshotAny_ptr(),
                oldSlot,
            ) {
                elog!(ERROR, "failed to fetch tuple being updated");
            }
            /* and project the new tuple to retry the UPDATE with */
            *retry_slot = ExecGetUpdateNewTuple(resultRelInfo, epqslot, oldSlot);
            return false;
        }
    }

    /*
     * resultRelInfo is one of the per-relation resultRelInfos.  So we should
     * convert the tuple into root's tuple descriptor if needed.
     */
    tupconv_map = ExecGetChildToRootMap(resultRelInfo);
    if !tupconv_map.is_null() {
        slot = execute_attr_map_slot(
            (*tupconv_map).attrMap,
            slot,
            (*mtstate).mt_root_tuple_slot,
        );
    }

    /* Tuple routing starts from the root table. */
    (*context).cpUpdateReturningSlot = ExecInsert(
        context,
        (*mtstate).rootResultRelInfo,
        slot,
        canSetTag,
        inserted_tuple,
        insert_destrel,
    );

    /*
     * Reset the transition state that may possibly have been written by
     * INSERT.
     */
    if !(*mtstate).mt_transition_capture.is_null() {
        (*(*mtstate).mt_transition_capture).tcs_original_insert_tuple = core::ptr::null_mut();
    }

    /* We're done moving. */
    true
}

/*
 * ExecUpdatePrologue -- subroutine for ExecUpdate
 *
 * Prepare executor state for UPDATE.  This includes running BEFORE ROW
 * triggers.  We return false if one of them makes the update a no-op;
 * otherwise, return true.
 */
unsafe fn ExecUpdatePrologue(
    context: *mut ModifyTableContext,
    resultRelInfo: *mut ResultRelInfo,
    tupleid: *mut ItemPointerData,
    oldtuple: HeapTuple,
    slot: *mut TupleTableSlot,
    result: *mut TM_Result,
) -> bool {
    let resultRelationDesc: Relation = (*resultRelInfo).ri_RelationDesc;

    if !result.is_null() {
        *result = TM_Ok;
    }

    ExecMaterializeSlot(slot);

    /*
     * Open the table's indexes, if we have not done so already.
     */
    if (*(*resultRelationDesc).rd_rel).relhasindex
        && (*resultRelInfo).ri_IndexRelationDescs.is_null()
    {
        ExecOpenIndices(resultRelInfo, false);
    }

    /* BEFORE ROW UPDATE triggers */
    if !(*resultRelInfo).ri_TrigDesc.is_null()
        && (*(*resultRelInfo).ri_TrigDesc).trig_update_before_row
    {
        /* Flush any pending inserts, so rows are visible to the triggers */
        if (*(*context).estate).es_insert_pending_result_relations != NIL {
            ExecPendingInserts((*context).estate);
        }

        return ExecBRUpdateTriggers(
            (*context).estate,
            (*context).epqstate,
            resultRelInfo,
            tupleid,
            oldtuple,
            slot,
            result,
            &mut (*context).tmfd,
            (*(*context).mtstate).operation == CMD_MERGE,
        );
    }

    true
}

/*
 * ExecUpdatePrepareSlot -- subroutine for ExecUpdateAct
 *
 * Apply the final modifications to the tuple slot before the update.
 */
unsafe fn ExecUpdatePrepareSlot(
    resultRelInfo: *mut ResultRelInfo,
    slot: *mut TupleTableSlot,
    estate: *mut EState,
) {
    let resultRelationDesc: Relation = (*resultRelInfo).ri_RelationDesc;

    /*
     * Constraints and GENERATED expressions might reference the tableoid
     * column, so (re-)initialize tts_tableOid before evaluating them.
     */
    (*slot).tts_tableOid = RelationGetRelid(resultRelationDesc);

    /*
     * Compute stored generated columns
     */
    if !(*(*resultRelationDesc).rd_att).constr.is_null()
        && (*(*(*resultRelationDesc).rd_att).constr).has_generated_stored
    {
        ExecComputeStoredGenerated(resultRelInfo, estate, slot, CMD_UPDATE);
    }
}

/*
 * ExecUpdateAct -- subroutine for ExecUpdate
 *
 * Actually update the tuple, when operating on a plain table.  If the
 * table is a partition, and the command was called referencing an ancestor
 * partitioned table, this routine migrates the resulting tuple to another
 * partition.
 *
 * The caller is in charge of keeping indexes current as necessary.  The
 * caller is also in charge of doing EvalPlanQual if the tuple is found to
 * be concurrently updated.  However, in case of a cross-partition update,
 * this routine does it.
 */
unsafe fn ExecUpdateAct(
    context: *mut ModifyTableContext,
    resultRelInfo: *mut ResultRelInfo,
    tupleid: *mut ItemPointerData,
    oldtuple: HeapTuple,
    mut slot: *mut TupleTableSlot,
    canSetTag: bool,
    updateCxt: *mut UpdateContext,
) -> TM_Result {
    let estate: *mut EState = (*context).estate;
    let resultRelationDesc: Relation = (*resultRelInfo).ri_RelationDesc;
    let partition_constraint_failed: bool;
    let result: TM_Result;

    (*updateCxt).crossPartUpdate = false;

    /*
     * If we move the tuple to a new partition, we loop back here to recompute
     * GENERATED values and recheck any RLS policies and constraints.
     */
    'lreplace: loop {
        /* Fill in GENERATEd columns */
        ExecUpdatePrepareSlot(resultRelInfo, slot, estate);

        /* ensure slot is independent, consider e.g. EPQ */
        ExecMaterializeSlot(slot);

        /*
         * If partition constraint fails, this row might get moved to another
         * partition, in which case we should check the RLS CHECK policy just
         * before inserting into the new partition.
         */
        let partition_constraint_failed: bool = (*(*resultRelationDesc).rd_rel).relispartition
            && !ExecPartitionCheck(resultRelInfo, slot, estate, false);

        /* Check any RLS UPDATE WITH CHECK policies */
        if !partition_constraint_failed && (*resultRelInfo).ri_WithCheckOptions != NIL {
            ExecWithCheckOptions(WCO_RLS_UPDATE_CHECK, resultRelInfo, slot, estate);
        }

        /*
         * If a partition check failed, try to move the row into the right
         * partition.
         */
        if partition_constraint_failed {
            let mut inserted_tuple: *mut TupleTableSlot = core::ptr::null_mut();
            let mut retry_slot: *mut TupleTableSlot = core::ptr::null_mut();
            let mut insert_destrel: *mut ResultRelInfo = core::ptr::null_mut();
            let mut result: TM_Result = TM_Ok;

            /*
             * ExecCrossPartitionUpdate will first DELETE the row from the
             * partition it's currently in and then insert it back into the root
             * table, which will re-route it to the correct partition.
             */
            if ExecCrossPartitionUpdate(
                context,
                resultRelInfo,
                tupleid,
                oldtuple,
                slot,
                canSetTag,
                updateCxt,
                &mut result,
                &mut retry_slot,
                &mut inserted_tuple,
                &mut insert_destrel,
            ) {
                /* success! */
                (*updateCxt).crossPartUpdate = true;

                /*
                 * If the partitioned table being updated is referenced in foreign
                 * keys, queue up trigger events to check that none of them were
                 * violated.
                 */
                if !insert_destrel.is_null()
                    && !(*resultRelInfo).ri_TrigDesc.is_null()
                    && (*(*resultRelInfo).ri_TrigDesc).trig_update_after_row
                {
                    ExecCrossPartitionUpdateForeignKey(
                        context,
                        resultRelInfo,
                        insert_destrel,
                        tupleid,
                        slot,
                        inserted_tuple,
                    );
                }

                return TM_Ok;
            }

            /*
             * No luck, a retry is needed.  If running MERGE, we do not do so
             * here; instead let it handle that on its own rules.
             */
            if (*(*context).mtstate).operation == CMD_MERGE {
                return result;
            }

            /*
             * ExecCrossPartitionUpdate installed an updated version of the new
             * tuple in the retry slot; start over.
             */
            slot = retry_slot;
            continue 'lreplace;
        }

        /*
         * Check the constraints of the tuple.
         */
        if !(*(*resultRelationDesc).rd_att).constr.is_null() {
            ExecConstraints(resultRelInfo, slot, estate);
        }

        /*
         * replace the heap tuple
         *
         * Note: if es_crosscheck_snapshot isn't InvalidSnapshot, we check that
         * the row to be updated is visible to that snapshot.
         */
        let result = table_tuple_update(
            resultRelationDesc,
            tupleid,
            slot,
            (*estate).es_output_cid,
            (*estate).es_snapshot as *mut _,
            (*estate).es_crosscheck_snapshot as *mut _,
            true, /* wait for commit */
            &mut (*context).tmfd,
            &mut (*updateCxt).lockmode,
            &mut (*updateCxt).updateIndexes,
        );

        return result;
    }
}

/*
 * ExecUpdateEpilogue -- subroutine for ExecUpdate
 *
 * Closing steps of updating a tuple.  Must be called if ExecUpdateAct
 * returns indicating that the tuple was updated.
 */
unsafe fn ExecUpdateEpilogue(
    context: *mut ModifyTableContext,
    updateCxt: *mut UpdateContext,
    resultRelInfo: *mut ResultRelInfo,
    tupleid: *mut ItemPointerData,
    oldtuple: HeapTuple,
    slot: *mut TupleTableSlot,
) {
    let mtstate: *mut ModifyTableState = (*context).mtstate;
    let mut recheckIndexes: *mut List = NIL;

    /* insert index entries for tuple if necessary */
    if (*resultRelInfo).ri_NumIndices > 0 && (*updateCxt).updateIndexes != TU_None {
        recheckIndexes = ExecInsertIndexTuples(
            resultRelInfo,
            slot,
            (*context).estate,
            true,
            false,
            core::ptr::null_mut(),
            NIL,
            (*updateCxt).updateIndexes == TU_Summarizing,
        );
    }

    /* AFTER ROW UPDATE Triggers */
    ExecARUpdateTriggers(
        (*context).estate,
        resultRelInfo,
        core::ptr::null_mut(),
        core::ptr::null_mut(),
        tupleid,
        oldtuple,
        slot,
        recheckIndexes,
        if (*mtstate).operation == CMD_INSERT {
            (*mtstate).mt_oc_transition_capture
        } else {
            (*mtstate).mt_transition_capture
        },
        false,
    );

    list_free(recheckIndexes);

    /*
     * Check any WITH CHECK OPTION constraints from parent views.
     */
    if (*resultRelInfo).ri_WithCheckOptions != NIL {
        ExecWithCheckOptions(WCO_VIEW_CHECK, resultRelInfo, slot, (*context).estate);
    }
}

/*
 * Queues up an update event using the target root partitioned table's
 * trigger to check that a cross-partition update hasn't broken any foreign
 * keys pointing into it.
 */
unsafe fn ExecCrossPartitionUpdateForeignKey(
    context: *mut ModifyTableContext,
    sourcePartInfo: *mut ResultRelInfo,
    destPartInfo: *mut ResultRelInfo,
    tupleid: *mut ItemPointerData,
    oldslot: *mut TupleTableSlot,
    newslot: *mut TupleTableSlot,
) {
    let rootRelInfo: *mut ResultRelInfo = (*sourcePartInfo).ri_RootResultRelInfo;
    let ancestorRels: *mut List = ExecGetAncestorResultRels((*context).estate, sourcePartInfo);

    /*
     * For any foreign keys that point directly into a non-root ancestors of
     * the source partition, we can in theory fire an update event.
     */
    /* TODO(pg-port): foreach(lc, ancestorRels) - needs list iteration */
    // We iterate; the actual loop body is faithfully translated but needs list iteration.
    // Stub the iteration part:
    #[allow(unreachable_code)]
    {
        unimplemented!("TODO(pg-port): ExecCrossPartitionUpdateForeignKey - needs list iteration (nodes/list.c)");
    }

    /* Perform the root table's triggers. */
    ExecARUpdateTriggers(
        (*context).estate,
        rootRelInfo,
        sourcePartInfo,
        destPartInfo,
        tupleid,
        core::ptr::null_mut(),
        newslot,
        NIL,
        core::ptr::null_mut(),
        true,
    );
}

/* ----------------------------------------------------------------
 *      ExecUpdate
 *
 *      note: we can't run UPDATE queries with transactions
 *      off because UPDATEs are actually INSERTs and our
 *      scan will mistakenly loop forever, updating the tuple
 *      it just inserted..  This should be fixed but until it
 *      is, we don't want to get stuck in an infinite loop
 *      which corrupts your database..
 *
 *      When updating a table, tupleid identifies the tuple to update and
 *      oldtuple is NULL.  When updating through a view INSTEAD OF trigger,
 *      oldtuple is passed to the triggers and identifies what to update, and
 *      tupleid is invalid.  When updating a foreign table, tupleid is
 *      invalid; the FDW has to figure out which row to update using data from
 *      the planSlot.  oldtuple is passed to foreign table triggers; it is
 *      NULL when the foreign table has no relevant triggers.
 *
 *      oldSlot contains the old tuple value.
 *      slot contains the new tuple value to be stored.
 *      planSlot is the output of the ModifyTable's subplan; we use it
 *      to access values from other input tables (for RETURNING),
 *      row-ID junk columns, etc.
 *
 *      Returns RETURNING result if any, otherwise NULL.  On exit, if tupleid
 *      had identified the tuple to update, it will identify the tuple
 *      actually updated after EvalPlanQual.
 * ----------------------------------------------------------------
 */
unsafe fn ExecUpdate(
    context: *mut ModifyTableContext,
    resultRelInfo: *mut ResultRelInfo,
    tupleid: *mut ItemPointerData,
    oldtuple: HeapTuple,
    oldSlot: *mut TupleTableSlot,
    mut slot: *mut TupleTableSlot,
    canSetTag: bool,
) -> *mut TupleTableSlot {
    let estate: *mut EState = (*context).estate;
    let resultRelationDesc: Relation = (*resultRelInfo).ri_RelationDesc;
    let mut updateCxt = UpdateContext {
        lockmode: LockTupleExclusive,
        updateIndexes: TU_None,
        crossPartUpdate: false,
    };
    let mut result: TM_Result;

    /* abort the operation if not running transactions */
    if IsBootstrapProcessingMode() {
        elog!(ERROR, "cannot UPDATE during bootstrap");
    }

    /*
     * Prepare for the update.  This includes BEFORE ROW triggers, so we're
     * done if it says we are.
     */
    if !ExecUpdatePrologue(context, resultRelInfo, tupleid, oldtuple, slot, core::ptr::null_mut()) {
        return core::ptr::null_mut();
    }

    /* INSTEAD OF ROW UPDATE Triggers */
    if !(*resultRelInfo).ri_TrigDesc.is_null()
        && (*(*resultRelInfo).ri_TrigDesc).trig_update_instead_row
    {
        if !ExecIRUpdateTriggers(estate, resultRelInfo, oldtuple, slot) {
            return core::ptr::null_mut(); /* "do nothing" */
        }
    } else if !(*resultRelInfo).ri_FdwRoutine.is_null() {
        /* Fill in GENERATEd columns */
        ExecUpdatePrepareSlot(resultRelInfo, slot, estate);

        /* update in foreign table: let the FDW do it */
        let fdw = (*resultRelInfo).ri_FdwRoutine;
        slot = ((*fdw).ExecForeignUpdate.unwrap())(estate, resultRelInfo, slot, (*context).planSlot);

        if slot.is_null() {
            /* "do nothing" */
            return core::ptr::null_mut();
        }

        /*
         * AFTER ROW Triggers or RETURNING expressions might reference the
         * tableoid column, so (re-)initialize tts_tableOid before evaluating
         * them.  (This covers the case where the FDW replaced the slot.)
         */
        (*slot).tts_tableOid = RelationGetRelid(resultRelationDesc);
    } else {
        let mut lockedtid: ItemPointerData = *tupleid;

        /*
         * If we generate a new candidate tuple after EvalPlanQual testing, we
         * must loop back here to try again.  (We don't need to redo triggers,
         * however.  If there are any BEFORE triggers then trigger.c will have
         * done table_tuple_lock to lock the correct tuple, so there's no need
         * to do them again.)
         */
        'redo_act: loop {
            lockedtid = *tupleid;
            result = ExecUpdateAct(context, resultRelInfo, tupleid, oldtuple, slot,
                                   canSetTag, &mut updateCxt);

            /*
             * If ExecUpdateAct reports that a cross-partition update was done,
             * then the RETURNING tuple (if any) has been projected and there's
             * nothing else for us to do.
             */
            if updateCxt.crossPartUpdate {
                return (*context).cpUpdateReturningSlot;
            }

            match result {
                TM_SelfModified => {
                    /*
                     * The target tuple was already updated or deleted by the
                     * current command, or by a later command in the current
                     * transaction.  The former case is possible in a join UPDATE
                     * where multiple tuples join to the same target tuple. This
                     * is pretty questionable, but Postgres has always allowed it:
                     * we just execute the first update action and ignore
                     * additional update attempts.
                     *
                     * The latter case arises if the tuple is modified by a
                     * command in a BEFORE trigger, or perhaps by a command in a
                     * volatile function used in the query.  In such situations we
                     * should not ignore the update, but it is equally unsafe to
                     * proceed.  We don't want to discard the original UPDATE
                     * while keeping the triggered actions based on it; and we
                     * have no principled way to merge this update with the
                     * previous ones.  So throwing an error is the only safe
                     * course.
                     *
                     * If a trigger actually intends this type of interaction, it
                     * can re-execute the UPDATE (assuming it can figure out how)
                     * and then return NULL to cancel the outer update.
                     */
                    if (*context).tmfd.cmax != (*estate).es_output_cid {
                        ereport!(ERROR, errmsg!("tuple to be updated was already modified by an operation triggered by the current command")) /* C also: errhint */;
                    }
                    /* Else, already updated by self; nothing to do */
                    return core::ptr::null_mut();
                }

                TM_Ok => {
                    break 'redo_act;
                }

                TM_Updated => {
                    if IsolationUsesXactSnapshot() {
                        ereport!(ERROR, errmsg!("could not serialize access due to concurrent update"));
                    }

                    /* Already know that we're going to need to do EPQ, so
                     * fetch tuple directly into the right slot. */
                    let inputslot = EvalPlanQualSlot(
                        (*context).epqstate,
                        resultRelationDesc,
                        (*resultRelInfo).ri_RangeTableIndex,
                    );

                    result = table_tuple_lock(
                        resultRelationDesc,
                        tupleid,
                        (*estate).es_snapshot as *mut _,
                        inputslot,
                        (*estate).es_output_cid,
                        updateCxt.lockmode,
                        LockWaitBlock,
                        TUPLE_LOCK_FLAG_FIND_LAST_VERSION,
                        &mut (*context).tmfd,
                    );

                    match result {
                        TM_Ok => {
                            debug_assert!((*context).tmfd.traversed);

                            let epqslot = EvalPlanQual(
                                (*context).epqstate,
                                resultRelationDesc,
                                (*resultRelInfo).ri_RangeTableIndex,
                                inputslot,
                            );
                            if TupIsNull(epqslot) {
                                /* Tuple not passing quals anymore, exiting... */
                                return core::ptr::null_mut();
                            }

                            /* Make sure ri_oldTupleSlot is initialized. */
                            if unlikely(!(*resultRelInfo).ri_projectNewInfoValid) {
                                ExecInitUpdateProjection((*context).mtstate, resultRelInfo);
                            }

                            if (*resultRelInfo).ri_needLockTagTuple {
                                UnlockTuple(resultRelationDesc, &lockedtid as *const _ as *mut _, InplaceUpdateTupleLock);
                                LockTuple(resultRelationDesc, tupleid, InplaceUpdateTupleLock);
                            }

                            /* Fetch the most recent version of old tuple. */
                            let old_slot = (*resultRelInfo).ri_oldTupleSlot;
                            if !table_tuple_fetch_row_version(
                                resultRelationDesc,
                                tupleid,
                                SnapshotAny_ptr(),
                                old_slot,
                            ) {
                                elog!(ERROR, "failed to fetch tuple being updated");
                            }
                            slot = ExecGetUpdateNewTuple(resultRelInfo, epqslot, old_slot);
                            continue 'redo_act;
                        }

                        TM_Deleted => {
                            /* tuple already deleted; nothing to do */
                            return core::ptr::null_mut();
                        }

                        TM_SelfModified => {
                            /*
                             * This can be reached when following an update
                             * chain from a tuple updated by another session,
                             * reaching a tuple that was already updated in
                             * this transaction. If previously modified by
                             * this command, ignore the redundant update,
                             * otherwise error out.
                             *
                             * See also TM_SelfModified response to
                             * table_tuple_update() above.
                             */
                            if (*context).tmfd.cmax != (*estate).es_output_cid {
                                ereport!(ERROR, errmsg!("tuple to be updated was already modified by an operation triggered by the current command")) /* C also: errhint */;
                            }
                            return core::ptr::null_mut();
                        }

                        _ => {
                            /* see table_tuple_lock call in ExecDelete() */
                            elog!(ERROR, "unexpected table_tuple_lock status: {}", result as c_int);
                            return core::ptr::null_mut();
                        }
                    }
                }

                TM_Deleted => {
                    if IsolationUsesXactSnapshot() {
                        ereport!(ERROR, errmsg!("could not serialize access due to concurrent delete"));
                    }
                    /* tuple already deleted; nothing to do */
                    return core::ptr::null_mut();
                }

                _ => {
                    elog!(ERROR, "unrecognized table_tuple_update status: {}", result as c_int);
                    return core::ptr::null_mut();
                }
            }
        }
    }

    if canSetTag {
        (*estate).es_processed += 1;
    }

    ExecUpdateEpilogue(context, &mut updateCxt, resultRelInfo, tupleid, oldtuple, slot);

    /* Process RETURNING if present */
    if !(*resultRelInfo).ri_projectReturning.is_null() {
        return ExecProcessReturning(context, resultRelInfo, CMD_UPDATE, oldSlot, slot, (*context).planSlot);
    }

    core::ptr::null_mut()
}

/*
 * ExecOnConflictUpdate --- execute UPDATE of INSERT ON CONFLICT DO UPDATE
 *
 * Try to lock tuple for update as part of speculative insertion.  If
 * a qual originating from ON CONFLICT DO UPDATE is satisfied, update
 * (but still lock row, even though it may not satisfy estate's
 * snapshot).
 *
 * Returns true if we're done (with or without an update), or false if
 * the caller must retry the INSERT from scratch.
 */
unsafe fn ExecOnConflictUpdate(
    context: *mut ModifyTableContext,
    resultRelInfo: *mut ResultRelInfo,
    conflictTid: *mut ItemPointerData,
    excludedSlot: *mut TupleTableSlot,
    canSetTag: bool,
    returning: *mut *mut TupleTableSlot,
) -> bool {
    let mtstate: *mut ModifyTableState = (*context).mtstate;
    let econtext: *mut ExprContext = (*mtstate).ps.ps_ExprContext;
    let relation: Relation = (*resultRelInfo).ri_RelationDesc;
    let onConflictSetWhere: *mut ExprState = (*(*resultRelInfo).ri_onConflict).oc_WhereClause;
    let existing: *mut TupleTableSlot = (*(*resultRelInfo).ri_onConflict).oc_Existing;
    let mut tmfd: TM_FailureData = core::mem::zeroed();
    let lockmode: LockTupleMode;
    let mut test: TM_Result;
    let xminDatum: Datum;
    let xmin: TransactionId;
    let mut isnull: bool = false;

    /*
     * Parse analysis should have blocked ON CONFLICT for all system
     * relations, which includes these.  There's no fundamental obstacle to
     * supporting this; we'd just need to handle LOCKTAG_TUPLE like the other
     * ExecUpdate() caller.
     */
    debug_assert!(!(*resultRelInfo).ri_needLockTagTuple);

    /* Determine lock mode to use */
    let lockmode = ExecUpdateLockMode((*context).estate, resultRelInfo);

    /*
     * Lock tuple for update.  Don't follow updates when tuple cannot be
     * locked without doing so.  A row locking conflict here means our
     * previous conclusion that the tuple is conclusively committed is not
     * true anymore.
     */
    test = table_tuple_lock(
        relation,
        conflictTid,
        (*(*context).estate).es_snapshot,
        existing,
        (*(*context).estate).es_output_cid,
        lockmode,
        LockWaitBlock,
        0,
        &mut tmfd,
    );

    match test {
        TM_Ok => {
            /* success! */
        }

        TM_Invisible => {
            /*
             * This can occur when a just inserted tuple is updated again in
             * the same command. E.g. because multiple rows with the same
             * conflicting key values are inserted.
             *
             * This is somewhat similar to the ExecUpdate() TM_SelfModified
             * case.  We do not want to proceed because it would lead to the
             * same row being updated a second time in some unspecified order,
             * and in contrast to plain UPDATEs there's no historical behavior
             * to break.
             *
             * It is the user's responsibility to prevent this situation from
             * occurring.  These problems are why the SQL standard similarly
             * specifies that for SQL MERGE, an exception must be raised in
             * the event of an attempt to update the same row twice.
             */
            let xminDatum = slot_getsysattr(existing, MinTransactionIdAttributeNumber as c_int, &mut isnull);
            debug_assert!(!isnull);
            let xmin: TransactionId = DatumGetTransactionId(xminDatum);

            if TransactionIdIsCurrentTransactionId(xmin) {
                ereport!(ERROR, errmsg!("{} command cannot affect row a second time", "ON CONFLICT DO UPDATE")) /* C also: errhint */;
            }

            /* This shouldn't happen */
            elog!(ERROR, "attempted to lock invisible tuple");
        }

        TM_SelfModified => {
            /*
             * This state should never be reached. As a dirty snapshot is used
             * to find conflicting tuples, speculative insertion wouldn't have
             * seen this row to conflict with.
             */
            elog!(ERROR, "unexpected self-updated tuple");
        }

        TM_Updated => {
            if IsolationUsesXactSnapshot() {
                ereport!(ERROR, errmsg!("could not serialize access due to concurrent update"));
            }

            /*
             * Tell caller to try again from the very start.
             *
             * It does not make sense to use the usual EvalPlanQual() style
             * loop here, as the new version of the row might not conflict
             * anymore, or the conflicting tuple has actually been deleted.
             */
            ExecClearTuple(existing);
            return false;
        }

        TM_Deleted => {
            if IsolationUsesXactSnapshot() {
                ereport!(ERROR, errmsg!("could not serialize access due to concurrent delete"));
            }

            /* see TM_Updated case */
            ExecClearTuple(existing);
            return false;
        }

        _ => {
            elog!(ERROR, "unrecognized table_tuple_lock status: {}", test as c_int);
        }
    }

    /* Success, the tuple is locked. */

    /*
     * Verify that the tuple is visible to our MVCC snapshot if the current
     * isolation level mandates that.
     *
     * It's not sufficient to rely on the check within ExecUpdate() as e.g.
     * CONFLICT ... WHERE clause may prevent us from reaching that.
     *
     * This means we only ever continue when a new command in the current
     * transaction could see the row, even though in READ COMMITTED mode the
     * tuple will not be visible according to the current statement's
     * snapshot.  This is in line with the way UPDATE deals with newer tuple
     * versions.
     */
    ExecCheckTupleVisible((*context).estate, relation, existing);

    /*
     * Make tuple and any needed join variables available to ExecQual and
     * ExecProject.  The EXCLUDED tuple is installed in ecxt_innertuple, while
     * the target's existing tuple is installed in the scantuple.  EXCLUDED
     * has been made to reference INNER_VAR in setrefs.c, but there is no
     * other redirection.
     */
    (*econtext).ecxt_scantuple = existing;
    (*econtext).ecxt_innertuple = excludedSlot;
    (*econtext).ecxt_outertuple = core::ptr::null_mut();

    if !ExecQual(onConflictSetWhere, econtext) {
        ExecClearTuple(existing);
        InstrCountFiltered1(&mut (*mtstate).ps, 1.0);
        return true; /* done with the tuple */
    }

    if (*resultRelInfo).ri_WithCheckOptions != NIL {
        /*
         * Check target's existing tuple against UPDATE-applicable USING
         * security barrier quals (if any), enforced here as RLS checks/WCOs.
         *
         * The rewriter creates UPDATE RLS checks/WCOs for UPDATE security
         * quals, and stores them as WCOs of "kind" WCO_RLS_CONFLICT_CHECK,
         * but that's almost the extent of its special handling for ON
         * CONFLICT DO UPDATE.
         *
         * The rewriter will also have associated UPDATE applicable straight
         * RLS checks/WCOs for the benefit of the ExecUpdate() call that
         * follows.  INSERTs and UPDATEs naturally have mutually exclusive WCO
         * kinds, so there is no danger of spurious over-enforcement in the
         * INSERT or UPDATE path.
         */
        ExecWithCheckOptions(WCO_RLS_CONFLICT_CHECK, resultRelInfo, existing, (*mtstate).ps.state);
    }

    /* Project the new tuple version */
    ExecProject((*(*resultRelInfo).ri_onConflict).oc_ProjInfo);

    /*
     * Note that it is possible that the target tuple has been modified in
     * this session, after the above table_tuple_lock. We choose to not error
     * out in that case, in line with ExecUpdate's treatment of similar cases.
     * This can happen if an UPDATE is triggered from within ExecQual(),
     * ExecWithCheckOptions() or ExecProject() above, e.g. by selecting from a
     * wCTE in the ON CONFLICT's SET.
     */

    /* Execute UPDATE with projection */
    *returning = ExecUpdate(
        context,
        resultRelInfo,
        conflictTid,
        core::ptr::null_mut(),
        existing,
        (*(*resultRelInfo).ri_onConflict).oc_ProjSlot,
        canSetTag,
    );

    /*
     * Clear out existing tuple, as there might not be another conflict among
     * the next input rows. Don't want to hold resources till the end of the
     * query.  First though, make sure that the returning slot, if any, has a
     * local copy of any OLD pass-by-reference values, if it refers to any OLD
     * columns.
     */
    if !(*returning).is_null()
        && (*(*resultRelInfo).ri_projectReturning).pi_state.flags & EEO_FLAG_HAS_OLD != 0
    {
        ExecMaterializeSlot(*returning);
    }

    ExecClearTuple(existing);

    true
}

/*
 * Perform MERGE.
 */
unsafe fn ExecMerge(
    context: *mut ModifyTableContext,
    resultRelInfo: *mut ResultRelInfo,
    tupleid: *mut ItemPointerData,
    oldtuple: HeapTuple,
    canSetTag: bool,
) -> *mut TupleTableSlot {
    let mut rslot: *mut TupleTableSlot = core::ptr::null_mut();
    let mut matched: bool;

    /*-----
     * If we are dealing with a WHEN MATCHED case, tupleid or oldtuple is
     * valid, depending on whether the result relation is a table or a view.
     * We execute the first action for which the additional WHEN MATCHED AND
     * quals pass.  If an action without quals is found, that action is
     * executed.
     *
     * Similarly, in the WHEN NOT MATCHED BY SOURCE case, tupleid or oldtuple
     * is valid, and we look at the given WHEN NOT MATCHED BY SOURCE actions
     * in sequence until one passes.  This is almost identical to the WHEN
     * MATCHED case, and both cases are handled by ExecMergeMatched().
     *
     * Finally, in the WHEN NOT MATCHED [BY TARGET] case, both tupleid and
     * oldtuple are invalid, and we look at the given WHEN NOT MATCHED [BY
     * TARGET] actions in sequence until one passes.
     *
     * Things get interesting in case of concurrent update/delete of the
     * target tuple. Such concurrent update/delete is detected while we are
     * executing a WHEN MATCHED or WHEN NOT MATCHED BY SOURCE action.
     *
     * A concurrent update can:
     *
     * 1. modify the target tuple so that the results from checking any
     *    additional quals attached to WHEN MATCHED or WHEN NOT MATCHED BY
     *    SOURCE actions potentially change, but the result from the join
     *    quals does not change.
     *
     *    In this case, we are still dealing with the same kind of match
     *    (MATCHED or NOT MATCHED BY SOURCE).  We recheck the same list of
     *    actions from the start and choose the first one that satisfies the
     *    new target tuple.
     *
     * 2. modify the target tuple in the WHEN MATCHED case so that the join
     *    quals no longer pass and hence the source and target tuples no
     *    longer match.
     *
     *    In this case, we are now dealing with a NOT MATCHED case, and we
     *    process both WHEN NOT MATCHED BY SOURCE and WHEN NOT MATCHED [BY
     *    TARGET] actions.  First ExecMergeMatched() processes the list of
     *    WHEN NOT MATCHED BY SOURCE actions in sequence until one passes,
     *    then ExecMergeNotMatched() processes any WHEN NOT MATCHED [BY
     *    TARGET] actions in sequence until one passes.  Thus we may execute
     *    two actions; one of each kind.
     *
     * Thus we support concurrent updates that turn MATCHED candidate rows
     * into NOT MATCHED rows.  However, we do not attempt to support cases
     * that would turn NOT MATCHED rows into MATCHED rows, or which would
     * cause a target row to match a different source row.
     *
     * A concurrent delete changes a WHEN MATCHED case to WHEN NOT MATCHED
     * [BY TARGET].
     *
     * ExecMergeMatched() takes care of following the update chain and
     * re-finding the qualifying WHEN MATCHED or WHEN NOT MATCHED BY SOURCE
     * action, as long as the target tuple still exists. If the target tuple
     * gets deleted or a concurrent update causes the join quals to fail, it
     * returns a matched status of false and we call ExecMergeNotMatched().
     * Given that ExecMergeMatched() always makes progress by following the
     * update chain and we never switch from ExecMergeNotMatched() to
     * ExecMergeMatched(), there is no risk of a livelock.
     */
    matched = !tupleid.is_null() || !oldtuple.is_null();
    if matched {
        rslot = ExecMergeMatched(context, resultRelInfo, tupleid, oldtuple, canSetTag, &mut matched);
    }

    /*
     * Deal with the NOT MATCHED case (either a NOT MATCHED tuple from the
     * join, or a previously MATCHED tuple for which ExecMergeMatched() set
     * "matched" to false, indicating that it no longer matches).
     */
    if !matched {
        /*
         * If a concurrent update turned a MATCHED case into a NOT MATCHED
         * case, and we have both WHEN NOT MATCHED BY SOURCE and WHEN NOT
         * MATCHED [BY TARGET] actions, and there is a RETURNING clause,
         * ExecMergeMatched() may have already executed a WHEN NOT MATCHED BY
         * SOURCE action, and computed the row to return.  If so, we cannot
         * execute a WHEN NOT MATCHED [BY TARGET] action now, so mark it as
         * pending (to be processed on the next call to ExecModifyTable()).
         * Otherwise, just process the action now.
         */
        if rslot.is_null() {
            rslot = ExecMergeNotMatched(context, resultRelInfo, canSetTag);
        } else {
            (*(*context).mtstate).mt_merge_pending_not_matched = (*context).planSlot;
        }
    }

    rslot
}

/*
 * Check and execute the first qualifying MATCHED or NOT MATCHED BY SOURCE
 * action, depending on whether the join quals are satisfied.  If the target
 * relation is a table, the current target tuple is identified by tupleid.
 * Otherwise, if the target relation is a view, oldtuple is the current target
 * tuple from the view.
 *
 * We start from the first WHEN MATCHED or WHEN NOT MATCHED BY SOURCE action
 * and check if the WHEN quals pass, if any. If the WHEN quals for the first
 * action do not pass, we check the second, then the third and so on. If we
 * reach the end without finding a qualifying action, we return NULL.
 * Otherwise, we execute the qualifying action and return its RETURNING
 * result, if any, or NULL.
 *
 * On entry, "*matched" is assumed to be true.  If a concurrent update or
 * delete is detected that causes the join quals to no longer pass, we set it
 * to false, indicating that the caller should process any NOT MATCHED [BY
 * TARGET] actions.
 *
 * After a concurrent update, we restart from the first action to look for a
 * new qualifying action to execute. If the join quals originally passed, and
 * the concurrent update caused them to no longer pass, then we switch from
 * the MATCHED to the NOT MATCHED BY SOURCE list of actions before restarting
 * (and setting "*matched" to false).  As a result we may execute a WHEN NOT
 * MATCHED BY SOURCE action, and set "*matched" to false, causing the caller
 * to also execute a WHEN NOT MATCHED [BY TARGET] action.
 */
unsafe fn ExecMergeMatched(
    context: *mut ModifyTableContext,
    resultRelInfo: *mut ResultRelInfo,
    tupleid: *mut ItemPointerData,
    oldtuple: HeapTuple,
    canSetTag: bool,
    matched: *mut bool,
) -> *mut TupleTableSlot {
    let mtstate: *mut ModifyTableState = (*context).mtstate;
    let mergeActions: *mut [*mut List; NUM_MERGE_MATCH_KINDS as usize] = &raw mut (*resultRelInfo).ri_MergeActions;
    let mut lockedtid: ItemPointerData = core::mem::zeroed();
    let mut actionStates: *mut List;
    let mut newslot: *mut TupleTableSlot = core::ptr::null_mut();
    let mut rslot: *mut TupleTableSlot = core::ptr::null_mut();
    let estate: *mut EState = (*context).estate;
    let econtext: *mut ExprContext = (*mtstate).ps.ps_ExprContext;
    let mut isNull: bool = false;
    let epqstate: *mut EPQState = &mut (*mtstate).mt_epqstate;

    /* Expect matched to be true on entry */
    debug_assert!(*matched);

    /*
     * If there are no WHEN MATCHED or WHEN NOT MATCHED BY SOURCE actions, we
     * are done.
     */
    if (*mergeActions)[MERGE_WHEN_MATCHED as usize].is_null()
        && (*mergeActions)[MERGE_WHEN_NOT_MATCHED_BY_SOURCE as usize].is_null()
    {
        return core::ptr::null_mut();
    }

    /*
     * Make tuple and any needed join variables available to ExecQual and
     * ExecProject. The target's existing tuple is installed in the scantuple.
     * This target relation's slot is required only in the case of a MATCHED
     * or NOT MATCHED BY SOURCE tuple and UPDATE/DELETE actions.
     */
    (*econtext).ecxt_scantuple = (*resultRelInfo).ri_oldTupleSlot;
    (*econtext).ecxt_innertuple = (*context).planSlot;
    (*econtext).ecxt_outertuple = core::ptr::null_mut();

    /*
     * This routine is only invoked for matched target rows, so we should
     * either have the tupleid of the target row, or an old tuple from the
     * target wholerow junk attr.
     */
    debug_assert!(!tupleid.is_null() || !oldtuple.is_null());
    ItemPointerSetInvalid(&mut lockedtid);

    if !oldtuple.is_null() {
        debug_assert!(!(*resultRelInfo).ri_needLockTagTuple);
        ExecForceStoreHeapTuple(oldtuple, (*resultRelInfo).ri_oldTupleSlot, false);
    } else {
        if (*resultRelInfo).ri_needLockTagTuple {
            /*
             * This locks even for CMD_DELETE, for CMD_NOTHING, and for tuples
             * that don't match mas_whenqual.  MERGE on system catalogs is a
             * minor use case, so don't bother optimizing those.
             */
            LockTuple((*resultRelInfo).ri_RelationDesc, tupleid, InplaceUpdateTupleLock);
            lockedtid = *tupleid;
        }
        if !table_tuple_fetch_row_version(
            (*resultRelInfo).ri_RelationDesc,
            tupleid,
            SnapshotAny_ptr(),
            (*resultRelInfo).ri_oldTupleSlot,
        ) {
            elog!(ERROR, "failed to fetch the target tuple");
        }
    }

    /*
     * Test the join condition.  If it's satisfied, perform a MATCHED action.
     * Otherwise, perform a NOT MATCHED BY SOURCE action.
     *
     * Note that this join condition will be NULL if there are no NOT MATCHED
     * BY SOURCE actions --- see transform_MERGE_to_join().  In that case, we
     * need only consider MATCHED actions here.
     */
    if ExecQual((*resultRelInfo).ri_MergeJoinCondition, econtext) {
        actionStates = (*mergeActions)[MERGE_WHEN_MATCHED as usize];
    } else {
        actionStates = (*mergeActions)[MERGE_WHEN_NOT_MATCHED_BY_SOURCE as usize];
    }

    /* lmerge_matched: restart loop over action list */
    'lmerge_matched: loop {
        /* TODO(pg-port): foreach list iteration - stub inner body; needs nodes/list.c pg_list infrastructure */
        unimplemented!("TODO(pg-port): ExecMergeMatched foreach(l, actionStates) - needs list iteration (nodes/list.c)");

        #[allow(unreachable_code)]
        {
            break 'lmerge_matched;
        }
    }

    /* out: */
    if ItemPointerIsValid(&lockedtid) {
        UnlockTuple((*resultRelInfo).ri_RelationDesc, &lockedtid as *const _ as *mut _, InplaceUpdateTupleLock);
    }

    rslot
}

/*
 * Execute the first qualifying NOT MATCHED [BY TARGET] action.
 */
unsafe fn ExecMergeNotMatched(
    context: *mut ModifyTableContext,
    resultRelInfo: *mut ResultRelInfo,
    canSetTag: bool,
) -> *mut TupleTableSlot {
    let mtstate: *mut ModifyTableState = (*context).mtstate;
    let econtext: *mut ExprContext = (*mtstate).ps.ps_ExprContext;
    let actionStates: *mut List;
    let mut rslot: *mut TupleTableSlot = core::ptr::null_mut();

    /*
     * For INSERT actions, the root relation's merge action is OK since the
     * INSERT's targetlist and the WHEN conditions can only refer to the
     * source relation and hence it does not matter which result relation we
     * work with.
     *
     * XXX does this mean that we can avoid creating copies of actionStates on
     * partitioned tables, for not-matched actions?
     */
    let actionStates = (*resultRelInfo).ri_MergeActions[MERGE_WHEN_NOT_MATCHED_BY_TARGET as usize];

    /*
     * Make source tuple available to ExecQual and ExecProject. We don't need
     * the target tuple, since the WHEN quals and targetlist can't refer to
     * the target columns.
     */
    (*econtext).ecxt_scantuple = core::ptr::null_mut();
    (*econtext).ecxt_innertuple = (*context).planSlot;
    (*econtext).ecxt_outertuple = core::ptr::null_mut();

    /* TODO(pg-port): foreach list iteration - needs list iteration (nodes/list.c) */
    unimplemented!("TODO(pg-port): ExecMergeNotMatched foreach(l, actionStates) - needs list iteration");

    #[allow(unreachable_code)]
    rslot
}

/*
 * Initialize state for execution of MERGE.
 */
pub unsafe fn ExecInitMerge(mtstate: *mut ModifyTableState, estate: *mut EState) {
    let mergeActionLists: *mut List = (*mtstate).mt_mergeActionLists;
    let mergeJoinConditions: *mut List = (*mtstate).mt_mergeJoinConditions;
    let rootRelInfo: *mut ResultRelInfo = (*mtstate).rootResultRelInfo;
    let mut resultRelInfo: *mut ResultRelInfo;
    let econtext: *mut ExprContext;

    if mergeActionLists == NIL {
        return;
    }

    (*mtstate).mt_merge_subcommands = 0;

    if (*mtstate).ps.ps_ExprContext.is_null() {
        ExecAssignExprContext(estate, &mut (*mtstate).ps);
    }
    let econtext = (*mtstate).ps.ps_ExprContext;

    /*
     * Create a MergeActionState for each action on the mergeActionList and
     * add it to either a list of matched actions or not-matched actions.
     *
     * Similar logic appears in ExecInitPartitionInfo(), so if changing
     * anything here, do so there too.
     *
     * TODO(pg-port): foreach iteration over mergeActionLists - needs list iteration
     */
    unimplemented!("TODO(pg-port): ExecInitMerge foreach iteration - needs list iteration (nodes/list.c)");
}

/*
 * Initializes the tuple slots in a ResultRelInfo for any MERGE action.
 *
 * We mark 'projectNewInfoValid' even though the projections themselves
 * are not initialized here.
 */
pub unsafe fn ExecInitMergeTupleSlots(
    mtstate: *mut ModifyTableState,
    resultRelInfo: *mut ResultRelInfo,
) {
    let estate: *mut EState = (*mtstate).ps.state;

    debug_assert!(!(*resultRelInfo).ri_projectNewInfoValid);

    (*resultRelInfo).ri_oldTupleSlot = table_slot_create(
        (*resultRelInfo).ri_RelationDesc,
        &mut (*estate).es_tupleTable,
    );
    (*resultRelInfo).ri_newTupleSlot = table_slot_create(
        (*resultRelInfo).ri_RelationDesc,
        &mut (*estate).es_tupleTable,
    );
    (*resultRelInfo).ri_projectNewInfoValid = true;
}

/*
 * Process BEFORE EACH STATEMENT triggers
 */
unsafe fn fireBSTriggers(node: *mut ModifyTableState) {
    let plan: *mut ModifyTable = (*node).ps.plan as *mut ModifyTable;
    let resultRelInfo: *mut ResultRelInfo = (*node).rootResultRelInfo;

    match (*node).operation {
        CMD_INSERT => {
            ExecBSInsertTriggers((*node).ps.state, resultRelInfo);
            if (*plan).onConflictAction == ONCONFLICT_UPDATE {
                ExecBSUpdateTriggers((*node).ps.state, resultRelInfo);
            }
        }
        CMD_UPDATE => {
            ExecBSUpdateTriggers((*node).ps.state, resultRelInfo);
        }
        CMD_DELETE => {
            ExecBSDeleteTriggers((*node).ps.state, resultRelInfo);
        }
        CMD_MERGE => {
            if (*node).mt_merge_subcommands & MERGE_INSERT != 0 {
                ExecBSInsertTriggers((*node).ps.state, resultRelInfo);
            }
            if (*node).mt_merge_subcommands & MERGE_UPDATE != 0 {
                ExecBSUpdateTriggers((*node).ps.state, resultRelInfo);
            }
            if (*node).mt_merge_subcommands & MERGE_DELETE != 0 {
                ExecBSDeleteTriggers((*node).ps.state, resultRelInfo);
            }
        }
        _ => {
            elog!(ERROR, "unknown operation");
        }
    }
}

/*
 * Process AFTER EACH STATEMENT triggers
 */
unsafe fn fireASTriggers(node: *mut ModifyTableState) {
    let plan: *mut ModifyTable = (*node).ps.plan as *mut ModifyTable;
    let resultRelInfo: *mut ResultRelInfo = (*node).rootResultRelInfo;

    match (*node).operation {
        CMD_INSERT => {
            if (*plan).onConflictAction == ONCONFLICT_UPDATE {
                ExecASUpdateTriggers(
                    (*node).ps.state,
                    resultRelInfo,
                    (*node).mt_oc_transition_capture,
                );
            }
            ExecASInsertTriggers((*node).ps.state, resultRelInfo, (*node).mt_transition_capture);
        }
        CMD_UPDATE => {
            ExecASUpdateTriggers((*node).ps.state, resultRelInfo, (*node).mt_transition_capture);
        }
        CMD_DELETE => {
            ExecASDeleteTriggers((*node).ps.state, resultRelInfo, (*node).mt_transition_capture);
        }
        CMD_MERGE => {
            if (*node).mt_merge_subcommands & MERGE_DELETE != 0 {
                ExecASDeleteTriggers(
                    (*node).ps.state,
                    resultRelInfo,
                    (*node).mt_transition_capture,
                );
            }
            if (*node).mt_merge_subcommands & MERGE_UPDATE != 0 {
                ExecASUpdateTriggers(
                    (*node).ps.state,
                    resultRelInfo,
                    (*node).mt_transition_capture,
                );
            }
            if (*node).mt_merge_subcommands & MERGE_INSERT != 0 {
                ExecASInsertTriggers(
                    (*node).ps.state,
                    resultRelInfo,
                    (*node).mt_transition_capture,
                );
            }
        }
        _ => {
            elog!(ERROR, "unknown operation");
        }
    }
}

/*
 * Set up the state needed for collecting transition tuples for AFTER
 * triggers.
 */
unsafe fn ExecSetupTransitionCaptureState(mtstate: *mut ModifyTableState, estate: *mut EState) {
    let plan: *mut ModifyTable = (*mtstate).ps.plan as *mut ModifyTable;
    let targetRelInfo: *mut ResultRelInfo = (*mtstate).rootResultRelInfo;

    /* Check for transition tables on the directly targeted relation. */
    (*mtstate).mt_transition_capture = MakeTransitionCaptureState(
        (*targetRelInfo).ri_TrigDesc,
        RelationGetRelid((*targetRelInfo).ri_RelationDesc),
        (*mtstate).operation,
    );
    if (*plan).operation == CMD_INSERT && (*plan).onConflictAction == ONCONFLICT_UPDATE {
        (*mtstate).mt_oc_transition_capture = MakeTransitionCaptureState(
            (*targetRelInfo).ri_TrigDesc,
            RelationGetRelid((*targetRelInfo).ri_RelationDesc),
            CMD_UPDATE,
        );
    }
}

/*
 * ExecPrepareTupleRouting --- prepare for routing one tuple
 *
 * Determine the partition in which the tuple in slot is to be inserted,
 * and return its ResultRelInfo in *partRelInfo.  The return value is
 * a slot holding the tuple of the partition rowtype.
 *
 * This also sets the transition table information in mtstate based on the
 * selected partition.
 */
unsafe fn ExecPrepareTupleRouting(
    mtstate: *mut ModifyTableState,
    estate: *mut EState,
    proute: *mut PartitionTupleRouting,
    targetRelInfo: *mut ResultRelInfo,
    mut slot: *mut TupleTableSlot,
    partRelInfo: *mut *mut ResultRelInfo,
) -> *mut TupleTableSlot {
    let partrel: *mut ResultRelInfo;
    let map: *mut TupleConversionMap;

    /*
     * Lookup the target partition's ResultRelInfo.  If ExecFindPartition does
     * not find a valid partition for the tuple in 'slot' then an error is
     * raised.  An error may also be raised if the found partition is not a
     * valid target for INSERTs.  This is required since a partitioned table
     * UPDATE to another partition becomes a DELETE+INSERT.
     */
    let partrel = ExecFindPartition(mtstate, targetRelInfo, proute, slot, estate);

    /*
     * If we're capturing transition tuples, we might need to convert from the
     * partition rowtype to root partitioned table's rowtype.  But if there
     * are no BEFORE triggers on the partition that could change the tuple, we
     * can just remember the original unconverted tuple to avoid a needless
     * round trip conversion.
     */
    if !(*mtstate).mt_transition_capture.is_null() {
        let has_before_insert_row_trig: bool = !(*partrel).ri_TrigDesc.is_null()
            && (*(*partrel).ri_TrigDesc).trig_insert_before_row;

        (*(*mtstate).mt_transition_capture).tcs_original_insert_tuple = if !has_before_insert_row_trig {
            slot
        } else {
            core::ptr::null_mut()
        };
    }

    /*
     * Convert the tuple, if necessary.
     */
    let map = ExecGetRootToChildMap(partrel, estate);
    if !map.is_null() {
        let new_slot: *mut TupleTableSlot = (*partrel).ri_PartitionTupleSlot;
        slot = execute_attr_map_slot((*map).attrMap, slot, new_slot);
    }

    *partRelInfo = partrel;
    slot
}

/* ----------------------------------------------------------------
 *     ExecModifyTable
 *
 *         Perform table modifications as required, and return RETURNING results
 *         if needed.
 * ----------------------------------------------------------------
 */
pub unsafe extern "C" fn ExecModifyTable(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut ModifyTableState = pstate as *mut ModifyTableState;
    let mut context = ModifyTableContext {
        mtstate: core::ptr::null_mut(),
        epqstate: core::ptr::null_mut(),
        estate: core::ptr::null_mut(),
        planSlot: core::ptr::null_mut(),
        tmfd: core::mem::zeroed(),
        cpDeletedSlot: core::ptr::null_mut(),
        cpUpdateReturningSlot: core::ptr::null_mut(),
    };
    let estate: *mut EState = (*node).ps.state;
    let operation: CmdType = (*node).operation;
    let mut resultRelInfo: *mut ResultRelInfo;
    let subplanstate: *mut PlanState;
    let mut slot: *mut TupleTableSlot;
    let mut oldSlot: *mut TupleTableSlot;
    let mut tuple_ctid: ItemPointerData = core::mem::zeroed();
    let mut oldtupdata: HeapTupleData = core::mem::zeroed();
    let mut oldtuple: HeapTuple;
    let mut tupleid: *mut ItemPointerData;
    let mut tuplock: bool;

    CHECK_FOR_INTERRUPTS();

    /*
     * This should NOT get called during EvalPlanQual; we should have passed a
     * subplan tree to EvalPlanQual, instead.  Use a runtime test not just
     * Assert because this condition is easy to miss in testing.  (Note:
     * although ModifyTable should not get executed within an EvalPlanQual
     * operation, we do have to allow it to be initialized and shut down in
     * case it is within a CTE subplan.  Hence this test must be here, not in
     * ExecInitModifyTable.)
     */
    if !(*estate).es_epq_active.is_null() {
        elog!(ERROR, "ModifyTable should not be called during EvalPlanQual");
    }

    /*
     * If we've already completed processing, don't try to do more.  We need
     * this test because ExecPostprocessPlan might call us an extra time, and
     * our subplan's nodes aren't necessarily robust against being called
     * extra times.
     */
    if (*node).mt_done {
        return core::ptr::null_mut();
    }

    /*
     * On first call, fire BEFORE STATEMENT triggers before proceeding.
     */
    if (*node).fireBSTriggers {
        fireBSTriggers(node);
        (*node).fireBSTriggers = false;
    }

    /* Preload local variables */
    resultRelInfo = (*node).resultRelInfo.add((*node).mt_lastResultIndex as usize);
    let subplanstate = outerPlanState(node as *mut PlanState);

    /* Set global context */
    context.mtstate = node;
    context.epqstate = &mut (*node).mt_epqstate;
    context.estate = estate;

    /*
     * Fetch rows from subplan, and execute the required table modification
     * for each row.
     */
    loop {
        /*
         * Reset the per-output-tuple exprcontext.  This is needed because
         * triggers expect to use that context as workspace.  It's a bit ugly
         * to do this below the top level of the plan, however.  We might need
         * to rethink this later.
         */
        ResetPerTupleExprContext(estate);

        /*
         * Reset per-tuple memory context used for processing on conflict and
         * returning clauses, to free any expression evaluation storage
         * allocated in the previous cycle.
         */
        if !(*pstate).ps_ExprContext.is_null() {
            ResetExprContext((*pstate).ps_ExprContext);
        }

        /*
         * If there is a pending MERGE ... WHEN NOT MATCHED [BY TARGET] action
         * to execute, do so now --- see the comments in ExecMerge().
         */
        if !(*node).mt_merge_pending_not_matched.is_null() {
            context.planSlot = (*node).mt_merge_pending_not_matched;
            context.cpDeletedSlot = core::ptr::null_mut();

            slot = ExecMergeNotMatched(&mut context, (*node).resultRelInfo, (*node).canSetTag);

            /* Clear the pending action */
            (*node).mt_merge_pending_not_matched = core::ptr::null_mut();

            /*
             * If we got a RETURNING result, return it to the caller.  We'll
             * continue the work on next call.
             */
            if !slot.is_null() {
                return slot;
            }

            continue; /* continue with the next tuple */
        }

        /* Fetch the next row from subplan */
        context.planSlot = ExecProcNode(subplanstate);
        context.cpDeletedSlot = core::ptr::null_mut();

        /* No more tuples to process? */
        if TupIsNull(context.planSlot) {
            break;
        }

        /*
         * When there are multiple result relations, each tuple contains a
         * junk column that gives the OID of the rel from which it came.
         * Extract it and select the correct result relation.
         */
        if AttributeNumberIsValid((*node).mt_resultOidAttno as crate::access::attnum::AttrNumber) {
            let mut isNull: bool = false;
            let datum: Datum = ExecGetJunkAttribute(
                context.planSlot,
                (*node).mt_resultOidAttno as crate::access::attnum::AttrNumber,
                &mut isNull,
            );

            if isNull {
                /*
                 * For commands other than MERGE, any tuples having InvalidOid
                 * for tableoid are errors.  For MERGE, we may need to handle
                 * them as WHEN NOT MATCHED clauses if any, so do that.
                 *
                 * Note that we use the node's toplevel resultRelInfo, not any
                 * specific partition's.
                 */
                if operation == CMD_MERGE {
                    EvalPlanQualSetSlot(&mut (*node).mt_epqstate, context.planSlot);

                    slot = ExecMerge(
                        &mut context,
                        (*node).resultRelInfo,
                        core::ptr::null_mut(),
                        core::ptr::null_mut(),
                        (*node).canSetTag,
                    );

                    /* If we got a RETURNING result, return it to the caller.
                     * We'll continue the work on next call. */
                    if !slot.is_null() {
                        return slot;
                    }

                    continue; /* continue with the next tuple */
                }

                elog!(ERROR, "tableoid is NULL");
            }

            let resultoid: Oid = DatumGetObjectId(datum);

            /* If it's not the same as last time, we need to locate the rel */
            if resultoid != (*node).mt_lastResultOid {
                resultRelInfo = ExecLookupResultRelByOid(node, resultoid, false, true);
            }
        }

        /*
         * If resultRelInfo->ri_usesFdwDirectModify is true, all we need to do
         * here is compute the RETURNING expressions.
         */
        if (*resultRelInfo).ri_usesFdwDirectModify {
            debug_assert!(!(*resultRelInfo).ri_projectReturning.is_null());

            /*
             * A scan slot containing the data that was actually inserted,
             * updated or deleted has already been made available to
             * ExecProcessReturning by IterateDirectModify, so no need to
             * provide it here.  The individual old and new slots are not
             * needed, since direct-modify is disabled if the RETURNING list
             * refers to OLD/NEW values.
             */
            debug_assert!(
                (*(*resultRelInfo).ri_projectReturning).pi_state.flags & EEO_FLAG_HAS_OLD == 0
                    && (*(*resultRelInfo).ri_projectReturning).pi_state.flags & EEO_FLAG_HAS_NEW == 0
            );

            slot = ExecProcessReturning(
                &mut context,
                resultRelInfo,
                operation,
                core::ptr::null_mut(),
                core::ptr::null_mut(),
                context.planSlot,
            );

            return slot;
        }

        EvalPlanQualSetSlot(&mut (*node).mt_epqstate, context.planSlot);
        slot = context.planSlot;

        tupleid = core::ptr::null_mut();
        oldtuple = core::ptr::null_mut();

        /*
         * For UPDATE/DELETE/MERGE, fetch the row identity info for the tuple
         * to be updated/deleted/merged.  For a heap relation, that's a TID;
         * otherwise we may have a wholerow junk attr that carries the old
         * tuple in toto.  Keep this in step with the part of
         * ExecInitModifyTable that sets up ri_RowIdAttNo.
         */
        if operation == CMD_UPDATE || operation == CMD_DELETE || operation == CMD_MERGE {
            let mut isNull: bool = false;
            let relkind: c_char = (*(*(*resultRelInfo).ri_RelationDesc).rd_rel).relkind;

            if relkind == RELKIND_RELATION as c_char
                || relkind == RELKIND_MATVIEW as c_char
                || relkind == RELKIND_PARTITIONED_TABLE as c_char
            {
                /*
                 * ri_RowIdAttNo refers to a ctid attribute.  See the comment
                 * in ExecInitModifyTable().
                 */
                debug_assert!(
                    AttributeNumberIsValid((*resultRelInfo).ri_RowIdAttNo)
                        || relkind == RELKIND_PARTITIONED_TABLE as c_char
                );
                let datum = ExecGetJunkAttribute(
                    slot,
                    (*resultRelInfo).ri_RowIdAttNo,
                    &mut isNull,
                );

                if isNull {
                    if operation == CMD_MERGE {
                        EvalPlanQualSetSlot(&mut (*node).mt_epqstate, context.planSlot);
                        slot = ExecMerge(
                            &mut context,
                            (*node).resultRelInfo,
                            core::ptr::null_mut(),
                            core::ptr::null_mut(),
                            (*node).canSetTag,
                        );
                        if !slot.is_null() {
                            return slot;
                        }
                        continue;
                    }
                    elog!(ERROR, "ctid is NULL");
                }

                tupleid = DatumGetPointer(datum) as *mut ItemPointerData;
                tuple_ctid = *tupleid; /* be sure we don't free ctid!! */
                tupleid = &mut tuple_ctid;
            } else if AttributeNumberIsValid((*resultRelInfo).ri_RowIdAttNo) {
                let datum = ExecGetJunkAttribute(
                    slot,
                    (*resultRelInfo).ri_RowIdAttNo,
                    &mut isNull,
                );

                if isNull {
                    if operation == CMD_MERGE {
                        EvalPlanQualSetSlot(&mut (*node).mt_epqstate, context.planSlot);
                        slot = ExecMerge(
                            &mut context,
                            (*node).resultRelInfo,
                            core::ptr::null_mut(),
                            core::ptr::null_mut(),
                            (*node).canSetTag,
                        );
                        if !slot.is_null() {
                            return slot;
                        }
                        continue;
                    }
                    elog!(ERROR, "wholerow is NULL");
                }

                oldtupdata.t_data = DatumGetHeapTupleHeader(datum);
                oldtupdata.t_len = HeapTupleHeaderGetDatumLength(oldtupdata.t_data);
                ItemPointerSetInvalid(&mut oldtupdata.t_self);
                /* Historically, view triggers see invalid t_tableOid. */
                oldtupdata.t_tableOid = if relkind == RELKIND_VIEW as c_char {
                    InvalidOid
                } else {
                    RelationGetRelid((*resultRelInfo).ri_RelationDesc)
                };

                oldtuple = &mut oldtupdata;
            } else {
                /* Only foreign tables are allowed to omit a row-ID attr */
                debug_assert_eq!(relkind, RELKIND_FOREIGN_TABLE as c_char);
            }
        }

        match operation {
            CMD_INSERT => {
                /* Initialize projection info if first time for this table */
                if unlikely(!(*resultRelInfo).ri_projectNewInfoValid) {
                    ExecInitInsertProjection(node, resultRelInfo);
                }
                slot = ExecGetInsertNewTuple(resultRelInfo, context.planSlot);
                slot = ExecInsert(
                    &mut context,
                    resultRelInfo,
                    slot,
                    (*node).canSetTag,
                    core::ptr::null_mut(),
                    core::ptr::null_mut(),
                );
            }

            CMD_UPDATE => {
                tuplock = false;

                /* Initialize projection info if first time for this table */
                if unlikely(!(*resultRelInfo).ri_projectNewInfoValid) {
                    ExecInitUpdateProjection(node, resultRelInfo);
                }

                /*
                 * Make the new tuple by combining plan's output tuple with
                 * the old tuple being updated.
                 */
                let oldSlot = (*resultRelInfo).ri_oldTupleSlot;
                if !oldtuple.is_null() {
                    debug_assert!(!(*resultRelInfo).ri_needLockTagTuple);
                    /* Use the wholerow junk attr as the old tuple. */
                    ExecForceStoreHeapTuple(oldtuple, oldSlot, false);
                } else {
                    /* Fetch the most recent version of old tuple. */
                    let relation: Relation = (*resultRelInfo).ri_RelationDesc;

                    if (*resultRelInfo).ri_needLockTagTuple {
                        LockTuple(relation, tupleid, InplaceUpdateTupleLock);
                        tuplock = true;
                    }
                    if !table_tuple_fetch_row_version(relation, tupleid, SnapshotAny_ptr(), oldSlot)
                    {
                        elog!(ERROR, "failed to fetch tuple being updated");
                    }
                }
                slot = ExecGetUpdateNewTuple(resultRelInfo, context.planSlot, oldSlot);

                /* Now apply the update. */
                slot = ExecUpdate(
                    &mut context,
                    resultRelInfo,
                    tupleid,
                    oldtuple,
                    oldSlot,
                    slot,
                    (*node).canSetTag,
                );
                if tuplock {
                    UnlockTuple(
                        (*resultRelInfo).ri_RelationDesc,
                        tupleid,
                        InplaceUpdateTupleLock,
                    );
                }
            }

            CMD_DELETE => {
                slot = ExecDelete(
                    &mut context,
                    resultRelInfo,
                    tupleid,
                    oldtuple,
                    true,
                    false,
                    (*node).canSetTag,
                    core::ptr::null_mut(),
                    core::ptr::null_mut(),
                    core::ptr::null_mut(),
                );
            }

            CMD_MERGE => {
                slot = ExecMerge(
                    &mut context,
                    resultRelInfo,
                    tupleid,
                    oldtuple,
                    (*node).canSetTag,
                );
            }

            _ => {
                elog!(ERROR, "unknown operation");
            }
        }

        /*
         * If we got a RETURNING result, return it to caller.  We'll continue
         * the work on next call.
         */
        if !slot.is_null() {
            return slot;
        }
    }

    /*
     * Insert remaining tuples for batch insert.
     */
    if (*estate).es_insert_pending_result_relations != NIL {
        ExecPendingInserts(estate);
    }

    /*
     * We're done, but fire AFTER STATEMENT triggers before exiting.
     */
    fireASTriggers(node);

    (*node).mt_done = true;

    core::ptr::null_mut()
}

/*
 * ExecLookupResultRelByOid
 *         If the table with given OID is among the result relations to be
 *         updated by the given ModifyTable node, return its ResultRelInfo.
 *
 * If not found, return NULL if missing_ok, else raise error.
 *
 * If update_cache is true, then upon successful lookup, update the node's
 * one-element cache.  ONLY ExecModifyTable may pass true for this.
 */
pub unsafe fn ExecLookupResultRelByOid(
    node: *mut ModifyTableState,
    resultoid: Oid,
    missing_ok: bool,
    update_cache: bool,
) -> *mut ResultRelInfo {
    if !(*node).mt_resultOidHash.is_null() {
        /* Use the pre-built hash table to locate the rel */
        let mtlookup: *mut MTTargetRelLookup = hash_search(
            (*node).mt_resultOidHash as *mut crate::utils::hash::dynahash::HTAB,
            &resultoid as *const Oid as *const c_void,
            HASH_FIND,
            core::ptr::null_mut(),
        ) as *mut MTTargetRelLookup;

        if !mtlookup.is_null() {
            if update_cache {
                (*node).mt_lastResultOid = resultoid;
                (*node).mt_lastResultIndex = (*mtlookup).relationIndex;
            }
            return (*node).resultRelInfo.add((*mtlookup).relationIndex as usize);
        }
    } else {
        /* With few target rels, just search the ResultRelInfo array */
        for ndx in 0..(*node).mt_nrels {
            let rInfo: *mut ResultRelInfo = (*node).resultRelInfo.add(ndx as usize);

            if RelationGetRelid((*rInfo).ri_RelationDesc) == resultoid {
                if update_cache {
                    (*node).mt_lastResultOid = resultoid;
                    (*node).mt_lastResultIndex = ndx;
                }
                return rInfo;
            }
        }
    }

    if !missing_ok {
        elog!(ERROR, "incorrect result relation OID {}", resultoid);
    }

    core::ptr::null_mut()
}

/* ----------------------------------------------------------------
 *     ExecInitModifyTable
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitModifyTable(
    node: *mut ModifyTable,
    estate: *mut EState,
    eflags: c_int,
) -> *mut ModifyTableState {
    let mut mtstate: *mut ModifyTableState;
    let subplan: *mut Plan = outerPlan(node as *mut Plan);
    let operation: CmdType = (*node).operation;
    let total_nrels: c_int = list_length((*node).resultRelations);
    let mut nrels: c_int;
    let mut resultRelations: *mut List = NIL;
    let mut withCheckOptionLists: *mut List = NIL;
    let mut returningLists: *mut List = NIL;
    let mut updateColnosLists: *mut List = NIL;
    let mut mergeActionLists: *mut List = NIL;
    let mut mergeJoinConditions: *mut List = NIL;
    let mut resultRelInfo: *mut ResultRelInfo;
    let mut arowmarks: *mut List = NIL;
    let mut i: c_int;
    let rel: Relation;

    /* check for unsupported flags */
    debug_assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);

    /*
     * Only consider unpruned relations for initializing their ResultRelInfo
     * struct and other fields such as withCheckOptions, etc.
     *
     * TODO(pg-port): foreach iteration - needs list iteration (nodes/list.c)
     */
    unimplemented!("TODO(pg-port): ExecInitModifyTable foreach(l, node->resultRelations) - needs list iteration (nodes/list.c)");

    #[allow(unreachable_code)]
    {
        nrels = list_length(resultRelations);
        debug_assert!(nrels > 0);

        /* create state structure */
        mtstate = makeNode!(ModifyTableState, T_ModifyTableState);
        (*mtstate).ps.plan = node as *mut Plan;
        (*mtstate).ps.state = estate;
        // ExecModifyTable is extern "C"; ExecProcNodeMtd is Rust ABI.
        // The C and Rust ABIs match on all supported platforms for this signature.
        (*mtstate).ps.ExecProcNode = Some(core::mem::transmute::<
            unsafe extern "C" fn(*mut PlanState) -> *mut TupleTableSlot,
            unsafe fn(*mut PlanState) -> *mut TupleTableSlot,
        >(ExecModifyTable));

        (*mtstate).operation = operation;
        (*mtstate).canSetTag = (*node).canSetTag;
        (*mtstate).mt_done = false;

        (*mtstate).mt_nrels = nrels;
        (*mtstate).resultRelInfo = palloc(nrels as usize * core::mem::size_of::<ResultRelInfo>())
            as *mut ResultRelInfo;

        (*mtstate).mt_merge_pending_not_matched = core::ptr::null_mut();
        (*mtstate).mt_merge_inserted = 0.0;
        (*mtstate).mt_merge_updated = 0.0;
        (*mtstate).mt_merge_deleted = 0.0;
        (*mtstate).mt_updateColnosLists = updateColnosLists;
        (*mtstate).mt_mergeActionLists = mergeActionLists;
        (*mtstate).mt_mergeJoinConditions = mergeJoinConditions;

        /* Resolve the target relation. */
        if (*node).rootRelation > 0 {
            debug_assert!(bms_is_member((*node).rootRelation as c_int, (*estate).es_unpruned_relids));
            (*mtstate).rootResultRelInfo = makeNode!(ResultRelInfo, T_ResultRelInfo);
            ExecInitResultRelation(estate, (*mtstate).rootResultRelInfo, (*node).rootRelation);
        } else {
            debug_assert_eq!(list_length((*node).resultRelations), 1);
            debug_assert_eq!(list_length(resultRelations), 1);
            (*mtstate).rootResultRelInfo = (*mtstate).resultRelInfo;
            ExecInitResultRelation(
                estate,
                (*mtstate).resultRelInfo,
                linitial_int(resultRelations) as crate::c::Index,
            );
        }

        /* set up epqstate with dummy subplan data for the moment */
        EvalPlanQualInit(
            &mut (*mtstate).mt_epqstate,
            estate,
            core::ptr::null_mut(),
            NIL,
            (*node).epqParam,
            resultRelations,
        );
        (*mtstate).fireBSTriggers = true;

        /*
         * Build state for collecting transition tuples.  This requires having a
         * valid trigger query context, so skip it in explain-only mode.
         */
        if eflags & EXEC_FLAG_EXPLAIN_ONLY == 0 {
            ExecSetupTransitionCaptureState(mtstate, estate);
        }

        /*
         * Open all the result relations and initialize the ResultRelInfo structs.
         * TODO(pg-port): foreach iteration - needs list iteration
         */
        // Stub: iteration over resultRelations requires list infrastructure
        // Would call ExecInitResultRelation, CheckValidResultRel, etc. for each rel

        /* Now we may initialize the subplan. */
        *outerPlanState_mut(mtstate as *mut PlanState) = ExecInitNode(subplan, estate, eflags);

        /* For a MERGE command, initialize its state */
        if (*mtstate).operation == CMD_MERGE {
            ExecInitMerge(mtstate, estate);
        }

        EvalPlanQualSetPlan(&mut (*mtstate).mt_epqstate, subplan, arowmarks);

        /* Use hash table for result-rel lookup if many rels */
        #[cfg(debug_assertions)]
        const MT_NRELS_HASH: usize = 4;
        #[cfg(not(debug_assertions))]
        const MT_NRELS_HASH: usize = 64;

        if nrels as usize >= MT_NRELS_HASH {
            let mut hash_ctl: HASHCTL = core::mem::zeroed();
            hash_ctl.keysize = core::mem::size_of::<Oid>();
            hash_ctl.entrysize = core::mem::size_of::<MTTargetRelLookup>();
            hash_ctl.hcxt = CurrentMemoryContext as *mut _;
            (*mtstate).mt_resultOidHash = hash_create(
                b"ModifyTable target hash\0".as_ptr() as *const c_char,
                nrels as c_long,
                &mut hash_ctl,
                HASH_ELEM | HASH_BLOBS | HASH_CONTEXT,
            ) as *mut crate::nodes::execnodes::HTAB;
            for idx in 0..nrels as usize {
                let mut found: bool = false;
                resultRelInfo = (*mtstate).resultRelInfo.add(idx);
                let hashkey: Oid = RelationGetRelid((*resultRelInfo).ri_RelationDesc);
                let mtlookup: *mut MTTargetRelLookup = hash_search(
                    (*mtstate).mt_resultOidHash as *mut crate::utils::hash::dynahash::HTAB,
                    &hashkey as *const Oid as *const c_void,
                    HASH_ENTER,
                    &mut found,
                ) as *mut MTTargetRelLookup;
                debug_assert!(!found);
                (*mtlookup).relationIndex = idx as c_int;
            }
        } else {
            (*mtstate).mt_resultOidHash = core::ptr::null_mut();
        }

        /*
         * Determine batch size for FDW inserts.
         */
        if operation == CMD_INSERT {
            debug_assert_eq!(total_nrels, 1);
            resultRelInfo = (*mtstate).resultRelInfo;
            if !(*resultRelInfo).ri_usesFdwDirectModify
                && !(*resultRelInfo).ri_FdwRoutine.is_null()
                && (*(*resultRelInfo).ri_FdwRoutine).GetForeignModifyBatchSize.is_some()
                && (*(*resultRelInfo).ri_FdwRoutine).ExecForeignBatchInsert.is_some()
            {
                let fdw = (*resultRelInfo).ri_FdwRoutine;
                (*resultRelInfo).ri_BatchSize =
                    ((*fdw).GetForeignModifyBatchSize.unwrap())(resultRelInfo);
                debug_assert!((*resultRelInfo).ri_BatchSize >= 1);
            } else {
                (*resultRelInfo).ri_BatchSize = 1;
            }
        }

        /*
         * Lastly, if this is not the primary (canSetTag) ModifyTable node, add it
         * to estate->es_auxmodifytables so that it will be run to completion by
         * ExecPostprocessPlan.
         */
        if !(*mtstate).canSetTag {
            (*estate).es_auxmodifytables = lcons(mtstate as *mut c_void, (*estate).es_auxmodifytables);
        }

        mtstate
    }
}

/* ----------------------------------------------------------------
 *     ExecEndModifyTable
 *
 *         Shuts down the plan.
 *
 *         Returns nothing of interest.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecEndModifyTable(node: *mut ModifyTableState) {
    /*
     * Allow any FDWs to shut down
     */
    for i in 0..(*node).mt_nrels {
        let resultRelInfo: *mut ResultRelInfo = (*node).resultRelInfo.add(i as usize);

        if !(*resultRelInfo).ri_usesFdwDirectModify
            && !(*resultRelInfo).ri_FdwRoutine.is_null()
            && (*(*resultRelInfo).ri_FdwRoutine).EndForeignModify.is_some()
        {
            let fdw = (*resultRelInfo).ri_FdwRoutine;
            ((*fdw).EndForeignModify.unwrap())((*node).ps.state, resultRelInfo);
        }

        /*
         * Cleanup the initialized batch slots. This only matters for FDWs
         * with batching, but the other cases will have ri_NumSlotsInitialized == 0.
         */
        for j in 0..(*resultRelInfo).ri_NumSlotsInitialized as usize {
            ExecDropSingleTupleTableSlot(*(*resultRelInfo).ri_Slots.add(j));
            ExecDropSingleTupleTableSlot(*(*resultRelInfo).ri_PlanSlots.add(j));
        }
    }

    /*
     * Close all the partitioned tables, leaf partitions, and their indices
     * and release the slot used for tuple routing, if set.
     */
    if !(*node).mt_partition_tuple_routing.is_null() {
        ExecCleanupTupleRouting(node, (*node).mt_partition_tuple_routing);

        if !(*node).mt_root_tuple_slot.is_null() {
            ExecDropSingleTupleTableSlot((*node).mt_root_tuple_slot);
        }
    }

    /* Terminate EPQ execution if active */
    EvalPlanQualEnd(&mut (*node).mt_epqstate);

    /* shut down subplan */
    ExecEndNode(outerPlanState(node as *mut PlanState));
}

pub unsafe fn ExecReScanModifyTable(_node: *mut ModifyTableState) {
    /*
     * Currently, we don't need to support rescan on ModifyTable nodes. The
     * semantics of that would be a bit debatable anyway.
     */
    elog!(ERROR, "ExecReScanModifyTable is not implemented");
}
