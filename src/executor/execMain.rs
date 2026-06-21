/*-------------------------------------------------------------------------
 *
 * execMain.c
 *    top level executor interface routines
 *
 * INTERFACE ROUTINES
 *    ExecutorStart()
 *    ExecutorRun()
 *    ExecutorFinish()
 *    ExecutorEnd()
 *
 *    These four procedures are the external interface to the executor.
 *    In each case, the query descriptor is required as an argument.
 *
 *    ExecutorStart must be called at the beginning of execution of any
 *    query plan and ExecutorEnd must always be called at the end of
 *    execution of a plan (unless it is aborted due to error).
 *
 *    ExecutorRun accepts direction and count arguments that specify whether
 *    the plan is to be executed forwards, backwards, and for how many tuples.
 *    In some cases ExecutorRun may be called multiple times to process all
 *    the tuples for a plan.  It is also acceptable to stop short of executing
 *    the whole plan (but only if it is a SELECT).
 *
 *    ExecutorFinish must be called after the final ExecutorRun call and
 *    before ExecutorEnd.  This can be omitted only in case of EXPLAIN,
 *    which should also omit ExecutorRun.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/executor/execMain.c
 *
 *-------------------------------------------------------------------------
 */

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
#![allow(unused_mut)]
#![allow(unused_unsafe)]
#![allow(unreachable_code)]

use crate::prelude::*;

use core::ffi::{c_char, c_int, CStr};
use core::mem::size_of;

use crate::postgres_ext::{InvalidOid, Oid};
use crate::c::{uint64, Index};

use crate::postgres::{Datum, DatumGetObjectId, DatumGetPointer};

use crate::nodes::nodes::{CmdType, Node, NodeTag, OnConflictAction};
use crate::nodes::nodes::CmdType::*;
use crate::nodes::nodes::OnConflictAction::*;
use crate::nodes::pg_list::{List, NIL};
use crate::nodes::pg_list::{list_length, lfirst_int, lfirst_oid, list_head, lnext};
use crate::{foreach, current_cell, lfirst_node, linitial_node, makeNode};
use crate::nodes::pg_list::lappend;
use crate::nodes::bitmapset::{
    Bitmapset, bms_is_empty, bms_is_member, bms_add_member, bms_copy,
    bms_num_members, bms_overlap, bms_union, bms_next_member,
};
use crate::nodes::parsenodes::{
    RTEPermissionInfo, RangeTblEntry, WCOKind, AclMode, WithCheckOption,
    ACL_SELECT, ACL_INSERT, ACL_UPDATE,
};
use crate::nodes::parsenodes::WCOKind::*;
use crate::nodes::plannodes::{Plan, PlannedStmt, PlanRowMark, RowMarkType};
use crate::nodes::plannodes::RowMarkType::*;
use crate::nodes::primnodes::{Expr, MergeAction, MergeMatchKind};
use crate::nodes::primnodes::MergeMatchKind::*;
use crate::nodes::execnodes::{
    EState, EPQState, ExprContext, ExprState, ProjectionInfo,
    PlanState, ResultRelInfo, ExecRowMark, ExecAuxRowMark,
    JunkFilter, TupleTableSlot,
};

use crate::access::sdir::ScanDirection;
use crate::access::attnum::{AttrNumber, InvalidAttrNumber, AttributeNumberIsValid};
use crate::access::sysattr::FirstLowInvalidHeapAttributeNumber;
use crate::access::common::tupdesc::TupleDesc;
use crate::access::common::attmap::{AttrMap, build_attrmap_by_name_if_req};
use crate::access::common::tupconvert::execute_attr_map_slot;

use crate::executor::execdesc::QueryDesc;
use crate::executor::tuptable::{TupIsNull, ExecCopySlot, ExecClearTuple, ExecMaterializeSlot};
use crate::executor::execTuples::TTSOpsVirtual;
use crate::executor::execJunk::{ExecFindJunkAttributeInTlist, ExecGetJunkAttribute};
use crate::executor::instrument::{InstrStartNode, InstrStopNode};
use crate::executor::executor::{
    EXEC_FLAG_EXPLAIN_ONLY, EXEC_FLAG_SKIP_TRIGGERS, EXEC_FLAG_BACKWARD,
    EXEC_FLAG_REWIND, EXEC_FLAG_MARK,
    ExecutorStart_hook, ExecutorRun_hook, ExecutorFinish_hook, ExecutorEnd_hook,
    ExecutorCheckPerms_hook,
    ExecInitNode, ExecEndNode, ExecProcNode, ExecReScan, ExecShutdownNode,
    ExecInitRangeTable, exec_rt_fetch, ExecGetRangeTableRelation,
    ExecGetResultType, ExecInitExtraTupleSlot,
    ExecInitJunkFilter, ExecFilterJunk,
    ExecOpenIndices, ExecCloseIndices,
    ExecGetInsertedCols, ExecGetUpdatedCols, ExecGetAllUpdatedCols,
    ExecPrepareExpr, ExecPrepareCheck,
    ExecCheck, ExecQual,
    CreateExecutorState, FreeExecutorState,
    GetPerTupleExprContext, ResetPerTupleExprContext,
};
use crate::executor::execTuples::ExecResetTupleTable;
use crate::executor::nodeSubplan::ExecSetParamPlanMulti;

use crate::tcop::dest::DestReceiver;
use crate::utils::rel::{Relation, RelationGetRelid, RelationGetDescr, RelationGetRelationName};
use crate::utils::palloc::{palloc, palloc0, MemoryContextSwitchTo};
use crate::utils::mmgr::memnodes::MemoryContext;
use crate::lib::stringinfo::{
    StringInfoData, initStringInfo, appendStringInfoChar,
    appendStringInfoString, appendBinaryStringInfo,
};
use crate::utils::misc::rls::{check_enable_rls, RLS_ENABLED};
use crate::utils::adt::acl::{
    AclResult, AclResult::*, AclMaskHow::*,
};
use crate::catalog::aclchk::aclcheck_error;
use crate::catalog::pg_class::{
    RELKIND_RELATION, RELKIND_PARTITIONED_TABLE, RELKIND_SEQUENCE,
    RELKIND_TOASTVALUE, RELKIND_VIEW, RELKIND_MATVIEW, RELKIND_FOREIGN_TABLE,
};
use crate::catalog::catalog::IsInplaceUpdateRelation;

// ---------------------------------------------------------------------------
// Forward declarations of functions defined only in this file (module-private)
// ---------------------------------------------------------------------------
// InitPlan, CheckValidRowMarkRel, ExecPostprocessPlan, ExecEndPlan,
// ExecutePlan, ExecCheckPermissionsModified, ExecCheckXactReadOnly,
// EvalPlanQualStart, ReportNotNullViolationError  are below.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Stubs for dependencies not yet ported.
// ---------------------------------------------------------------------------

/// TODO(pg-port): real sym lives in pgstat.h
unsafe fn pgstat_report_query_id(_queryId: u64, _force: bool) {
    crate::utils::activity::backend_status::pgstat_report_query_id(_queryId as _, _force as _)
}

/// TODO(pg-port): real sym lives in pgstat.h (PgStat_Counter)
type PgStat_Counter = i64;
unsafe fn pgstat_update_parallel_workers_stats(
    _launched: PgStat_Counter,
    _launched2: PgStat_Counter,
) {
    crate::utils::activity::pgstat_database::pgstat_update_parallel_workers_stats(_launched as _, _launched2 as _)
}

/// TODO(pg-port): access/xact.h
pub static mut XactReadOnly: bool = false;

/// TODO(pg-port): access/xact.h
unsafe fn IsInParallelMode() -> bool {
    false // TODO(pg-port): stub
}

/// TODO(pg-port): access/xact.h
unsafe fn GetCurrentCommandId(_force: bool) -> u32 {
    crate::access::transam::xact::GetCurrentCommandId(_force as _) as _
}

/// TODO(pg-port): utils/snapmgr.h - Snapshot type
pub type Snapshot = *mut crate::nodes::execnodes::SnapshotData;

/// TODO(pg-port): utils/snapmgr.h
unsafe fn GetActiveSnapshot() -> Snapshot {
    crate::utils::time::snapmgr::GetActiveSnapshot() as _
}

/// TODO(pg-port): utils/snapmgr.h
unsafe fn RegisterSnapshot(_snap: Snapshot) -> Snapshot {
    crate::utils::time::snapmgr::RegisterSnapshot(_snap as _) as _
}

/// TODO(pg-port): utils/snapmgr.h
unsafe fn UnregisterSnapshot(_snap: Snapshot) {
    crate::utils::time::snapmgr::UnregisterSnapshot(_snap as _)
}

/// TODO(pg-port): commands/trigger.h
#[no_mangle]
unsafe fn AfterTriggerBeginQuery() {
    // TODO(pg-port): stub
}

/// TODO(pg-port): commands/trigger.h
#[no_mangle]
unsafe fn AfterTriggerEndQuery(_estate: *mut EState) {
    // TODO(pg-port): stub
}

/// TODO(pg-port): utils/acl.h
unsafe fn GetUserId() -> Oid { crate::utils::init::miscinit::GetUserId() }

/// TODO(pg-port): utils/acl.h / aclchk.c
unsafe fn pg_class_aclmask(
    table_oid: Oid,
    roleid: Oid,
    mode: AclMode,
    how: crate::utils::adt::acl::AclMaskHow,
) -> AclMode {
    crate::catalog::aclchk::pg_class_aclmask(table_oid, roleid, mode, how)
}

/// TODO(pg-port): utils/acl.h
unsafe fn pg_attribute_aclcheck(
    _table_oid: Oid,
    _attnum: AttrNumber,
    _roleid: Oid,
    _mode: AclMode,
) -> AclResult {
    crate::catalog::aclchk::pg_attribute_aclcheck(_table_oid as _, _attnum as _, _roleid as _, _mode as _)
}

/// TODO(pg-port): utils/acl.h
unsafe fn pg_attribute_aclcheck_all(
    _table_oid: Oid,
    _roleid: Oid,
    _mode: AclMode,
    _how: crate::utils::adt::acl::AclMaskHow,
) -> AclResult {
    ACLCHECK_OK // TODO(pg-port): stub
}

/// TODO(pg-port): utils/acl.h
unsafe fn pg_class_aclcheck(_table_oid: Oid, _roleid: Oid, _mode: AclMode) -> AclResult {
    crate::catalog::aclchk::pg_class_aclcheck(_table_oid as _, _roleid as _, _mode as _)
}

/// TODO(pg-port): utils/lsyscache.h
unsafe fn get_rel_namespace(_relid: Oid) -> Oid {
    crate::utils::cache::lsyscache::get_rel_namespace(_relid as _) as _
}

/// TODO(pg-port): utils/lsyscache.h
unsafe fn get_rel_name(_relid: Oid) -> *mut c_char {
    crate::utils::cache::lsyscache::get_rel_name(_relid as _) as _
}

/// TODO(pg-port): utils/lsyscache.h
unsafe fn get_rel_relkind(_relid: Oid) -> c_char {
    crate::utils::cache::lsyscache::get_rel_relkind(_relid as _) as _
}

/// TODO(pg-port): utils/acl.h
unsafe fn get_relkind_objtype(_relkind: c_char) -> crate::nodes::parsenodes::ObjectType {
    crate::nodes::parsenodes::ObjectType::OBJECT_TABLE // TODO(pg-port): stub
}

/// TODO(pg-port): catalog/namespace.h
unsafe fn isTempNamespace(_namespaceId: Oid) -> bool {
    crate::catalog::namespace::isTempNamespace(_namespaceId as _) as _
}

/// TODO(pg-port): tcop/utility.h
unsafe fn CreateCommandName(_utilityStmt: *mut Node) -> *const c_char {
    c"".as_ptr() // TODO(pg-port): stub
}

/// TODO(pg-port): tcop/utility.h
unsafe fn PreventCommandIfReadOnly(_cmdname: *const c_char) {
    crate::tcop::utility::PreventCommandIfReadOnly(_cmdname as _)
}

/// TODO(pg-port): tcop/utility.h
unsafe fn PreventCommandIfParallelMode(_cmdname: *const c_char) {
    crate::tcop::utility::PreventCommandIfParallelMode(_cmdname as _)
}

/// TODO(pg-port): executor/execPartition.h
unsafe fn ExecDoInitialPruning(_estate: *mut EState) {
    crate::executor::execPartition::ExecDoInitialPruning(_estate as _)
}

/// TODO(pg-port): catalog/partition.h
unsafe fn get_partition_ancestors(_relid: Oid) -> *mut List {
    NIL // TODO(pg-port): stub
}

/// TODO(pg-port): commands/trigger.h
unsafe fn CopyTriggerDesc(_trigdesc: *mut core::ffi::c_void) -> *mut core::ffi::c_void {
    core::ptr::null_mut() // TODO(pg-port): stub
}

/// TODO(pg-port): foreign/fdwapi.h
type FdwRoutine = core::ffi::c_void;

/// TODO(pg-port): foreign/fdwapi.h
unsafe fn GetFdwRoutineForRelation(_rel: Relation, _needInfo: bool) -> *mut FdwRoutine {
    crate::foreign::foreign::GetFdwRoutineForRelation(_rel as _, _needInfo as _) as _
}

/// TODO(pg-port): executor/instrument.h
unsafe fn InstrAlloc(_n: c_int, _instrument_options: c_int, _async_mode: bool) -> *mut core::ffi::c_void {
    crate::executor::instrument::InstrAlloc(_n as _, _instrument_options as _, _async_mode as _) as _
}

/// TODO(pg-port): access/tableam.h
unsafe fn table_open(_relid: Oid, _lockmode: c_int) -> Relation {
    core::ptr::null_mut() // TODO(pg-port): stub
}

unsafe fn table_close(relation: Relation, lockmode: c_int) {
    crate::access::table::table::table_close(relation, lockmode)
}

/// TODO(pg-port): access/tableam.h
unsafe fn table_slot_create(
    _relation: Relation,
    _tuple_table: *mut *mut List,
) -> *mut TupleTableSlot {
    crate::access::table::tableam::table_slot_create(_relation as _, _tuple_table as _) as _
}

/// TODO(pg-port): access/tableam.h
unsafe fn table_tuple_fetch_row_version(
    _relation: Relation,
    _tid: *mut crate::storage::itemptr::ItemPointerData,
    _snapshot: Snapshot,
    _slot: *mut TupleTableSlot,
) -> bool {
    false // TODO(pg-port): stub
}

pub const NoLock: c_int = 0;

/// TODO(pg-port): rewrite/rewriteHandler.h - view_has_instead_trigger
unsafe fn view_has_instead_trigger(
    _resultRel: Relation,
    _operation: CmdType,
    _mergeActions: *mut List,
) -> bool {
    false // TODO(pg-port): stub
}

/// TODO(pg-port): rewrite/rewriteHandler.h - error_view_not_updatable
unsafe fn error_view_not_updatable(
    _resultRel: Relation,
    _operation: CmdType,
    _mergeActions: *mut List,
    _detail: *const c_char,
) {
    // TODO(pg-port): stub
}

/// TODO(pg-port): commands/matview.h
unsafe fn MatViewIncrementalMaintenanceIsEnabled() -> bool {
    crate::commands::matview::MatViewIncrementalMaintenanceIsEnabled() as _
}

/// TODO(pg-port): access/reloptions.h
unsafe fn CheckCmdReplicaIdentity(_rel: Relation, _cmd: CmdType) {
    // TODO(pg-port): stub
}

/// TODO(pg-port): utils/rel.h
unsafe fn RelationGetIndexAttrBitmap(
    _relation: Relation,
    _attrKind: c_int,
) -> *mut Bitmapset {
    unimplemented!()
}
pub const INDEX_ATTR_BITMAP_KEY: c_int = 1;

/// TODO(pg-port): executor/execTuples.h
unsafe fn MakeTupleTableSlot(
    _tupdesc: TupleDesc,
    _tts_ops: *const crate::executor::tuptable::TupleTableSlotOps,
) -> *mut TupleTableSlot {
    crate::executor::execTuples::MakeTupleTableSlot(_tupdesc as _, _tts_ops as _) as _
}

/// TODO(pg-port): executor/execUtils.h
unsafe fn ExecStoreHeapTupleDatum(_datum: Datum, _slot: *mut TupleTableSlot) {
    crate::executor::execTuples::ExecStoreHeapTupleDatum(_datum as _, _slot as _)
}

/// TODO(pg-port): catalog/pg_constraint.h - ConstrCheck type
#[repr(C)]
pub struct ConstrCheck {
    pub ccname: *mut c_char,
    pub ccbin: *mut c_char,
    pub ccsrc: *mut c_char,
    pub ccvalid: bool,
    pub ccnoinherit: bool,
    pub ccenforced: bool,
}

/// TODO(pg-port): nodes/primnodes.h - NullTest node
#[repr(C)]
pub struct NullTest {
    pub xpr: Expr,
    pub arg: *mut Expr,
    pub nulltesttype: c_int,
    pub argisrow: bool,
    pub location: c_int,
}
pub const IS_NOT_NULL: c_int = 1;
// makeNode!(NullTest, T_NullTest) -- T_NullTest tag

/// TODO(pg-port): nodes/parsenodes.h - build_generation_expression
unsafe fn build_generation_expression(_rel: Relation, _attnum: AttrNumber) -> *mut Node {
    crate::rewrite::rewriteHandler::build_generation_expression(_rel as _, _attnum as _) as _
}

/// TODO(pg-port): parser/parse_relation.h - stringToNode
unsafe fn stringToNode(_str: *mut c_char) -> *mut core::ffi::c_void {
    crate::nodes::read::stringToNode(_str as _) as _
}

/// TODO(pg-port): catalog/partition.h - expand_generated_columns_in_expr
unsafe fn expand_generated_columns_in_expr(
    _node: *mut Node,
    _rel: Relation,
    _varno: c_int,
) -> *mut Node {
    crate::rewrite::rewriteHandler::expand_generated_columns_in_expr(_node as _, _rel as _, _varno as _) as _
}

/// TODO(pg-port): utils/lsyscache.h - getTypeOutputInfo
unsafe fn getTypeOutputInfo(_typid: Oid, _typoutput: *mut Oid, _typIsVarlena: *mut bool) {
    // TODO(pg-port): stub
}

/// TODO(pg-port): fmgr.h - OidOutputFunctionCall
unsafe fn OidOutputFunctionCall(_functionId: Oid, _val: Datum) -> *mut c_char {
    crate::utils::fmgr::OidOutputFunctionCall(_functionId as _, _val as _) as _
}

/// TODO(pg-port): mb/pg_wchar.h - pg_mbcliplen
unsafe fn pg_mbcliplen(_mbstr: *const c_char, _len: c_int, _limit: c_int) -> c_int {
    _limit // TODO(pg-port): stub
}

/// TODO(pg-port): catalog/partition.h - RelationGetPartitionQual
unsafe fn RelationGetPartitionQual(_relation: Relation) -> *mut List {
    NIL // TODO(pg-port): stub
}

/// TODO(pg-port): commands/trigger.h - CopyTriggerDesc (real sig)
/// TrigDesc is opaque here; use c_void
unsafe fn getRTEPermissionInfo(
    _rteperminfos: *mut List,
    _rte: *const RangeTblEntry,
) -> *mut RTEPermissionInfo {
    crate::parser::parse_relation::getRTEPermissionInfo(_rteperminfos as _, _rte as _) as _
}

/// TODO(pg-port): executor/execUtils.h - ExecBuildSlotValueDescription signature
/// (the real public fn is below; this is here so early stubs compile)
unsafe fn ExecBuildSlotValueDescription_stub(
    _reloid: Oid,
    _slot: *mut TupleTableSlot,
    _tupdesc: TupleDesc,
    _modifiedCols: *mut Bitmapset,
    _maxfieldlen: c_int,
) -> *mut c_char {
    core::ptr::null_mut() // TODO(pg-port): implemented below
}

/// TODO(pg-port): executor/execGrouping.h - ExecRelGenVirtualNotNull
unsafe fn ExecRelGenVirtualNotNull_stub(
    _resultRelInfo: *mut ResultRelInfo,
    _slot: *mut TupleTableSlot,
    _estate: *mut EState,
    _notnull_virtual_attrs: *mut List,
) -> AttrNumber {
    InvalidAttrNumber // TODO(pg-port): implemented below
}

/// TODO(pg-port): utils/acl.h - ExecCheckOneRelPerms declared in executor.h
/// already declared in executor.rs; re-used here via the executor module.

/// TODO(pg-port): lappend / lappend_int wrappers from pg_list.rs
unsafe fn lappend_int_wrapper(list: *mut List, datum: c_int) -> *mut List {
    // TODO(pg-port): real pg_list lappend_int
    list
}

/// TODO(pg-port): snprintf libc wrapper
unsafe fn snprintf_libc(
    buf: *mut c_char,
    size: usize,
    fmt: *const c_char,
    val: u32,
) {
    extern "C" {
        fn snprintf(buf: *mut c_char, size: usize, fmt: *const c_char, ...) -> c_int;
    }
    snprintf(buf, size, fmt, val);
}

/// TODO(pg-port): executor/execTuples.h
unsafe fn slot_attisnull(_slot: *mut TupleTableSlot, _attnum: c_int) -> bool {
    crate::executor::tuptable::slot_attisnull(_slot as _, _attnum as _) as _
}

/// TODO(pg-port): executor/tuptable.h
unsafe fn slot_getallattrs(_slot: *mut TupleTableSlot) {
    crate::executor::tuptable::slot_getallattrs(_slot as _)
}

// ---------------------------------------------------------------------------
// End of stubs
// ---------------------------------------------------------------------------

/* ----------------------------------------------------------------
 *      ExecutorStart
 *
 *      This routine must be called at the beginning of execution of any
 *      query plan
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecutorStart(queryDesc: *mut QueryDesc, eflags: c_int) {
    /*
     * In some cases (e.g. an EXECUTE statement or an execute message with the
     * extended query protocol) the query_id won't be reported, so do it now.
     *
     * Note that it's harmless to report the query_id multiple times, as the
     * call will be ignored if the top level query_id has already been
     * reported.
     */
    pgstat_report_query_id((*(*queryDesc).plannedstmt).queryId as u64, false);

    if let Some(hook) = ExecutorStart_hook {
        hook(queryDesc, eflags);
    } else {
        standard_ExecutorStart(queryDesc, eflags);
    }
}

pub unsafe fn standard_ExecutorStart(queryDesc: *mut QueryDesc, mut eflags: c_int) {
    let estate: *mut EState;
    let oldcontext: MemoryContext;

    /* sanity checks: queryDesc must not be started already */
    Assert!(queryDesc != core::ptr::null_mut());
    Assert!((*queryDesc).estate == core::ptr::null_mut());

    /* caller must ensure the query's snapshot is active */
    Assert!(GetActiveSnapshot() == (*queryDesc).snapshot);

    /*
     * If the transaction is read-only, we need to check if any writes are
     * planned to non-temporary tables.  EXPLAIN is considered read-only.
     *
     * Don't allow writes in parallel mode.  Supporting UPDATE and DELETE
     * would require (a) storing the combo CID hash in shared memory, rather
     * than synchronizing it just once at the start of parallelism, and (b) an
     * alternative to heap_update()'s reliance on xmax for mutual exclusion.
     * INSERT may have no such troubles, but we forbid it to simplify the
     * checks.
     *
     * We have lower-level defenses in CommandCounterIncrement and elsewhere
     * against performing unsafe operations in parallel mode, but this gives a
     * more user-friendly error message.
     */
    if (XactReadOnly || IsInParallelMode()) && (eflags & EXEC_FLAG_EXPLAIN_ONLY) == 0 {
        ExecCheckXactReadOnly((*queryDesc).plannedstmt);
    }

    /*
     * Build EState, switch into per-query memory context for startup.
     */
    let estate_ptr = CreateExecutorState();
    (*queryDesc).estate = estate_ptr;
    let estate = estate_ptr;

    oldcontext = MemoryContextSwitchTo((*estate).es_query_cxt);

    /*
     * Fill in external parameters, if any, from queryDesc; and allocate
     * workspace for internal parameters
     */
    (*estate).es_param_list_info = (*queryDesc).params;

    if (*(*queryDesc).plannedstmt).paramExecTypes != NIL {
        let nParamExec: c_int;

        nParamExec = list_length((*(*queryDesc).plannedstmt).paramExecTypes);
        (*estate).es_param_exec_vals = palloc0(
            nParamExec as usize * size_of::<crate::nodes::params::ParamExecData>(),
        ) as *mut crate::nodes::params::ParamExecData;
    }

    /* We now require all callers to provide sourceText */
    Assert!((*queryDesc).sourceText != core::ptr::null());
    (*estate).es_sourceText = (*queryDesc).sourceText;

    /*
     * Fill in the query environment, if any, from queryDesc.
     */
    (*estate).es_queryEnv = (*queryDesc).queryEnv;

    /*
     * If non-read-only query, set the command ID to mark output tuples with
     */
    match (*queryDesc).operation {
        CMD_SELECT => {
            /*
             * SELECT FOR [KEY] UPDATE/SHARE and modifying CTEs need to mark
             * tuples
             */
            if (*(*queryDesc).plannedstmt).rowMarks != NIL
                || (*(*queryDesc).plannedstmt).hasModifyingCTE
            {
                (*estate).es_output_cid = GetCurrentCommandId(true);
            }

            /*
             * A SELECT without modifying CTEs can't possibly queue triggers,
             * so force skip-triggers mode. This is just a marginal efficiency
             * hack, since AfterTriggerBeginQuery/AfterTriggerEndQuery aren't
             * all that expensive, but we might as well do it.
             */
            if !(*(*queryDesc).plannedstmt).hasModifyingCTE {
                eflags |= EXEC_FLAG_SKIP_TRIGGERS;
            }
        }
        CMD_INSERT | CMD_DELETE | CMD_UPDATE | CMD_MERGE => {
            (*estate).es_output_cid = GetCurrentCommandId(true);
        }
        _ => {
            elog!(
                ERROR,
                "unrecognized operation code: {}",
                (*queryDesc).operation as c_int
            );
        }
    }

    /*
     * Copy other important information into the EState
     */
    (*estate).es_snapshot = RegisterSnapshot((*queryDesc).snapshot);
    (*estate).es_crosscheck_snapshot =
        RegisterSnapshot((*queryDesc).crosscheck_snapshot);
    (*estate).es_top_eflags = eflags;
    (*estate).es_instrument = (*queryDesc).instrument_options;
    (*estate).es_jit_flags = (*(*queryDesc).plannedstmt).jitFlags;

    /*
     * Set up an AFTER-trigger statement context, unless told not to, or
     * unless it's EXPLAIN-only mode (when ExecutorFinish won't be called).
     */
    if (eflags & (EXEC_FLAG_SKIP_TRIGGERS | EXEC_FLAG_EXPLAIN_ONLY)) == 0 {
        AfterTriggerBeginQuery();
    }

    /*
     * Initialize the plan state tree
     */
    InitPlan(queryDesc, eflags);

    MemoryContextSwitchTo(oldcontext);
}

/* ----------------------------------------------------------------
 *      ExecutorRun
 *
 *      This is the main routine of the executor module. It accepts
 *      the query descriptor from the traffic cop and executes the
 *      query plan.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecutorRun(queryDesc: *mut QueryDesc, direction: ScanDirection, count: uint64) {
    if let Some(hook) = ExecutorRun_hook {
        hook(queryDesc, direction, count);
    } else {
        standard_ExecutorRun(queryDesc, direction, count);
    }
}

pub unsafe fn standard_ExecutorRun(
    queryDesc: *mut QueryDesc,
    direction: ScanDirection,
    count: uint64,
) {
    let estate: *mut EState;
    let operation: CmdType;
    let dest: *mut DestReceiver;
    let sendTuples: bool;
    let oldcontext: MemoryContext;

    /* sanity checks */
    Assert!(queryDesc != core::ptr::null_mut());

    estate = (*queryDesc).estate;

    Assert!(estate != core::ptr::null_mut());
    Assert!(((*estate).es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY) == 0);

    /* caller must ensure the query's snapshot is active */
    Assert!(GetActiveSnapshot() == (*estate).es_snapshot);

    /*
     * Switch into per-query memory context
     */
    oldcontext = MemoryContextSwitchTo((*estate).es_query_cxt);

    /* Allow instrumentation of Executor overall runtime */
    if !(*queryDesc).totaltime.is_null() {
        InstrStartNode((*queryDesc).totaltime);
    }

    /*
     * extract information from the query descriptor and the query feature.
     */
    operation = (*queryDesc).operation;
    dest = (*queryDesc).dest;

    /*
     * startup tuple receiver, if we will be emitting tuples
     */
    (*estate).es_processed = 0;

    sendTuples = operation == CMD_SELECT || (*(*queryDesc).plannedstmt).hasReturning;

    if sendTuples {
        if let Some(rStartup) = (*dest).rStartup {
            rStartup(dest, operation as c_int, (*queryDesc).tupDesc);
        }
    }

    /*
     * Run plan, unless direction is NoMovement.
     *
     * Note: pquery.c selects NoMovement if a prior call already reached
     * end-of-data in the user-specified fetch direction.  This is important
     * because various parts of the executor can misbehave if called again
     * after reporting EOF.  For example, heapam.c would actually restart a
     * heapscan and return all its data afresh.  There is also some doubt
     * about whether a parallel plan would operate properly if an additional,
     * necessarily non-parallel execution request occurs after completing a
     * parallel execution.  (That case should work, but it's untested.)
     */
    if !ScanDirectionIsNoMovement(direction) {
        ExecutePlan(queryDesc, operation, sendTuples, count, direction, dest);
    }

    /*
     * Update es_total_processed to keep track of the number of tuples
     * processed across multiple ExecutorRun() calls.
     */
    (*estate).es_total_processed += (*estate).es_processed;

    /*
     * shutdown tuple receiver, if we started it
     */
    if sendTuples {
        if let Some(rShutdown) = (*dest).rShutdown {
            rShutdown(dest);
        }
    }

    if !(*queryDesc).totaltime.is_null() {
        InstrStopNode((*queryDesc).totaltime, (*estate).es_processed as f64);
    }

    MemoryContextSwitchTo(oldcontext);
}

/* ----------------------------------------------------------------
 *      ExecutorFinish
 *
 *      This routine must be called after the last ExecutorRun call.
 *      It performs cleanup such as firing AFTER triggers.  It is
 *      separate from ExecutorEnd because EXPLAIN ANALYZE needs to
 *      include these actions in the total runtime.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecutorFinish(queryDesc: *mut QueryDesc) {
    if let Some(hook) = ExecutorFinish_hook {
        hook(queryDesc);
    } else {
        standard_ExecutorFinish(queryDesc);
    }
}

pub unsafe fn standard_ExecutorFinish(queryDesc: *mut QueryDesc) {
    let estate: *mut EState;
    let oldcontext: MemoryContext;

    /* sanity checks */
    Assert!(queryDesc != core::ptr::null_mut());

    estate = (*queryDesc).estate;

    Assert!(estate != core::ptr::null_mut());
    Assert!(((*estate).es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY) == 0);

    /* This should be run once and only once per Executor instance */
    Assert!(!(*estate).es_finished);

    /* Switch into per-query memory context */
    oldcontext = MemoryContextSwitchTo((*estate).es_query_cxt);

    /* Allow instrumentation of Executor overall runtime */
    if !(*queryDesc).totaltime.is_null() {
        InstrStartNode((*queryDesc).totaltime);
    }

    /* Run ModifyTable nodes to completion */
    ExecPostprocessPlan(estate);

    /* Execute queued AFTER triggers, unless told not to */
    if ((*estate).es_top_eflags & EXEC_FLAG_SKIP_TRIGGERS) == 0 {
        AfterTriggerEndQuery(estate);
    }

    if !(*queryDesc).totaltime.is_null() {
        InstrStopNode((*queryDesc).totaltime, 0.0);
    }

    MemoryContextSwitchTo(oldcontext);

    (*estate).es_finished = true;
}

/* ----------------------------------------------------------------
 *      ExecutorEnd
 *
 *      This routine must be called at the end of execution of any
 *      query plan
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecutorEnd(queryDesc: *mut QueryDesc) {
    if let Some(hook) = ExecutorEnd_hook {
        hook(queryDesc);
    } else {
        standard_ExecutorEnd(queryDesc);
    }
}

pub unsafe fn standard_ExecutorEnd(queryDesc: *mut QueryDesc) {
    let estate: *mut EState;
    let oldcontext: MemoryContext;

    /* sanity checks */
    Assert!(queryDesc != core::ptr::null_mut());

    estate = (*queryDesc).estate;

    Assert!(estate != core::ptr::null_mut());

    if (*estate).es_parallel_workers_to_launch > 0 {
        pgstat_update_parallel_workers_stats(
            (*estate).es_parallel_workers_to_launch as PgStat_Counter,
            (*estate).es_parallel_workers_launched as PgStat_Counter,
        );
    }

    /*
     * Check that ExecutorFinish was called, unless in EXPLAIN-only mode. This
     * Assert is needed because ExecutorFinish is new as of 9.1, and callers
     * might forget to call it.
     */
    Assert!(
        (*estate).es_finished
            || ((*estate).es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY) != 0,
    );

    /*
     * Switch into per-query memory context to run ExecEndPlan
     */
    oldcontext = MemoryContextSwitchTo((*estate).es_query_cxt);

    ExecEndPlan((*queryDesc).planstate, estate);

    /* do away with our snapshots */
    UnregisterSnapshot((*estate).es_snapshot);
    UnregisterSnapshot((*estate).es_crosscheck_snapshot);

    /*
     * Must switch out of context before destroying it
     */
    MemoryContextSwitchTo(oldcontext);

    /*
     * Release EState and per-query memory context.  This should release
     * everything the executor has allocated.
     */
    FreeExecutorState(estate);

    /* Reset queryDesc fields that no longer point to anything */
    (*queryDesc).tupDesc = core::ptr::null_mut();
    (*queryDesc).estate = core::ptr::null_mut();
    (*queryDesc).planstate = core::ptr::null_mut();
    (*queryDesc).totaltime = core::ptr::null_mut();
}

/* ----------------------------------------------------------------
 *      ExecutorRewind
 *
 *      This routine may be called on an open queryDesc to rewind it
 *      to the start.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecutorRewind(queryDesc: *mut QueryDesc) {
    let estate: *mut EState;
    let oldcontext: MemoryContext;

    /* sanity checks */
    Assert!(queryDesc != core::ptr::null_mut());

    estate = (*queryDesc).estate;

    Assert!(estate != core::ptr::null_mut());

    /* It's probably not sensible to rescan updating queries */
    Assert!((*queryDesc).operation == CMD_SELECT);

    /*
     * Switch into per-query memory context
     */
    oldcontext = MemoryContextSwitchTo((*estate).es_query_cxt);

    /*
     * rescan plan
     */
    ExecReScan((*queryDesc).planstate);

    MemoryContextSwitchTo(oldcontext);
}

/*
 * ExecCheckPermissions
 *      Check access permissions of relations mentioned in a query
 *
 * Returns true if permissions are adequate.  Otherwise, throws an appropriate
 * error if ereport_on_violation is true, or simply returns false otherwise.
 *
 * Note that this does NOT address row-level security policies (aka: RLS).  If
 * rows will be returned to the user as a result of this permission check
 * passing, then RLS also needs to be consulted (and check_enable_rls()).
 *
 * See rewrite/rowsecurity.c.
 *
 * NB: rangeTable is no longer used by us, but kept around for the hooks that
 * might still want to look at the RTEs.
 */
pub unsafe fn ExecCheckPermissions(
    rangeTable: *mut List,
    rteperminfos: *mut List,
    ereport_on_violation: bool,
) -> bool {
    let mut result = true;

    #[cfg(feature = "use_assert_checking")]
    {
        let mut indexset: *mut Bitmapset = core::ptr::null_mut();

        /* Check that rteperminfos is consistent with rangeTable */
        let mut lc_range = list_head(rangeTable);
        while !lc_range.is_null() {
            let rte = (*lc_range).ptr_value as *mut RangeTblEntry;
            if (*rte).perminfoindex != 0 {
                /* Sanity checks */
                Assert!(
                    (*rte).rtekind == crate::nodes::parsenodes::RTEKind::RTE_RELATION
                        || ((*rte).rtekind == crate::nodes::parsenodes::RTEKind::RTE_SUBQUERY
                            && (*rte).relkind == RELKIND_VIEW as c_char),
                );
                getRTEPermissionInfo(rteperminfos, rte);
                /* Many-to-one mapping not allowed */
                Assert!(!bms_is_member((*rte).perminfoindex as c_int, indexset));
                indexset = bms_add_member(indexset, (*rte).perminfoindex as c_int);
            }
            lc_range = lnext(rangeTable, lc_range);
        }

        /* All rteperminfos are referenced */
        Assert!(bms_num_members(indexset) == list_length(rteperminfos));
    }

    let mut l = if rteperminfos.is_null() {
        core::ptr::null_mut()
    } else {
        list_head(rteperminfos)
    };
    while !l.is_null() {
        let perminfo = (*l).ptr_value as *mut RTEPermissionInfo;

        Assert!((*perminfo).relid != InvalidOid);
        result = ExecCheckOneRelPerms(perminfo);
        if !result {
            if ereport_on_violation {
                aclcheck_error(
                    crate::utils::adt::acl::ACLCHECK_NO_PRIV,
                    get_relkind_objtype(get_rel_relkind((*perminfo).relid)),
                    get_rel_name((*perminfo).relid),
                );
            }
            return false;
        }
        l = lnext(rteperminfos, l);
    }

    if let Some(hook) = ExecutorCheckPerms_hook {
        result = hook(rangeTable, rteperminfos, ereport_on_violation);
    }
    result
}

/*
 * ExecCheckOneRelPerms
 *      Check access permissions for a single relation.
 */
pub unsafe fn ExecCheckOneRelPerms(perminfo: *mut RTEPermissionInfo) -> bool {
    let requiredPerms: AclMode;
    let relPerms: AclMode;
    let remainingPerms: AclMode;
    let userid: Oid;
    let relOid: Oid = (*perminfo).relid;

    requiredPerms = (*perminfo).requiredPerms;
    Assert!(requiredPerms != 0);

    /*
     * userid to check as: current user unless we have a setuid indication.
     *
     * Note: GetUserId() is presently fast enough that there's no harm in
     * calling it separately for each relation.  If that stops being true, we
     * could call it once in ExecCheckPermissions and pass the userid down
     * from there.  But for now, no need for the extra clutter.
     */
    userid = if (*perminfo).checkAsUser != InvalidOid {
        (*perminfo).checkAsUser
    } else {
        GetUserId()
    };

    /*
     * We must have *all* the requiredPerms bits, but some of the bits can be
     * satisfied from column-level rather than relation-level permissions.
     * First, remove any bits that are satisfied by relation permissions.
     */
    relPerms = pg_class_aclmask(relOid, userid, requiredPerms, ACLMASK_ALL);
    if std::env::var_os("PDB_AUTH").is_some() { eprintln!("PDB_AUTH ExecCheckOneRelPerms pid={} relOid={} userid={} getuid={} req={} relPerms={} super={}", std::process::id(), relOid, userid, GetUserId(), requiredPerms, relPerms, crate::utils::misc::superuser::superuser_arg(userid)); }
    remainingPerms = requiredPerms & !relPerms;
    if remainingPerms != 0 {
        let mut col: c_int = -1;

        /*
         * If we lack any permissions that exist only as relation permissions,
         * we can fail straight away.
         */
        if remainingPerms & !(ACL_SELECT | ACL_INSERT | ACL_UPDATE) != 0 {
            return false;
        }

        /*
         * Check to see if we have the needed privileges at column level.
         *
         * Note: failures just report a table-level error; it would be nicer
         * to report a column-level error if we have some but not all of the
         * column privileges.
         */
        if remainingPerms & ACL_SELECT != 0 {
            /*
             * When the query doesn't explicitly reference any columns (for
             * example, SELECT COUNT(*) FROM table), allow the query if we
             * have SELECT on any column of the rel, as per SQL spec.
             */
            if bms_is_empty((*perminfo).selectedCols) {
                if pg_attribute_aclcheck_all(relOid, userid, ACL_SELECT, ACLMASK_ANY) != ACLCHECK_OK {
                    return false;
                }
            }

            loop {
                col = bms_next_member((*perminfo).selectedCols, col);
                if col < 0 {
                    break;
                }
                /* bit #s are offset by FirstLowInvalidHeapAttributeNumber */
                let attno: AttrNumber =
                    (col + FirstLowInvalidHeapAttributeNumber as c_int) as AttrNumber;

                if attno == InvalidAttrNumber {
                    /* Whole-row reference, must have priv on all cols */
                    if pg_attribute_aclcheck_all(relOid, userid, ACL_SELECT, ACLMASK_ALL)
                        != ACLCHECK_OK
                    {
                        return false;
                    }
                } else {
                    if pg_attribute_aclcheck(relOid, attno, userid, ACL_SELECT) != ACLCHECK_OK {
                        return false;
                    }
                }
            }
        }

        /*
         * Basically the same for the mod columns, for both INSERT and UPDATE
         * privilege as specified by remainingPerms.
         */
        if remainingPerms & ACL_INSERT != 0
            && !ExecCheckPermissionsModified(
                relOid,
                userid,
                (*perminfo).insertedCols,
                ACL_INSERT,
            )
        {
            return false;
        }

        if remainingPerms & ACL_UPDATE != 0
            && !ExecCheckPermissionsModified(
                relOid,
                userid,
                (*perminfo).updatedCols,
                ACL_UPDATE,
            )
        {
            return false;
        }
    }
    true
}

/*
 * ExecCheckPermissionsModified
 *      Check INSERT or UPDATE access permissions for a single relation (these
 *      are processed uniformly).
 */
unsafe fn ExecCheckPermissionsModified(
    relOid: Oid,
    userid: Oid,
    modifiedCols: *mut Bitmapset,
    requiredPerms: AclMode,
) -> bool {
    let mut col: c_int = -1;

    /*
     * When the query doesn't explicitly update any columns, allow the query
     * if we have permission on any column of the rel.  This is to handle
     * SELECT FOR UPDATE as well as possible corner cases in UPDATE.
     */
    if bms_is_empty(modifiedCols) {
        if pg_attribute_aclcheck_all(relOid, userid, requiredPerms, ACLMASK_ANY) != ACLCHECK_OK {
            return false;
        }
    }

    loop {
        col = bms_next_member(modifiedCols, col);
        if col < 0 {
            break;
        }
        /* bit #s are offset by FirstLowInvalidHeapAttributeNumber */
        let attno: AttrNumber =
            (col + FirstLowInvalidHeapAttributeNumber as c_int) as AttrNumber;

        if attno == InvalidAttrNumber {
            /* whole-row reference can't happen here */
            elog!(ERROR, "whole-row update is not implemented");
        } else {
            if pg_attribute_aclcheck(relOid, attno, userid, requiredPerms) != ACLCHECK_OK {
                return false;
            }
        }
    }
    true
}

/*
 * Check that the query does not imply any writes to non-temp tables;
 * unless we're in parallel mode, in which case don't even allow writes
 * to temp tables.
 *
 * Note: in a Hot Standby this would need to reject writes to temp
 * tables just as we do in parallel mode; but an HS standby can't have created
 * any temp tables in the first place, so no need to check that.
 */
unsafe fn ExecCheckXactReadOnly(plannedstmt: *mut PlannedStmt) {
    /*
     * Fail if write permissions are requested in parallel mode for table
     * (temp or non-temp), otherwise fail for any non-temp table.
     */
    let mut l = if !(*plannedstmt).permInfos.is_null() {
        list_head((*plannedstmt).permInfos)
    } else {
        core::ptr::null_mut()
    };
    while !l.is_null() {
        let perminfo = (*l).ptr_value as *mut RTEPermissionInfo;

        if ((*perminfo).requiredPerms & (!ACL_SELECT)) == 0 {
            l = lnext((*plannedstmt).permInfos, l);
            continue;
        }

        if isTempNamespace(get_rel_namespace((*perminfo).relid)) {
            l = lnext((*plannedstmt).permInfos, l);
            continue;
        }

        PreventCommandIfReadOnly(CreateCommandName(plannedstmt as *mut Node));
        l = lnext((*plannedstmt).permInfos, l);
    }

    if (*plannedstmt).commandType != CMD_SELECT || (*plannedstmt).hasModifyingCTE {
        PreventCommandIfParallelMode(CreateCommandName(plannedstmt as *mut Node));
    }
}


/* ----------------------------------------------------------------
 *      InitPlan
 *
 *      Initializes the query plan: open files, allocate storage
 *      and start up the rule manager
 * ----------------------------------------------------------------
 */
unsafe fn InitPlan(queryDesc: *mut QueryDesc, eflags: c_int) {
    let operation: CmdType = (*queryDesc).operation;
    let plannedstmt: *mut PlannedStmt = (*queryDesc).plannedstmt;
    let plan: *mut Plan = (*plannedstmt).planTree;
    let rangeTable: *mut List = (*plannedstmt).rtable;
    let estate: *mut EState = (*queryDesc).estate;
    let planstate: *mut PlanState;
    let tupType: TupleDesc;
    let mut i: c_int;

    /*
     * Do permissions checks
     */
    ExecCheckPermissions(rangeTable, (*plannedstmt).permInfos, true);

    /*
     * initialize the node's execution state
     */
    ExecInitRangeTable(
        estate,
        rangeTable,
        (*plannedstmt).permInfos,
        bms_copy((*plannedstmt).unprunableRelids),
    );

    (*estate).es_plannedstmt = plannedstmt;
    (*estate).es_part_prune_infos = (*plannedstmt).partPruneInfos;

    /*
     * Perform runtime "initial" pruning to identify which child subplans,
     * corresponding to the children of plan nodes that contain
     * PartitionPruneInfo such as Append, will not be executed. The results,
     * which are bitmapsets of indexes of the child subplans that will be
     * executed, are saved in es_part_prune_results.  These results correspond
     * to each PartitionPruneInfo entry, and the es_part_prune_results list is
     * parallel to es_part_prune_infos.
     */
    ExecDoInitialPruning(estate);

    /*
     * Next, build the ExecRowMark array from the PlanRowMark(s), if any.
     */
    if !(*plannedstmt).rowMarks.is_null() {
        (*estate).es_rowmarks = palloc0(
            (*estate).es_range_table_size as usize * size_of::<*mut ExecRowMark>(),
        ) as *mut *mut ExecRowMark;
        let mut l = list_head((*plannedstmt).rowMarks);
        while !l.is_null() {
            let rc = (*l).ptr_value as *mut PlanRowMark;
            let rte: *mut RangeTblEntry = exec_rt_fetch((*rc).rti, estate);
            let relid: Oid;
            let relation: Relation;
            let erm: *mut ExecRowMark;

            /* ignore "parent" rowmarks; they are irrelevant at runtime */
            if (*rc).isParent {
                l = lnext((*plannedstmt).rowMarks, l);
                continue;
            }

            /*
             * Also ignore rowmarks belonging to child tables that have been
             * pruned in ExecDoInitialPruning().
             */
            if (*rte).rtekind == crate::nodes::parsenodes::RTEKind::RTE_RELATION
                && !bms_is_member((*rc).rti as c_int, (*estate).es_unpruned_relids)
            {
                l = lnext((*plannedstmt).rowMarks, l);
                continue;
            }

            /* get relation's OID (will produce InvalidOid if subquery) */
            relid = (*rte).relid;

            /* open relation, if we need to access it for this mark type */
            relation = match (*rc).markType {
                ROW_MARK_EXCLUSIVE
                | ROW_MARK_NOKEYEXCLUSIVE
                | ROW_MARK_SHARE
                | ROW_MARK_KEYSHARE
                | ROW_MARK_REFERENCE => ExecGetRangeTableRelation(estate, (*rc).rti, false),
                ROW_MARK_COPY => {
                    /* no physical table access is required */
                    core::ptr::null_mut()
                }
                _ => {
                    elog!(ERROR, "unrecognized markType: {}", (*rc).markType as c_int);
                    core::ptr::null_mut() /* keep compiler quiet */
                }
            };

            /* Check that relation is a legal target for marking */
            if !relation.is_null() {
                CheckValidRowMarkRel(relation, (*rc).markType);
            }

            erm = palloc(size_of::<ExecRowMark>()) as *mut ExecRowMark;
            (*erm).relation = relation;
            (*erm).relid = relid;
            (*erm).rti = (*rc).rti;
            (*erm).prti = (*rc).prti;
            (*erm).rowmarkId = (*rc).rowmarkId;
            (*erm).markType = (*rc).markType;
            (*erm).strength = (*rc).strength;
            (*erm).waitPolicy = (*rc).waitPolicy;
            (*erm).ermActive = false;
            crate::storage::itemptr::ItemPointerSetInvalid(&mut (*erm).curCtid);
            (*erm).ermExtra = core::ptr::null_mut();

            Assert!(
                (*erm).rti > 0
                    && (*erm).rti <= (*estate).es_range_table_size
                    && (*(*estate).es_rowmarks.add(((*erm).rti - 1) as usize)).is_null(),
            );

            *(*estate).es_rowmarks.add(((*erm).rti - 1) as usize) = erm;
            l = lnext((*plannedstmt).rowMarks, l);
        }
    }

    /*
     * Initialize the executor's tuple table to empty.
     */
    (*estate).es_tupleTable = NIL;

    /* signal that this EState is not used for EPQ */
    (*estate).es_epq_active = core::ptr::null_mut();

    /*
     * Initialize private state information for each SubPlan.  We must do this
     * before running ExecInitNode on the main query tree, since
     * ExecInitSubPlan expects to be able to find these entries.
     */
    Assert!((*estate).es_subplanstates == NIL);
    i = 1; /* subplan indices count from 1 */
    let mut l = if !(*plannedstmt).subplans.is_null() {
        list_head((*plannedstmt).subplans)
    } else {
        core::ptr::null_mut()
    };
    while !l.is_null() {
        let subplan = (*l).ptr_value as *mut Plan;
        let subplanstate: *mut PlanState;
        let sp_eflags: c_int;

        /*
         * A subplan will never need to do BACKWARD scan nor MARK/RESTORE. If
         * it is a parameterless subplan (not initplan), we suggest that it be
         * prepared to handle REWIND efficiently; otherwise there is no need.
         */
        let mut sp_eflags = eflags & !(EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK);
        if bms_is_member(i, (*plannedstmt).rewindPlanIDs) {
            sp_eflags |= EXEC_FLAG_REWIND;
        }

        let subplanstate_val = ExecInitNode(subplan, estate, sp_eflags);

        (*estate).es_subplanstates =
            crate::nodes::pg_list::lappend((*estate).es_subplanstates, subplanstate_val as *mut core::ffi::c_void);

        i += 1;
        l = lnext((*plannedstmt).subplans, l);
    }

    /*
     * Initialize the private state information for all the nodes in the query
     * tree.  This opens files, allocates storage and leaves us ready to start
     * processing tuples.
     */
    let planstate_val = ExecInitNode(plan, estate, eflags);
    (*queryDesc).planstate = planstate_val;

    /*
     * Get the tuple descriptor describing the type of tuples to return.
     */
    let mut tupType = ExecGetResultType(planstate_val);

    /*
     * Initialize the junk filter if needed.  SELECT queries need a filter if
     * there are any junk attrs in the top-level tlist.
     */
    if operation == CMD_SELECT {
        let mut junk_filter_needed = false;
        let mut tlist = if !(*plan).targetlist.is_null() {
            list_head((*plan).targetlist)
        } else {
            core::ptr::null_mut()
        };
        while !tlist.is_null() {
            let tle = (*tlist).ptr_value as *mut crate::nodes::primnodes::TargetEntry;

            if (*tle).resjunk {
                junk_filter_needed = true;
                break;
            }
            tlist = lnext((*plan).targetlist, tlist);
        }

        if junk_filter_needed {
            let slot = ExecInitExtraTupleSlot(
                estate,
                core::ptr::null_mut(),
                &TTSOpsVirtual as *const _,
            );
            let j = ExecInitJunkFilter((*(*planstate_val).plan).targetlist, slot);
            (*estate).es_junkFilter = j;

            /* Want to return the cleaned tuple type */
            tupType = (*j).jf_cleanTupType;
        }
    }

    (*queryDesc).tupDesc = tupType;
    /* planstate already set above */
}

/*
 * Check that a proposed result relation is a legal target for the operation
 *
 * Generally the parser and/or planner should have noticed any such mistake
 * already, but let's make sure.
 */
pub unsafe fn CheckValidResultRel(
    resultRelInfo: *mut ResultRelInfo,
    operation: CmdType,
    onConflictAction: OnConflictAction,
    mergeActions: *mut List,
) {
    let resultRel: Relation = (*resultRelInfo).ri_RelationDesc;
    let fdwroutine: *mut FdwRoutine;

    /* Expect a fully-formed ResultRelInfo from InitResultRelInfo(). */
    Assert!((*resultRelInfo).ri_needLockTagTuple == IsInplaceUpdateRelation(resultRel));

    match (*(*resultRel).rd_rel).relkind as c_char {
        x if x == RELKIND_RELATION || x == RELKIND_PARTITIONED_TABLE => {
            /*
             * For MERGE, check that the target relation supports each action.
             * For other operations, just check the operation itself.
             */
            if operation == CMD_MERGE {
                let mut l = if !mergeActions.is_null() {
                    list_head(mergeActions)
                } else {
                    core::ptr::null_mut()
                };
                while !l.is_null() {
                    let action = (*l).ptr_value as *mut MergeAction;
                    CheckCmdReplicaIdentity(resultRel, (*action).commandType);
                    l = lnext(mergeActions, l);
                }
            } else {
                CheckCmdReplicaIdentity(resultRel, operation);
            }

            /*
             * For INSERT ON CONFLICT DO UPDATE, additionally check that the
             * target relation supports UPDATE.
             */
            if onConflictAction == ONCONFLICT_UPDATE {
                CheckCmdReplicaIdentity(resultRel, CMD_UPDATE);
            }
        }
        x if x == RELKIND_SEQUENCE => {
            ereport!(ERROR, errmsg!(
                    "cannot change sequence \"{}\"",
                    core::ffi::CStr::from_ptr(RelationGetRelationName(resultRel)).to_string_lossy()
                ) /* C also: errcode!(ERRCODE_WRONG_OBJECT_TYPE) */);
        }
        x if x == RELKIND_TOASTVALUE => {
            ereport!(ERROR, errmsg!(
                    "cannot change TOAST relation \"{}\"",
                    core::ffi::CStr::from_ptr(RelationGetRelationName(resultRel)).to_string_lossy()
                ) /* C also: errcode!(ERRCODE_WRONG_OBJECT_TYPE) */);
        }
        x if x == RELKIND_VIEW => {
            /*
             * Okay only if there's a suitable INSTEAD OF trigger.  Otherwise,
             * complain, but omit errdetail because we haven't got the
             * information handy (and given that it really shouldn't happen,
             * it's not worth great exertion to get).
             */
            if !view_has_instead_trigger(resultRel, operation, mergeActions) {
                error_view_not_updatable(resultRel, operation, mergeActions, core::ptr::null());
            }
        }
        x if x == RELKIND_MATVIEW => {
            if !MatViewIncrementalMaintenanceIsEnabled() {
                ereport!(ERROR, errmsg!(
                        "cannot change materialized view \"{}\"",
                        core::ffi::CStr::from_ptr(RelationGetRelationName(resultRel))
                            .to_string_lossy()
                    ) /* C also: errcode!(ERRCODE_WRONG_OBJECT_TYPE) */);
            }
        }
        x if x == RELKIND_FOREIGN_TABLE => {
            /* Okay only if the FDW supports it */
            let fdwroutine = (*resultRelInfo).ri_FdwRoutine as *mut FdwRoutineOpaque;
            match operation {
                CMD_INSERT => {
                    if (*fdwroutine).ExecForeignInsert.is_none() {
                        ereport!(ERROR, errmsg!(
                                "cannot insert into foreign table \"{}\"",
                                core::ffi::CStr::from_ptr(RelationGetRelationName(resultRel))
                                    .to_string_lossy()
                            ) /* C also: errcode!(ERRCODE_FEATURE_NOT_SUPPORTED) */);
                    }
                    if (*fdwroutine).IsForeignRelUpdatable.is_some()
                        && ((*fdwroutine).IsForeignRelUpdatable.unwrap()(resultRel)
                            & (1 << CMD_INSERT as c_int))
                            == 0
                    {
                        ereport!(ERROR, errmsg!(
                                "foreign table \"{}\" does not allow inserts",
                                core::ffi::CStr::from_ptr(RelationGetRelationName(resultRel))
                                    .to_string_lossy()
                            ) /* C also: errcode!(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */);
                    }
                }
                CMD_UPDATE => {
                    if (*fdwroutine).ExecForeignUpdate.is_none() {
                        ereport!(ERROR, errmsg!(
                                "cannot update foreign table \"{}\"",
                                core::ffi::CStr::from_ptr(RelationGetRelationName(resultRel))
                                    .to_string_lossy()
                            ) /* C also: errcode!(ERRCODE_FEATURE_NOT_SUPPORTED) */);
                    }
                    if (*fdwroutine).IsForeignRelUpdatable.is_some()
                        && ((*fdwroutine).IsForeignRelUpdatable.unwrap()(resultRel)
                            & (1 << CMD_UPDATE as c_int))
                            == 0
                    {
                        ereport!(ERROR, errmsg!(
                                "foreign table \"{}\" does not allow updates",
                                core::ffi::CStr::from_ptr(RelationGetRelationName(resultRel))
                                    .to_string_lossy()
                            ) /* C also: errcode!(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */);
                    }
                }
                CMD_DELETE => {
                    if (*fdwroutine).ExecForeignDelete.is_none() {
                        ereport!(ERROR, errmsg!(
                                "cannot delete from foreign table \"{}\"",
                                core::ffi::CStr::from_ptr(RelationGetRelationName(resultRel))
                                    .to_string_lossy()
                            ) /* C also: errcode!(ERRCODE_FEATURE_NOT_SUPPORTED) */);
                    }
                    if (*fdwroutine).IsForeignRelUpdatable.is_some()
                        && ((*fdwroutine).IsForeignRelUpdatable.unwrap()(resultRel)
                            & (1 << CMD_DELETE as c_int))
                            == 0
                    {
                        ereport!(ERROR, errmsg!(
                                "foreign table \"{}\" does not allow deletes",
                                core::ffi::CStr::from_ptr(RelationGetRelationName(resultRel))
                                    .to_string_lossy()
                            ) /* C also: errcode!(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */);
                    }
                }
                _ => {
                    elog!(ERROR, "unrecognized CmdType: {}", operation as c_int);
                }
            }
        }
        _ => {
            ereport!(ERROR, errmsg!(
                    "cannot change relation \"{}\"",
                    core::ffi::CStr::from_ptr(RelationGetRelationName(resultRel)).to_string_lossy()
                ) /* C also: errcode!(ERRCODE_WRONG_OBJECT_TYPE) */);
        }
    }
}

/// Opaque FDW routine struct with just the function pointers we need.
/// TODO(pg-port): real FdwRoutine is in foreign/fdwapi.h
#[repr(C)]
struct FdwRoutineOpaque {
    pub ExecForeignInsert: Option<unsafe fn() -> ()>,
    pub ExecForeignUpdate: Option<unsafe fn() -> ()>,
    pub ExecForeignDelete: Option<unsafe fn() -> ()>,
    pub IsForeignRelUpdatable: Option<unsafe fn(rel: Relation) -> c_int>,
    pub RefetchForeignRow: Option<
        unsafe fn(
            estate: *mut EState,
            erm: *mut ExecRowMark,
            rowid: Datum,
            slot: *mut TupleTableSlot,
            updated: *mut bool,
        ) -> (),
    >,
}

/*
 * Check that a proposed rowmark target relation is a legal target
 *
 * In most cases parser and/or planner should have noticed this already, but
 * they don't cover all cases.
 */
unsafe fn CheckValidRowMarkRel(rel: Relation, markType: RowMarkType) {
    let fdwroutine: *mut FdwRoutineOpaque;

    match (*(*rel).rd_rel).relkind as c_char {
        x if x == RELKIND_RELATION || x == RELKIND_PARTITIONED_TABLE => {
            /* OK */
        }
        x if x == RELKIND_SEQUENCE => {
            /* Must disallow this because we don't vacuum sequences */
            ereport!(ERROR, errmsg!(
                    "cannot lock rows in sequence \"{}\"",
                    core::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                ) /* C also: errcode!(ERRCODE_WRONG_OBJECT_TYPE) */);
        }
        x if x == RELKIND_TOASTVALUE => {
            /* We could allow this, but there seems no good reason to */
            ereport!(ERROR, errmsg!(
                    "cannot lock rows in TOAST relation \"{}\"",
                    core::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                ) /* C also: errcode!(ERRCODE_WRONG_OBJECT_TYPE) */);
        }
        x if x == RELKIND_VIEW => {
            /* Should not get here; planner should have expanded the view */
            ereport!(ERROR, errmsg!(
                    "cannot lock rows in view \"{}\"",
                    core::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                ) /* C also: errcode!(ERRCODE_WRONG_OBJECT_TYPE) */);
        }
        x if x == RELKIND_MATVIEW => {
            /* Allow referencing a matview, but not actual locking clauses */
            if markType != ROW_MARK_REFERENCE {
                ereport!(ERROR, errmsg!(
                        "cannot lock rows in materialized view \"{}\"",
                        core::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                    ) /* C also: errcode!(ERRCODE_WRONG_OBJECT_TYPE) */);
            }
        }
        x if x == RELKIND_FOREIGN_TABLE => {
            /* Okay only if the FDW supports it */
            let fdwroutine =
                GetFdwRoutineForRelation(rel, false) as *mut FdwRoutineOpaque;
            if (*fdwroutine).RefetchForeignRow.is_none() {
                ereport!(ERROR, errmsg!(
                        "cannot lock rows in foreign table \"{}\"",
                        core::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                    ) /* C also: errcode!(ERRCODE_FEATURE_NOT_SUPPORTED) */);
            }
        }
        _ => {
            ereport!(ERROR, errmsg!(
                    "cannot lock rows in relation \"{}\"",
                    core::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                ) /* C also: errcode!(ERRCODE_WRONG_OBJECT_TYPE) */);
        }
    }
}

/*
 * Initialize ResultRelInfo data for one result relation
 *
 * Caution: before Postgres 9.1, this function included the relkind checking
 * that's now in CheckValidResultRel, and it also did ExecOpenIndices if
 * appropriate.  Be sure callers cover those needs.
 */
pub unsafe fn InitResultRelInfo(
    resultRelInfo: *mut ResultRelInfo,
    resultRelationDesc: Relation,
    resultRelationIndex: Index,
    partition_root_rri: *mut ResultRelInfo,
    instrument_options: c_int,
) {
    core::ptr::write_bytes(resultRelInfo as *mut u8, 0, size_of::<ResultRelInfo>());
    (*resultRelInfo).r#type = NodeTag::T_ResultRelInfo;
    (*resultRelInfo).ri_RangeTableIndex = resultRelationIndex;
    (*resultRelInfo).ri_RelationDesc = resultRelationDesc;
    (*resultRelInfo).ri_NumIndices = 0;
    (*resultRelInfo).ri_IndexRelationDescs = core::ptr::null_mut();
    (*resultRelInfo).ri_IndexRelationInfo = core::ptr::null_mut();
    (*resultRelInfo).ri_needLockTagTuple = IsInplaceUpdateRelation(resultRelationDesc);
    /* make a copy so as not to depend on relcache info not changing... */
    (*resultRelInfo).ri_TrigDesc =
        CopyTriggerDesc((*resultRelationDesc).trigdesc) as *mut crate::nodes::execnodes::TriggerDesc;
    if !(*resultRelInfo).ri_TrigDesc.is_null() {
        let n: c_int = (*( (*resultRelInfo).ri_TrigDesc
            as *mut crate::utils::reltrigger::TriggerDesc))
            .numtriggers;

        (*resultRelInfo).ri_TrigFunctions =
            palloc0(n as usize * size_of::<crate::utils::fmgr::FmgrInfo>())
                as *mut crate::utils::fmgr::FmgrInfo;
        (*resultRelInfo).ri_TrigWhenExprs =
            palloc0(n as usize * size_of::<*mut ExprState>()) as *mut *mut ExprState;
        if instrument_options != 0 {
            (*resultRelInfo).ri_TrigInstrument =
                InstrAlloc(n, instrument_options, false) as *mut crate::executor::instrument::Instrumentation;
        }
    } else {
        (*resultRelInfo).ri_TrigFunctions = core::ptr::null_mut();
        (*resultRelInfo).ri_TrigWhenExprs = core::ptr::null_mut();
        (*resultRelInfo).ri_TrigInstrument = core::ptr::null_mut();
    }
    if (*(*resultRelationDesc).rd_rel).relkind as c_char == RELKIND_FOREIGN_TABLE {
        (*resultRelInfo).ri_FdwRoutine =
            GetFdwRoutineForRelation(resultRelationDesc, true) as *mut crate::nodes::execnodes::FdwRoutine;
    } else {
        (*resultRelInfo).ri_FdwRoutine = core::ptr::null_mut();
    }

    /* The following fields are set later if needed */
    (*resultRelInfo).ri_RowIdAttNo = 0;
    (*resultRelInfo).ri_extraUpdatedCols = core::ptr::null_mut();
    (*resultRelInfo).ri_projectNew = core::ptr::null_mut();
    (*resultRelInfo).ri_newTupleSlot = core::ptr::null_mut();
    (*resultRelInfo).ri_oldTupleSlot = core::ptr::null_mut();
    (*resultRelInfo).ri_projectNewInfoValid = false;
    (*resultRelInfo).ri_FdwState = core::ptr::null_mut();
    (*resultRelInfo).ri_usesFdwDirectModify = false;
    (*resultRelInfo).ri_CheckConstraintExprs = core::ptr::null_mut();
    (*resultRelInfo).ri_GenVirtualNotNullConstraintExprs = core::ptr::null_mut();
    (*resultRelInfo).ri_GeneratedExprsI = core::ptr::null_mut();
    (*resultRelInfo).ri_GeneratedExprsU = core::ptr::null_mut();
    (*resultRelInfo).ri_projectReturning = core::ptr::null_mut();
    (*resultRelInfo).ri_onConflictArbiterIndexes = NIL;
    (*resultRelInfo).ri_onConflict = core::ptr::null_mut();
    (*resultRelInfo).ri_ReturningSlot = core::ptr::null_mut();
    (*resultRelInfo).ri_TrigOldSlot = core::ptr::null_mut();
    (*resultRelInfo).ri_TrigNewSlot = core::ptr::null_mut();
    (*resultRelInfo).ri_AllNullSlot = core::ptr::null_mut();
    (*resultRelInfo).ri_MergeActions[MERGE_WHEN_MATCHED as usize] = NIL;
    (*resultRelInfo).ri_MergeActions[MERGE_WHEN_NOT_MATCHED_BY_SOURCE as usize] = NIL;
    (*resultRelInfo).ri_MergeActions[MERGE_WHEN_NOT_MATCHED_BY_TARGET as usize] = NIL;
    (*resultRelInfo).ri_MergeJoinCondition = core::ptr::null_mut();

    /*
     * Only ExecInitPartitionInfo() and ExecInitPartitionDispatchInfo() pass
     * non-NULL partition_root_rri.  For child relations that are part of the
     * initial query rather than being dynamically added by tuple routing,
     * this field is filled in ExecInitModifyTable().
     */
    (*resultRelInfo).ri_RootResultRelInfo = partition_root_rri;
    /* Set by ExecGetRootToChildMap */
    (*resultRelInfo).ri_RootToChildMap = core::ptr::null_mut();
    (*resultRelInfo).ri_RootToChildMapValid = false;
    /* Set by ExecInitRoutingInfo */
    (*resultRelInfo).ri_PartitionTupleSlot = core::ptr::null_mut();
    (*resultRelInfo).ri_ChildToRootMap = core::ptr::null_mut();
    (*resultRelInfo).ri_ChildToRootMapValid = false;
    (*resultRelInfo).ri_CopyMultiInsertBuffer = core::ptr::null_mut();
}

/*
 * ExecGetTriggerResultRel
 *      Get a ResultRelInfo for a trigger target relation.
 */
pub unsafe fn ExecGetTriggerResultRel(
    estate: *mut EState,
    relid: Oid,
    rootRelInfo: *mut ResultRelInfo,
) -> *mut ResultRelInfo {
    let mut rInfo: *mut ResultRelInfo;
    let rel: Relation;
    let oldcontext: MemoryContext;

    /*
     * Before creating a new ResultRelInfo, check if we've already made and
     * cached one for this relation.
     */

    /* Search through the query result relations */
    let mut l = if !(*estate).es_opened_result_relations.is_null() {
        list_head((*estate).es_opened_result_relations)
    } else {
        core::ptr::null_mut()
    };
    while !l.is_null() {
        rInfo = (*l).ptr_value as *mut ResultRelInfo;
        if RelationGetRelid((*rInfo).ri_RelationDesc) == relid
            && (*rInfo).ri_RootResultRelInfo == rootRelInfo
        {
            return rInfo;
        }
        l = lnext((*estate).es_opened_result_relations, l);
    }

    /*
     * Search through the result relations that were created during tuple
     * routing, if any.
     */
    let mut l = if !(*estate).es_tuple_routing_result_relations.is_null() {
        list_head((*estate).es_tuple_routing_result_relations)
    } else {
        core::ptr::null_mut()
    };
    while !l.is_null() {
        rInfo = (*l).ptr_value as *mut ResultRelInfo;
        if RelationGetRelid((*rInfo).ri_RelationDesc) == relid
            && (*rInfo).ri_RootResultRelInfo == rootRelInfo
        {
            return rInfo;
        }
        l = lnext((*estate).es_tuple_routing_result_relations, l);
    }

    /* Nope, but maybe we already made an extra ResultRelInfo for it */
    let mut l = if !(*estate).es_trig_target_relations.is_null() {
        list_head((*estate).es_trig_target_relations)
    } else {
        core::ptr::null_mut()
    };
    while !l.is_null() {
        rInfo = (*l).ptr_value as *mut ResultRelInfo;
        if RelationGetRelid((*rInfo).ri_RelationDesc) == relid
            && (*rInfo).ri_RootResultRelInfo == rootRelInfo
        {
            return rInfo;
        }
        l = lnext((*estate).es_trig_target_relations, l);
    }
    /* Nope, so we need a new one */

    /*
     * Open the target relation's relcache entry.  We assume that an
     * appropriate lock is still held by the backend from whenever the trigger
     * event got queued, so we need take no new lock here.
     */
    rel = table_open(relid, NoLock);

    /*
     * Make the new entry in the right context.
     */
    oldcontext = MemoryContextSwitchTo((*estate).es_query_cxt);
    rInfo = makeNode!(ResultRelInfo, T_ResultRelInfo);
    InitResultRelInfo(
        rInfo,
        rel,
        0, /* dummy rangetable index */
        rootRelInfo,
        (*estate).es_instrument,
    );
    (*estate).es_trig_target_relations =
        crate::nodes::pg_list::lappend((*estate).es_trig_target_relations, rInfo as *mut core::ffi::c_void);
    MemoryContextSwitchTo(oldcontext);

    /*
     * Currently, we don't need any index information in ResultRelInfos used
     * only for triggers, so no need to call ExecOpenIndices.
     */

    rInfo
}

/*
 * Return the ancestor relations of a given leaf partition result relation
 * up to and including the query's root target relation.
 */
pub unsafe fn ExecGetAncestorResultRels(
    estate: *mut EState,
    resultRelInfo: *mut ResultRelInfo,
) -> *mut List {
    let rootRelInfo: *mut ResultRelInfo = (*resultRelInfo).ri_RootResultRelInfo;
    let partRel: Relation = (*resultRelInfo).ri_RelationDesc;
    let rootRelOid: Oid;

    if !(*(*partRel).rd_rel).relispartition {
        elog!(ERROR, "cannot find ancestors of a non-partition result relation");
    }
    Assert!(!rootRelInfo.is_null());
    rootRelOid = RelationGetRelid((*rootRelInfo).ri_RelationDesc);
    if (*resultRelInfo).ri_ancestorResultRels == NIL {
        let oids: *mut List = get_partition_ancestors(RelationGetRelid(partRel));
        let mut ancResultRels: *mut List = NIL;

        let mut lc = if !oids.is_null() { list_head(oids) } else { core::ptr::null_mut() };
        while !lc.is_null() {
            let ancOid: Oid = (*lc).oid_value;
            let ancRel: Relation;
            let rInfo: *mut ResultRelInfo;

            /*
             * Ignore the root ancestor here, and use ri_RootResultRelInfo
             * (below) for it instead.  Also, we stop climbing up the
             * hierarchy when we find the table that was mentioned in the
             * query.
             */
            if ancOid == rootRelOid {
                break;
            }

            /*
             * All ancestors up to the root target relation must have been
             * locked by the planner or AcquireExecutorLocks().
             */
            ancRel = table_open(ancOid, NoLock);
            rInfo = makeNode!(ResultRelInfo, T_ResultRelInfo);

            /* dummy rangetable index */
            InitResultRelInfo(rInfo, ancRel, 0, core::ptr::null_mut(), (*estate).es_instrument);
            ancResultRels = crate::nodes::pg_list::lappend(ancResultRels, rInfo as *mut core::ffi::c_void);
            lc = lnext(oids, lc);
        }
        ancResultRels = crate::nodes::pg_list::lappend(ancResultRels, rootRelInfo as *mut core::ffi::c_void);
        (*resultRelInfo).ri_ancestorResultRels = ancResultRels;
    }

    /* We must have found some ancestor */
    Assert!((*resultRelInfo).ri_ancestorResultRels != NIL);

    (*resultRelInfo).ri_ancestorResultRels
}

/* ----------------------------------------------------------------
 *      ExecPostprocessPlan
 *
 *      Give plan nodes a final chance to execute before shutdown
 * ----------------------------------------------------------------
 */
unsafe fn ExecPostprocessPlan(estate: *mut EState) {
    /*
     * Make sure nodes run forward.
     */
    (*estate).es_direction = crate::access::sdir::ForwardScanDirection;

    /*
     * Run any secondary ModifyTable nodes to completion, in case the main
     * query did not fetch all rows from them.  (We do this to ensure that
     * such nodes have predictable results.)
     */
    let mut lc = if !(*estate).es_auxmodifytables.is_null() {
        list_head((*estate).es_auxmodifytables)
    } else {
        core::ptr::null_mut()
    };
    while !lc.is_null() {
        let ps = (*lc).ptr_value as *mut PlanState;

        loop {
            /* Reset the per-output-tuple exprcontext each time */
            ResetPerTupleExprContext(estate);

            let slot = ExecProcNode(ps);

            if TupIsNull(slot) {
                break;
            }
        }
        lc = lnext((*estate).es_auxmodifytables, lc);
    }
}

/* ----------------------------------------------------------------
 *      ExecEndPlan
 *
 *      Cleans up the query plan -- closes files and frees up storage
 * ----------------------------------------------------------------
 */
unsafe fn ExecEndPlan(planstate: *mut PlanState, estate: *mut EState) {
    /*
     * shut down the node-type-specific query processing
     */
    ExecEndNode(planstate);

    /*
     * for subplans too
     */
    let mut l = if !(*estate).es_subplanstates.is_null() {
        list_head((*estate).es_subplanstates)
    } else {
        core::ptr::null_mut()
    };
    while !l.is_null() {
        let subplanstate = (*l).ptr_value as *mut PlanState;

        ExecEndNode(subplanstate);
        l = lnext((*estate).es_subplanstates, l);
    }

    /*
     * destroy the executor's tuple table.  Actually we only care about
     * releasing buffer pins and tupdesc refcounts; there's no need to pfree
     * the TupleTableSlots, since the containing memory context is about to go
     * away anyway.
     */
    ExecResetTupleTable((*estate).es_tupleTable, false);

    /*
     * Close any Relations that have been opened for range table entries or
     * result relations.
     */
    ExecCloseResultRelations(estate);
    ExecCloseRangeTableRelations(estate);
}

/*
 * Close any relations that have been opened for ResultRelInfos.
 */
pub unsafe fn ExecCloseResultRelations(estate: *mut EState) {
    /*
     * close indexes of result relation(s) if any.  (Rels themselves are
     * closed in ExecCloseRangeTableRelations())
     *
     * In addition, close the stub RTs that may be in each resultrel's
     * ri_ancestorResultRels.
     */
    let mut l = if !(*estate).es_opened_result_relations.is_null() {
        list_head((*estate).es_opened_result_relations)
    } else {
        core::ptr::null_mut()
    };
    while !l.is_null() {
        let resultRelInfo = (*l).ptr_value as *mut ResultRelInfo;

        ExecCloseIndices(resultRelInfo);
        let mut lc = if !(*resultRelInfo).ri_ancestorResultRels.is_null() {
            list_head((*resultRelInfo).ri_ancestorResultRels)
        } else {
            core::ptr::null_mut()
        };
        while !lc.is_null() {
            let rInfo = (*lc).ptr_value as *mut ResultRelInfo;

            /*
             * Ancestors with RTI > 0 (should only be the root ancestor) are
             * closed by ExecCloseRangeTableRelations.
             */
            if (*rInfo).ri_RangeTableIndex > 0 {
                lc = lnext((*resultRelInfo).ri_ancestorResultRels, lc);
                continue;
            }

            table_close((*rInfo).ri_RelationDesc, NoLock);
            lc = lnext((*resultRelInfo).ri_ancestorResultRels, lc);
        }
        l = lnext((*estate).es_opened_result_relations, l);
    }

    /* Close any relations that have been opened by ExecGetTriggerResultRel(). */
    let mut l = if !(*estate).es_trig_target_relations.is_null() {
        list_head((*estate).es_trig_target_relations)
    } else {
        core::ptr::null_mut()
    };
    while !l.is_null() {
        let resultRelInfo = (*l).ptr_value as *mut ResultRelInfo;

        /*
         * Assert this is a "dummy" ResultRelInfo, see above.  Otherwise we
         * might be issuing a duplicate close against a Relation opened by
         * ExecGetRangeTableRelation.
         */
        Assert!((*resultRelInfo).ri_RangeTableIndex == 0);

        /*
         * Since ExecGetTriggerResultRel doesn't call ExecOpenIndices for
         * these rels, we needn't call ExecCloseIndices either.
         */
        Assert!((*resultRelInfo).ri_NumIndices == 0);

        table_close((*resultRelInfo).ri_RelationDesc, NoLock);
        l = lnext((*estate).es_trig_target_relations, l);
    }
}

/*
 * Close all relations opened by ExecGetRangeTableRelation().
 *
 * We do not release any locks we might hold on those rels.
 */
pub unsafe fn ExecCloseRangeTableRelations(estate: *mut EState) {
    let mut i: c_int = 0;

    while i < (*estate).es_range_table_size as c_int {
        if !(*(*estate).es_relations.add(i as usize)).is_null() {
            table_close(*(*estate).es_relations.add(i as usize), NoLock);
        }
        i += 1;
    }
}

/* ----------------------------------------------------------------
 *      ExecutePlan
 *
 *      Processes the query plan until we have retrieved 'numberTuples' tuples,
 *      moving in the specified direction.
 *
 *      Runs to completion if numberTuples is 0
 * ----------------------------------------------------------------
 */
unsafe fn ExecutePlan(
    queryDesc: *mut QueryDesc,
    operation: CmdType,
    sendTuples: bool,
    numberTuples: uint64,
    direction: ScanDirection,
    dest: *mut DestReceiver,
) {
    let estate: *mut EState = (*queryDesc).estate;
    let planstate: *mut PlanState = (*queryDesc).planstate;
    let use_parallel_mode: bool;
    let mut slot: *mut TupleTableSlot;
    let mut current_tuple_count: uint64;

    /*
     * initialize local variables
     */
    current_tuple_count = 0;

    /*
     * Set the direction.
     */
    (*estate).es_direction = direction;

    /*
     * Set up parallel mode if appropriate.
     *
     * Parallel mode only supports complete execution of a plan.  If we've
     * already partially executed it, or if the caller asks us to exit early,
     * we must force the plan to run without parallelism.
     */
    let use_parallel_mode = if (*queryDesc).already_executed || numberTuples != 0 {
        false
    } else {
        (*(*queryDesc).plannedstmt).parallelModeNeeded
    };
    (*queryDesc).already_executed = true;

    (*estate).es_use_parallel_mode = use_parallel_mode;
    if use_parallel_mode {
        EnterParallelMode();
    }

    /*
     * Loop until we've processed the proper number of tuples from the plan.
     */
    loop {
        /* Reset the per-output-tuple exprcontext */
        ResetPerTupleExprContext(estate);

        /*
         * Execute the plan and obtain a tuple
         */
        slot = ExecProcNode(planstate);

        /*
         * if the tuple is null, then we assume there is nothing more to
         * process so we just end the loop...
         */
        if TupIsNull(slot) {
            break;
        }

        /*
         * If we have a junk filter, then project a new tuple with the junk
         * removed.
         *
         * Store this new "clean" tuple in the junkfilter's resultSlot.
         * (Formerly, we stored it back over the "dirty" tuple, which is WRONG
         * because that tuple slot has the wrong descriptor.)
         */
        if !(*estate).es_junkFilter.is_null() {
            slot = ExecFilterJunk((*estate).es_junkFilter, slot);
        }

        /*
         * If we are supposed to send the tuple somewhere, do so. (In
         * practice, this is probably always the case at this point.)
         */
        if sendTuples {
            /*
             * If we are not able to send the tuple, we assume the destination
             * has closed and no more tuples can be sent. If that's the case,
             * end the loop.
             */
            if let Some(receiveSlot) = (*dest).receiveSlot {
                if !receiveSlot(slot, dest) {
                    break;
                }
            }
        }

        /*
         * Count tuples processed, if this is a SELECT.  (For other operation
         * types, the ModifyTable plan node must count the appropriate
         * events.)
         */
        if operation == CMD_SELECT {
            (*estate).es_processed += 1;
        }

        /*
         * check our tuple count.. if we've processed the proper number then
         * quit, else loop again and process more tuples.  Zero numberTuples
         * means no limit.
         */
        current_tuple_count += 1;
        if numberTuples != 0 && numberTuples == current_tuple_count {
            break;
        }
    }

    /*
     * If we know we won't need to back up, we can release resources at this
     * point.
     */
    if ((*estate).es_top_eflags & EXEC_FLAG_BACKWARD) == 0 {
        ExecShutdownNode(planstate);
    }

    if use_parallel_mode {
        ExitParallelMode();
    }
}

/// TODO(pg-port): access/xact.h
unsafe fn EnterParallelMode() {
    crate::access::transam::xact::EnterParallelMode()
}

/// TODO(pg-port): access/xact.h
unsafe fn ExitParallelMode() {
    crate::access::transam::xact::ExitParallelMode()
}

/// TODO(pg-port): utils/sdir.h
#[inline]
unsafe fn ScanDirectionIsNoMovement(dir: ScanDirection) -> bool {
    dir == crate::access::sdir::NoMovementScanDirection
}

// ---------------------------------------------------------------------------
// Additional stubs needed by functions below
// ---------------------------------------------------------------------------

/// TODO(pg-port): access/tableam.h - LockTupleMode
pub type LockTupleMode = c_int;
pub const LockTupleExclusive: LockTupleMode = 4;
pub const LockTupleNoKeyExclusive: LockTupleMode = 3;

/// TODO(pg-port): access/heapam.h - RowMarkRequiresRowShareLock
#[inline]
unsafe fn RowMarkRequiresRowShareLock(marktype: RowMarkType) -> bool {
    // ROW_MARK_EXCLUSIVE, ROW_MARK_NOKEYEXCLUSIVE, ROW_MARK_SHARE, ROW_MARK_KEYSHARE
    // require a row share lock.  REFERENCE and COPY do not.
    !matches!(marktype, ROW_MARK_REFERENCE | ROW_MARK_COPY)
}

/// TODO(pg-port): access/tupdesc.h - TupleConstr
#[repr(C)]
pub struct TupleConstr {
    pub defval: *mut core::ffi::c_void,
    pub missing: *mut core::ffi::c_void,
    pub check: *mut ConstrCheck,
    pub clusterKeys: *mut core::ffi::c_void,
    pub num_defval: u16,
    pub num_check: u16,
    pub num_clusterkeys: u16,
    pub has_not_null: bool,
    pub has_generated_stored: bool,
    pub has_generated_virtual: bool,
}

/// TODO(pg-port): access/tupdesc.h - Form_pg_attribute / TupleDescAttr
#[repr(C)]
pub struct FormData_pg_attribute {
    pub attrelid: Oid,
    pub attname: crate::c::NameData,
    pub atttypid: Oid,
    pub attlen: i16,
    pub attnum: AttrNumber,
    pub attcacheoff: i32,
    pub atttypmod: i32,
    pub attndims: i16,
    pub attbyval: bool,
    pub attalign: c_char,
    pub attstorage: c_char,
    pub attcompression: c_char,
    pub attnotnull: bool,
    pub atthasdef: bool,
    pub atthasmissing: bool,
    pub attidentity: c_char,
    pub attgenerated: c_char,
    pub attisdropped: bool,
    pub attislocal: bool,
    pub attnfields: i32,
    pub attstattarget: i32,
    pub attcollation: Oid,
}
pub type Form_pg_attribute = *mut FormData_pg_attribute;
pub const ATTRIBUTE_GENERATED_VIRTUAL: c_char = b'v' as c_char;

#[inline]
unsafe fn TupleDescAttr(tupdesc: TupleDesc, i: usize) -> Form_pg_attribute {
    crate::access::common::tupdesc::TupleDescAttr(tupdesc, i as c_int) as *mut FormData_pg_attribute
}

/// TODO(pg-port): catalog/pg_attribute.h - NameStr macro
macro_rules! NameStr {
    ($name:expr) => {
        $name.data.as_ptr()
    };
}

/// TODO(pg-port): utils/palloc.h - palloc_array / palloc0_array
macro_rules! palloc_array {
    ($t:ty, $n:expr) => {
        palloc(core::mem::size_of::<$t>() * ($n) as usize) as *mut $t
    };
}
macro_rules! palloc0_array {
    ($t:ty, $n:expr) => {
        palloc0(core::mem::size_of::<$t>() * ($n) as usize) as *mut $t
    };
}

/// TODO(pg-port): nodes/params.h - ParamExecData
use crate::nodes::params::ParamExecData;

/// TODO(pg-port): utils/acl.h - ERRCODE constants
pub const ERRCODE_CHECK_VIOLATION: c_int = 0;        // TODO(pg-port): real value
pub const ERRCODE_NOT_NULL_VIOLATION: c_int = 0;     // TODO(pg-port): real value
pub const ERRCODE_WITH_CHECK_OPTION_VIOLATION: c_int = 0; // TODO(pg-port): real value
pub const ERRCODE_INSUFFICIENT_PRIVILEGE: c_int = 0; // TODO(pg-port): real value
pub const ERRCODE_FEATURE_NOT_SUPPORTED_2: c_int = 0; // already defined elsewhere

/// TODO(pg-port): utils/errcodes.h - errtable / errtableconstraint / errtablecol
unsafe fn errtable(_rel: Relation) -> c_int {
    crate::utils::cache::relcache::errtable(_rel as _) as _
}
unsafe fn errtableconstraint(_rel: Relation, _constrname: *const c_char) -> c_int {
    crate::utils::cache::relcache::errtableconstraint(_rel as _, _constrname as _) as _
}
unsafe fn errtablecol(_rel: Relation, _attnum: c_int) -> c_int {
    crate::utils::cache::relcache::errtablecol(_rel as _, _attnum as _) as _
}

/// TODO(pg-port): nodes/pg_list.h - NameData (already in pg_list?)
// If NameData is not in pg_list, define here:
// pub struct NameData { pub data: [c_char; 64] }  -- likely already in catalog types


// ---------------------------------------------------------------------------
// ExecRelCheck
// ---------------------------------------------------------------------------

/*
 * ExecRelCheck --- check that tuple meets all check-constraint expressions
 *
 * Returns the constraint name if check fails, else NULL.
 */
unsafe fn ExecRelCheck(
    resultRelInfo: *mut ResultRelInfo,
    slot: *mut TupleTableSlot,
    estate: *mut EState,
) -> *const c_char {
    let rel: Relation = (*resultRelInfo).ri_RelationDesc;
    let ncheck: c_int = (*(*(*rel).rd_att).constr).num_check as c_int;
    let check: *mut crate::access::common::tupdesc::ConstrCheck = (*(*(*rel).rd_att).constr).check;
    let econtext: *mut ExprContext;
    let oldContext: MemoryContext;

    /*
     * CheckNNConstraintFetch let this pass with only a warning, but now we
     * should fail rather than possibly failing to enforce an important
     * constraint.
     */
    if ncheck != (*(*rel).rd_rel).relchecks as c_int {
        elog!(ERROR, "{} pg_constraint record(s) missing for relation \"{}\"",
            (*(*rel).rd_rel).relchecks as c_int - ncheck,
            CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy());
    }

    /*
     * If first time through for this result relation, build expression
     * nodetrees for rel's constraint expressions.  Keep them in the per-query
     * memory context so they'll survive throughout the query.
     */
    if (*resultRelInfo).ri_CheckConstraintExprs.is_null() {
        oldContext = MemoryContextSwitchTo((*estate).es_query_cxt);
        (*resultRelInfo).ri_CheckConstraintExprs =
            palloc0_array!(*mut ExprState, ncheck);
        let mut i: c_int = 0;
        while i < ncheck {
            /* Skip not enforced constraint */
            if !(*check.add(i as usize)).ccenforced {
                i += 1;
                continue;
            }

            let checkconstr_raw = stringToNode((*check.add(i as usize)).ccbin) as *mut Expr;
            let checkconstr = expand_generated_columns_in_expr(
                checkconstr_raw as *mut Node, rel, 1) as *mut Expr;
            *(*resultRelInfo).ri_CheckConstraintExprs.add(i as usize) =
                ExecPrepareExpr(checkconstr, estate);
            i += 1;
        }
        MemoryContextSwitchTo(oldContext);
    }

    /*
     * We will use the EState's per-tuple context for evaluating constraint
     * expressions (creating it if it's not already there).
     */
    econtext = GetPerTupleExprContext(estate);

    /* Arrange for econtext's scan tuple to be the tuple under test */
    (*econtext).ecxt_scantuple = slot;

    /* And evaluate the constraints */
    let mut i: c_int = 0;
    while i < ncheck {
        let checkconstr: *mut ExprState =
            *(*resultRelInfo).ri_CheckConstraintExprs.add(i as usize);

        /*
         * NOTE: SQL specifies that a NULL result from a constraint expression
         * is not to be treated as a failure.  Therefore, use ExecCheck not
         * ExecQual.
         */
        if !checkconstr.is_null() && !ExecCheck(checkconstr, econtext) {
            return (*check.add(i as usize)).ccname;
        }
        i += 1;
    }

    /* NULL result means no error */
    core::ptr::null()
}

// ---------------------------------------------------------------------------
// ExecPartitionCheck
// ---------------------------------------------------------------------------

/*
 * ExecPartitionCheck --- check that tuple meets the partition constraint.
 *
 * Returns true if it meets the partition constraint.  If the constraint
 * fails and we're asked to emit an error, do so and don't return; otherwise
 * return false.
 */
pub unsafe fn ExecPartitionCheck(
    resultRelInfo: *mut ResultRelInfo,
    slot: *mut TupleTableSlot,
    estate: *mut EState,
    emitError: bool,
) -> bool {
    let econtext: *mut ExprContext;
    let success: bool;

    /*
     * If first time through, build expression state tree for the partition
     * check expression.
     */
    if (*resultRelInfo).ri_PartitionCheckExpr.is_null() {
        /*
         * Ensure that the qual tree and prepared expression are in the
         * query-lifespan context.
         */
        let oldcxt: MemoryContext =
            MemoryContextSwitchTo((*estate).es_query_cxt);
        let qual: *mut List =
            RelationGetPartitionQual((*resultRelInfo).ri_RelationDesc);
        (*resultRelInfo).ri_PartitionCheckExpr =
            ExecPrepareCheck(qual, estate);
        MemoryContextSwitchTo(oldcxt);
    }

    /*
     * We will use the EState's per-tuple context for evaluating constraint
     * expressions (creating it if it's not already there).
     */
    econtext = GetPerTupleExprContext(estate);

    /* Arrange for econtext's scan tuple to be the tuple under test */
    (*econtext).ecxt_scantuple = slot;

    /*
     * As in case of the cataloged constraints, we treat a NULL result as
     * success here, not a failure.
     */
    success = ExecCheck((*resultRelInfo).ri_PartitionCheckExpr, econtext);

    /* if asked to emit error, don't actually return on failure */
    if !success && emitError {
        ExecPartitionCheckEmitError(resultRelInfo, slot, estate);
    }

    success
}

// ---------------------------------------------------------------------------
// ExecPartitionCheckEmitError
// ---------------------------------------------------------------------------

/*
 * ExecPartitionCheckEmitError - Form and emit an error message after a failed
 * partition constraint check.
 */
pub unsafe fn ExecPartitionCheckEmitError(
    resultRelInfo: *mut ResultRelInfo,
    mut slot: *mut TupleTableSlot,
    estate: *mut EState,
) {
    let root_relid: Oid;
    let mut tupdesc: TupleDesc;
    let val_desc: *mut c_char;
    let modifiedCols: *mut Bitmapset;

    /*
     * If the tuple has been routed, it's been converted to the partition's
     * rowtype, which might differ from the root table's.  We must convert it
     * back to the root table's rowtype so that val_desc in the error message
     * matches the input tuple.
     */
    if !(*resultRelInfo).ri_RootResultRelInfo.is_null() {
        let rootrel: *mut ResultRelInfo = (*resultRelInfo).ri_RootResultRelInfo;
        let old_tupdesc: TupleDesc =
            RelationGetDescr((*resultRelInfo).ri_RelationDesc);
        /* a reverse map */
        let map: *mut AttrMap =
            build_attrmap_by_name_if_req(old_tupdesc, RelationGetDescr((*rootrel).ri_RelationDesc), false);

        root_relid = RelationGetRelid((*rootrel).ri_RelationDesc);
        tupdesc = RelationGetDescr((*rootrel).ri_RelationDesc);

        /*
         * Partition-specific slot's tupdesc can't be changed, so allocate a
         * new one.
         */
        if !map.is_null() {
            slot = execute_attr_map_slot(
                map,
                slot,
                MakeTupleTableSlot(tupdesc, &TTSOpsVirtual),
            );
        }
        let mc: *mut Bitmapset = bms_union(
            ExecGetInsertedCols(rootrel, estate),
            ExecGetUpdatedCols(rootrel, estate),
        );
        // modifiedCols assigned below (binding)
        val_desc = ExecBuildSlotValueDescription(
            root_relid, slot, tupdesc,
            bms_union(ExecGetInsertedCols(rootrel, estate),
                      ExecGetUpdatedCols(rootrel, estate)),
            64,
        );
    } else {
        root_relid = RelationGetRelid((*resultRelInfo).ri_RelationDesc);
        tupdesc = RelationGetDescr((*resultRelInfo).ri_RelationDesc);
        val_desc = ExecBuildSlotValueDescription(
            root_relid, slot, tupdesc,
            bms_union(ExecGetInsertedCols(resultRelInfo, estate),
                      ExecGetUpdatedCols(resultRelInfo, estate)),
            64,
        );
    }

    ereport!(ERROR, errmsg!("new row for relation \"{}\" violates partition constraint",
            CStr::from_ptr(RelationGetRelationName((*resultRelInfo).ri_RelationDesc)).to_string_lossy())
        /* C also: errcode(ERRCODE_CHECK_VIOLATION) /* C also: errdetail if val_desc; errtable */ */);
}

// ---------------------------------------------------------------------------
// ExecConstraints
// ---------------------------------------------------------------------------

/*
 * ExecConstraints - check constraints of the tuple in 'slot'
 *
 * This checks the traditional NOT NULL and check constraints.
 *
 * The partition constraint is *NOT* checked.
 */
pub unsafe fn ExecConstraints(
    resultRelInfo: *mut ResultRelInfo,
    mut slot: *mut TupleTableSlot,
    estate: *mut EState,
) {
    let rel: Relation = (*resultRelInfo).ri_RelationDesc;
    let mut tupdesc: TupleDesc = RelationGetDescr(rel);
    let constr: *mut crate::access::common::tupdesc::TupleConstr = (*tupdesc).constr;
    let mut notnull_virtual_attrs: *mut List = NIL;

    // Assert(constr); -- we should not be called otherwise

    /*
     * Verify not-null constraints.
     *
     * Not-null constraints on virtual generated columns are collected and
     * checked separately below.
     */
    if (*constr).has_not_null {
        let mut attnum: AttrNumber = 1;
        while attnum <= (*tupdesc).natts as AttrNumber {
            let att: Form_pg_attribute = TupleDescAttr(tupdesc, (attnum - 1) as usize);
            if (*att).attnotnull
                && (*att).attgenerated == ATTRIBUTE_GENERATED_VIRTUAL
            {
                notnull_virtual_attrs =
                    lappend_int_wrapper(notnull_virtual_attrs, attnum as c_int);
            } else if (*att).attnotnull
                && slot_attisnull(slot, attnum as c_int)
            {
                ReportNotNullViolationError(
                    resultRelInfo, slot, estate, attnum as c_int);
            }
            attnum += 1;
        }
    }

    /*
     * Verify not-null constraints on virtual generated column, if any.
     */
    if !notnull_virtual_attrs.is_null() {
        let attnum2: AttrNumber = ExecRelGenVirtualNotNull(
            resultRelInfo, slot, estate, notnull_virtual_attrs);
        if attnum2 != InvalidAttrNumber {
            ReportNotNullViolationError(
                resultRelInfo, slot, estate, attnum2 as c_int);
        }
    }

    /*
     * Verify check constraints.
     */
    if (*(*rel).rd_rel).relchecks > 0 {
        let failed: *const c_char =
            ExecRelCheck(resultRelInfo, slot, estate);
        if !failed.is_null() {
            let orig_rel: Relation = rel;
            let mut rel2: Relation = rel;
            let val_desc: *mut c_char;

            /*
             * If the tuple has been routed, it's been converted to the
             * partition's rowtype, which might differ from the root table's.
             * We must convert it back to the root table's rowtype so that
             * val_desc shown error message matches the input tuple.
             */
            if !(*resultRelInfo).ri_RootResultRelInfo.is_null() {
                let rootrel: *mut ResultRelInfo =
                    (*resultRelInfo).ri_RootResultRelInfo;
                let old_tupdesc: TupleDesc = RelationGetDescr(rel);
                tupdesc = RelationGetDescr((*rootrel).ri_RelationDesc);
                /* a reverse map */
                let map: *mut AttrMap =
                    build_attrmap_by_name_if_req(old_tupdesc, tupdesc, false);
                /*
                 * Partition-specific slot's tupdesc can't be changed, so
                 * allocate a new one.
                 */
                if !map.is_null() {
                    slot = execute_attr_map_slot(
                        map, slot,
                        MakeTupleTableSlot(tupdesc, &TTSOpsVirtual));
                }
                val_desc = ExecBuildSlotValueDescription(
                    RelationGetRelid((*rootrel).ri_RelationDesc),
                    slot, tupdesc,
                    bms_union(ExecGetInsertedCols(rootrel, estate),
                              ExecGetUpdatedCols(rootrel, estate)),
                    64,
                );
                rel2 = (*rootrel).ri_RelationDesc;
            } else {
                val_desc = ExecBuildSlotValueDescription(
                    RelationGetRelid(rel2),
                    slot, tupdesc,
                    bms_union(ExecGetInsertedCols(resultRelInfo, estate),
                              ExecGetUpdatedCols(resultRelInfo, estate)),
                    64,
                );
            }
            ereport!(ERROR, errmsg!("new row for relation \"{}\" violates check constraint \"{}\"",
                    CStr::from_ptr(RelationGetRelationName(orig_rel)).to_string_lossy(),
                    CStr::from_ptr(failed).to_string_lossy())
                /* C also: errcode(ERRCODE_CHECK_VIOLATION) /* C also: errdetail if val_desc; errtableconstraint */ */);
        }
    }
}


// ---------------------------------------------------------------------------
// ExecRelGenVirtualNotNull
// ---------------------------------------------------------------------------

/*
 * Verify not-null constraints on virtual generated columns of the given
 * tuple slot.
 *
 * Return value of InvalidAttrNumber means all not-null constraints on virtual
 * generated columns are satisfied.  A return value > 0 means a not-null
 * violation happened for that attribute.
 *
 * notnull_virtual_attrs is the list of the attnums of virtual generated column with
 * not-null constraints.
 */
pub unsafe fn ExecRelGenVirtualNotNull(
    resultRelInfo: *mut ResultRelInfo,
    slot: *mut TupleTableSlot,
    estate: *mut EState,
    notnull_virtual_attrs: *mut List,
) -> AttrNumber {
    let rel: Relation = (*resultRelInfo).ri_RelationDesc;
    let econtext: *mut ExprContext;
    let oldContext: MemoryContext;

    /*
     * We implement this by building a NullTest node for each virtual
     * generated column, which we cache in resultRelInfo, and running those
     * through ExecCheck().
     */
    if (*resultRelInfo).ri_GenVirtualNotNullConstraintExprs.is_null() {
        oldContext = MemoryContextSwitchTo((*estate).es_query_cxt);
        (*resultRelInfo).ri_GenVirtualNotNullConstraintExprs =
            palloc0_array!(*mut ExprState, list_length(notnull_virtual_attrs));

        let mut i: c_int = 0;
        let mut lc = list_head(notnull_virtual_attrs);
        while !lc.is_null() {
            let attnum: AttrNumber = lfirst_int(lc) as AttrNumber;

            /* "generated_expression IS NOT NULL" check. */
            let nnulltest: *mut NullTest = makeNode!(NullTest, T_NullTest);
            (*nnulltest).arg = build_generation_expression(rel, attnum) as *mut Expr;
            (*nnulltest).nulltesttype = IS_NOT_NULL;
            (*nnulltest).argisrow = false;
            (*nnulltest).location = -1;

            *(*resultRelInfo).ri_GenVirtualNotNullConstraintExprs.add(i as usize) =
                ExecPrepareExpr(nnulltest as *mut Expr, estate);
            i += 1;
            lc = lnext(notnull_virtual_attrs, lc);
        }
        MemoryContextSwitchTo(oldContext);
    }

    /*
     * We will use the EState's per-tuple context for evaluating virtual
     * generated column not null constraint expressions (creating it if it's
     * not already there).
     */
    econtext = GetPerTupleExprContext(estate);

    /* Arrange for econtext's scan tuple to be the tuple under test */
    (*econtext).ecxt_scantuple = slot;

    /* And evaluate the check constraints for virtual generated column */
    let mut i: c_int = 0;
    let mut lc = list_head(notnull_virtual_attrs);
    while !lc.is_null() {
        let attnum: AttrNumber = lfirst_int(lc) as AttrNumber;
        let exprstate: *mut ExprState =
            *(*resultRelInfo).ri_GenVirtualNotNullConstraintExprs.add(i as usize);

        // Assert(exprstate != NULL);
        if !ExecCheck(exprstate, econtext) {
            return attnum;
        }
        i += 1;
        lc = lnext(notnull_virtual_attrs, lc);
    }

    /* InvalidAttrNumber result means no error */
    InvalidAttrNumber
}

// ---------------------------------------------------------------------------
// ReportNotNullViolationError
// ---------------------------------------------------------------------------

/*
 * Report a violation of a not-null constraint that was already detected.
 */
unsafe fn ReportNotNullViolationError(
    resultRelInfo: *mut ResultRelInfo,
    mut slot: *mut TupleTableSlot,
    estate: *mut EState,
    attnum: c_int,
) {
    let mut rel: Relation = (*resultRelInfo).ri_RelationDesc;
    let orig_rel: Relation = rel;
    let mut tupdesc: TupleDesc = RelationGetDescr(rel);
    let orig_tupdesc: TupleDesc = RelationGetDescr(rel);
    let att: Form_pg_attribute = TupleDescAttr(tupdesc, (attnum - 1) as usize);
    let val_desc: *mut c_char;

    // Assert(attnum > 0);

    /*
     * If the tuple has been routed, it's been converted to the partition's
     * rowtype, which might differ from the root table's.  We must convert it
     * back to the root table's rowtype so that val_desc shown error message
     * matches the input tuple.
     */
    if !(*resultRelInfo).ri_RootResultRelInfo.is_null() {
        let rootrel: *mut ResultRelInfo = (*resultRelInfo).ri_RootResultRelInfo;
        tupdesc = RelationGetDescr((*rootrel).ri_RelationDesc);
        /* a reverse map */
        let map: *mut AttrMap =
            build_attrmap_by_name_if_req(orig_tupdesc, tupdesc, false);

        /*
         * Partition-specific slot's tupdesc can't be changed, so allocate a
         * new one.
         */
        if !map.is_null() {
            slot = execute_attr_map_slot(
                map, slot,
                MakeTupleTableSlot(tupdesc, &TTSOpsVirtual));
        }
        val_desc = ExecBuildSlotValueDescription(
            RelationGetRelid((*rootrel).ri_RelationDesc),
            slot, tupdesc,
            bms_union(ExecGetInsertedCols(rootrel, estate),
                      ExecGetUpdatedCols(rootrel, estate)),
            64,
        );
        rel = (*rootrel).ri_RelationDesc;
    } else {
        val_desc = ExecBuildSlotValueDescription(
            RelationGetRelid(rel),
            slot, tupdesc,
            bms_union(ExecGetInsertedCols(resultRelInfo, estate),
                      ExecGetUpdatedCols(resultRelInfo, estate)),
            64,
        );
    }

    ereport!(ERROR, errmsg!("null value in column of relation \"{}\" violates not-null constraint",
            CStr::from_ptr(RelationGetRelationName(orig_rel)).to_string_lossy())
        /* C also: errcode(ERRCODE_NOT_NULL_VIOLATION) /* C also: errdetail if val_desc; errtablecol */ */);
}


// ---------------------------------------------------------------------------
// ExecWithCheckOptions
// ---------------------------------------------------------------------------

/*
 * ExecWithCheckOptions -- check that tuple satisfies any WITH CHECK OPTIONs
 * of the specified kind.
 *
 * Note that this needs to be called multiple times to ensure that all kinds of
 * WITH CHECK OPTIONs are handled (both those from views which have the WITH
 * CHECK OPTION set and from row-level security policies).
 */
pub unsafe fn ExecWithCheckOptions(
    kind: WCOKind,
    resultRelInfo: *mut ResultRelInfo,
    mut slot: *mut TupleTableSlot,
    estate: *mut EState,
) {
    let mut rel: Relation = (*resultRelInfo).ri_RelationDesc;
    let mut tupdesc: TupleDesc = RelationGetDescr(rel);
    let econtext: *mut ExprContext;

    /*
     * We will use the EState's per-tuple context for evaluating constraint
     * expressions (creating it if it's not already there).
     */
    econtext = GetPerTupleExprContext(estate);

    /* Arrange for econtext's scan tuple to be the tuple under test */
    (*econtext).ecxt_scantuple = slot;

    /* Check each of the constraints */
    // forboth(l1, ri_WithCheckOptions, l2, ri_WithCheckOptionExprs)
    let mut l1 = if !(*resultRelInfo).ri_WithCheckOptions.is_null() {
        list_head((*resultRelInfo).ri_WithCheckOptions)
    } else {
        core::ptr::null_mut()
    };
    let mut l2 = if !(*resultRelInfo).ri_WithCheckOptionExprs.is_null() {
        list_head((*resultRelInfo).ri_WithCheckOptionExprs)
    } else {
        core::ptr::null_mut()
    };
    while !l1.is_null() && !l2.is_null() {
        let wco: *mut WithCheckOption = (*l1).ptr_value as *mut WithCheckOption;
        let wcoExpr: *mut ExprState = (*l2).ptr_value as *mut ExprState;

        /*
         * Skip any WCOs which are not the kind we are looking for at this
         * time.
         */
        if (*wco).kind != kind {
            l1 = lnext((*resultRelInfo).ri_WithCheckOptions, l1);
            l2 = lnext((*resultRelInfo).ri_WithCheckOptionExprs, l2);
            continue;
        }

        /*
         * WITH CHECK OPTION checks are intended to ensure that the new tuple
         * is visible (in the case of a view) or that it passes the
         * 'with-check' policy (in the case of row security).
         */
        if !ExecQual(wcoExpr, econtext) {
            match (*wco).kind {
                /*
                 * For WITH CHECK OPTIONs coming from views, we might be
                 * able to provide the details on the row.
                 */
                WCO_VIEW_CHECK => {
                    /* See the comment in ExecConstraints(). */
                    if !(*resultRelInfo).ri_RootResultRelInfo.is_null() {
                        let rootrel: *mut ResultRelInfo =
                            (*resultRelInfo).ri_RootResultRelInfo;
                        let old_tupdesc: TupleDesc = RelationGetDescr(rel);
                        tupdesc = RelationGetDescr((*rootrel).ri_RelationDesc);
                        /* a reverse map */
                        let map: *mut AttrMap =
                            build_attrmap_by_name_if_req(old_tupdesc, tupdesc, false);
                        /*
                         * Partition-specific slot's tupdesc can't be changed,
                         * so allocate a new one.
                         */
                        if !map.is_null() {
                            slot = execute_attr_map_slot(
                                map, slot,
                                MakeTupleTableSlot(tupdesc, &TTSOpsVirtual));
                        }
                        rel = (*rootrel).ri_RelationDesc;
                        let val_desc = ExecBuildSlotValueDescription(
                            RelationGetRelid(rel), slot, tupdesc,
                            bms_union(ExecGetInsertedCols(rootrel, estate),
                                      ExecGetUpdatedCols(rootrel, estate)),
                            64,
                        );
                        ereport!(ERROR, errmsg!("new row violates check option for view \"{}\"",
                                CStr::from_ptr((*wco).relname).to_string_lossy())
                            /* C also: errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION) /* C also: errdetail if val_desc */ */);
                    } else {
                        let val_desc = ExecBuildSlotValueDescription(
                            RelationGetRelid(rel), slot, tupdesc,
                            bms_union(ExecGetInsertedCols(resultRelInfo, estate),
                                      ExecGetUpdatedCols(resultRelInfo, estate)),
                            64,
                        );
                        ereport!(ERROR, errmsg!("new row violates check option for view \"{}\"",
                                CStr::from_ptr((*wco).relname).to_string_lossy())
                            /* C also: errcode /* C also: errdetail */ */);
                    }
                }
                WCO_RLS_INSERT_CHECK | WCO_RLS_UPDATE_CHECK => {
                    if !(*wco).polname.is_null() {
                        ereport!(ERROR, errmsg!("new row violates row-level security policy \"{}\" for table \"{}\"",
                                CStr::from_ptr((*wco).polname).to_string_lossy(),
                                CStr::from_ptr((*wco).relname).to_string_lossy())
                            /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */);
                    } else {
                        ereport!(ERROR, errmsg!("new row violates row-level security policy for table \"{}\"",
                                CStr::from_ptr((*wco).relname).to_string_lossy())
                            /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */);
                    }
                }
                WCO_RLS_MERGE_UPDATE_CHECK | WCO_RLS_MERGE_DELETE_CHECK => {
                    if !(*wco).polname.is_null() {
                        ereport!(ERROR, errmsg!("target row violates row-level security policy \"{}\" (USING expression) for table \"{}\"",
                                CStr::from_ptr((*wco).polname).to_string_lossy(),
                                CStr::from_ptr((*wco).relname).to_string_lossy())
                            /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */);
                    } else {
                        ereport!(ERROR, errmsg!("target row violates row-level security policy (USING expression) for table \"{}\"",
                                CStr::from_ptr((*wco).relname).to_string_lossy())
                            /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */);
                    }
                }
                WCO_RLS_CONFLICT_CHECK => {
                    if !(*wco).polname.is_null() {
                        ereport!(ERROR, errmsg!("new row violates row-level security policy \"{}\" (USING expression) for table \"{}\"",
                                CStr::from_ptr((*wco).polname).to_string_lossy(),
                                CStr::from_ptr((*wco).relname).to_string_lossy())
                            /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */);
                    } else {
                        ereport!(ERROR, errmsg!("new row violates row-level security policy (USING expression) for table \"{}\"",
                                CStr::from_ptr((*wco).relname).to_string_lossy())
                            /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */);
                    }
                }
                _ => {
                    elog!(ERROR, "unrecognized WCO kind: {}", (*wco).kind as u32);
                }
            }
        }
        l1 = lnext((*resultRelInfo).ri_WithCheckOptions, l1);
        l2 = lnext((*resultRelInfo).ri_WithCheckOptionExprs, l2);
    }
}


// ---------------------------------------------------------------------------
// ExecBuildSlotValueDescription
// ---------------------------------------------------------------------------

/*
 * ExecBuildSlotValueDescription -- construct a string representing a tuple
 *
 * This is intentionally very similar to BuildIndexValueDescription, but
 * unlike that function, we truncate long field values (to at most maxfieldlen
 * bytes).
 */
pub unsafe fn ExecBuildSlotValueDescription(
    reloid: Oid,
    slot: *mut TupleTableSlot,
    tupdesc: TupleDesc,
    modifiedCols: *mut Bitmapset,
    maxfieldlen: c_int,
) -> *mut c_char {
    let mut buf: StringInfoData = core::mem::zeroed();
    let mut collist: StringInfoData = core::mem::zeroed();
    let mut write_comma = false;
    let mut write_comma_collist = false;
    let mut aclresult: AclResult;
    let mut table_perm = false;
    let mut any_perm = false;

    /*
     * Check if RLS is enabled and should be active for the relation; if so,
     * then don't return anything.
     */
    if check_enable_rls(reloid, InvalidOid, true) == RLS_ENABLED {
        return core::ptr::null_mut();
    }

    initStringInfo(&mut buf);

    appendStringInfoChar(&mut buf, b'(' as c_char);

    /*
     * Check if the user has permissions to see the row.
     */
    aclresult = pg_class_aclcheck(reloid, GetUserId(), ACL_SELECT);
    if aclresult != ACLCHECK_OK {
        /* Set up the buffer for the column list */
        initStringInfo(&mut collist);
        appendStringInfoChar(&mut collist, b'(' as c_char);
    } else {
        table_perm = true;
        any_perm = true;
    }

    /* Make sure the tuple is fully deconstructed */
    slot_getallattrs(slot);

    let mut i: c_int = 0;
    while i < (*tupdesc).natts as c_int {
        let mut column_perm = false;
        let att: Form_pg_attribute = TupleDescAttr(tupdesc, i as usize);

        /* ignore dropped columns */
        if (*att).attisdropped {
            i += 1;
            continue;
        }

        if !table_perm {
            /*
             * No table-level SELECT, so need to make sure they either have
             * SELECT rights on the column or that they have provided the data
             * for the column.
             */
            aclresult = pg_attribute_aclcheck(reloid, (*att).attnum,
                                               GetUserId(), ACL_SELECT);
            if bms_is_member(
                ((*att).attnum - FirstLowInvalidHeapAttributeNumber as AttrNumber) as c_int,
                modifiedCols,
            ) || aclresult == ACLCHECK_OK
            {
                column_perm = true;
                any_perm = true;

                if write_comma_collist {
                    appendStringInfoString(
                        &mut collist,
                        c", ".as_ptr(),
                    );
                } else {
                    write_comma_collist = true;
                }

                appendStringInfoString(
                    &mut collist,
                    (*att).attname.data.as_ptr(),
                );
            }
        }

        if table_perm || column_perm {
            let val: *const c_char;
            if (*att).attgenerated == ATTRIBUTE_GENERATED_VIRTUAL {
                val = c"virtual".as_ptr();
            } else if *(*slot).tts_isnull.add(i as usize) {
                val = c"null".as_ptr();
            } else {
                let mut foutoid: Oid = 0;
                let mut typisvarlena: bool = false;
                getTypeOutputInfo((*att).atttypid,
                                  &mut foutoid, &mut typisvarlena);
                val = OidOutputFunctionCall(
                    foutoid,
                    *(*slot).tts_values.add(i as usize),
                );
            }

            if write_comma {
                appendStringInfoString(&mut buf, c", ".as_ptr());
            } else {
                write_comma = true;
            }

            /* truncate if needed */
            let vallen = libc_strlen(val) as c_int;
            if vallen <= maxfieldlen {
                appendBinaryStringInfo(&mut buf, val as *const core::ffi::c_void, vallen);
            } else {
                let clipped = pg_mbcliplen(val, vallen, maxfieldlen);
                appendBinaryStringInfo(&mut buf, val as *const core::ffi::c_void, clipped);
                appendStringInfoString(&mut buf, c"...".as_ptr());
            }
        }
        i += 1;
    }

    /* If we end up with zero columns being returned, then return NULL. */
    if !any_perm {
        return core::ptr::null_mut();
    }

    appendStringInfoChar(&mut buf, b')' as c_char);

    if !table_perm {
        appendStringInfoString(&mut collist, c") = ".as_ptr());
        appendBinaryStringInfo(&mut collist, buf.data as *const core::ffi::c_void, buf.len);
        return collist.data;
    }

    buf.data
}

/// TODO(pg-port): libc strlen
unsafe fn libc_strlen(s: *const c_char) -> usize {
    extern "C" { fn strlen(s: *const c_char) -> usize; }
    strlen(s)
}

// ---------------------------------------------------------------------------
// ExecUpdateLockMode
// ---------------------------------------------------------------------------

/*
 * ExecUpdateLockMode -- find the appropriate UPDATE tuple lock mode for a
 * given ResultRelInfo
 */
pub unsafe fn ExecUpdateLockMode(
    estate: *mut EState,
    relinfo: *mut ResultRelInfo,
) -> LockTupleMode {
    let keyCols: *mut Bitmapset;
    let updatedCols: *mut Bitmapset;

    /*
     * Compute lock mode to use.  If columns that are part of the key have not
     * been modified, then we can use a weaker lock, allowing for better
     * concurrency.
     */
    updatedCols = ExecGetAllUpdatedCols(relinfo, estate);
    keyCols = RelationGetIndexAttrBitmap(
        (*relinfo).ri_RelationDesc,
        INDEX_ATTR_BITMAP_KEY,
    );

    if bms_overlap(keyCols, updatedCols) {
        return LockTupleExclusive;
    }

    LockTupleNoKeyExclusive
}

// ---------------------------------------------------------------------------
// ExecFindRowMark
// ---------------------------------------------------------------------------

/*
 * ExecFindRowMark -- find the ExecRowMark struct for given rangetable index
 *
 * If no such struct, either return NULL or throw error depending on missing_ok
 */
pub unsafe fn ExecFindRowMark(
    estate: *mut EState,
    rti: Index,
    missing_ok: bool,
) -> *mut ExecRowMark {
    if rti > 0
        && rti <= (*estate).es_range_table_size
        && !(*estate).es_rowmarks.is_null()
    {
        let erm: *mut ExecRowMark =
            *(*estate).es_rowmarks.add((rti - 1) as usize);
        if !erm.is_null() {
            return erm;
        }
    }
    if !missing_ok {
        elog!(ERROR, "failed to find ExecRowMark for rangetable index {}", rti);
    }
    core::ptr::null_mut()
}

// ---------------------------------------------------------------------------
// ExecBuildAuxRowMark
// ---------------------------------------------------------------------------

/*
 * ExecBuildAuxRowMark -- create an ExecAuxRowMark struct
 *
 * Inputs are the underlying ExecRowMark struct and the targetlist of the
 * input plan node (not planstate node!).
 */
pub unsafe fn ExecBuildAuxRowMark(
    erm: *mut ExecRowMark,
    targetlist: *mut List,
) -> *mut ExecAuxRowMark {
    let aerm: *mut ExecAuxRowMark =
        palloc0(core::mem::size_of::<ExecAuxRowMark>()) as *mut ExecAuxRowMark;
    let mut resname: [c_char; 32] = [0; 32];

    (*aerm).rowmark = erm;

    /* Look up the resjunk columns associated with this rowmark */
    if (*erm).markType != ROW_MARK_COPY {
        /* need ctid for all methods other than COPY */
        snprintf_libc(
            resname.as_mut_ptr(),
            32,
            c"ctid%u".as_ptr(),
            (*erm).rowmarkId,
        );
        (*aerm).ctidAttNo = ExecFindJunkAttributeInTlist(
            targetlist,
            resname.as_ptr(),
        );
        if !AttributeNumberIsValid((*aerm).ctidAttNo) {
            elog!(ERROR, "could not find junk ctid column");
        }
    } else {
        /* need wholerow if COPY */
        snprintf_libc(
            resname.as_mut_ptr(),
            32,
            c"wholerow%u".as_ptr(),
            (*erm).rowmarkId,
        );
        (*aerm).wholeAttNo = ExecFindJunkAttributeInTlist(
            targetlist,
            resname.as_ptr(),
        );
        if !AttributeNumberIsValid((*aerm).wholeAttNo) {
            elog!(ERROR, "could not find junk wholerow column");
        }
    }

    /* if child rel, need tableoid */
    if (*erm).rti != (*erm).prti {
        snprintf_libc(
            resname.as_mut_ptr(),
            32,
            c"tableoid%u".as_ptr(),
            (*erm).rowmarkId,
        );
        (*aerm).toidAttNo = ExecFindJunkAttributeInTlist(
            targetlist,
            resname.as_ptr(),
        );
        if !AttributeNumberIsValid((*aerm).toidAttNo) {
            elog!(ERROR, "could not find junk tableoid column");
        }
    }

    aerm
}


// ---------------------------------------------------------------------------
// EvalPlanQual logic
//
// EvalPlanQual logic --- recheck modified tuple(s) to see if we want to
// process the updated version under READ COMMITTED rules.
//
// See backend/executor/README for some info about how this works.
// ---------------------------------------------------------------------------

/*
 * Check the updated version of a tuple to see if we want to process it under
 * READ COMMITTED rules.
 */
pub unsafe fn EvalPlanQual(
    epqstate: *mut EPQState,
    relation: Relation,
    rti: Index,
    inputslot: *mut TupleTableSlot,
) -> *mut TupleTableSlot {
    let mut slot: *mut TupleTableSlot;
    let testslot: *mut TupleTableSlot;

    // Assert(rti > 0);

    /*
     * Need to run a recheck subquery.  Initialize or reinitialize EPQ state.
     */
    EvalPlanQualBegin(epqstate);

    /*
     * Callers will often use the EvalPlanQualSlot to store the tuple to avoid
     * an unnecessary copy.
     */
    testslot = EvalPlanQualSlot(epqstate, relation, rti);
    if testslot != inputslot {
        ExecCopySlot(testslot, inputslot);
    }

    /*
     * Mark that an EPQ tuple is available for this relation.
     */
    *(*epqstate).relsubs_done.add((rti - 1) as usize) = false;
    *(*epqstate).relsubs_blocked.add((rti - 1) as usize) = false;

    /*
     * Run the EPQ query.  We assume it will return at most one tuple.
     */
    slot = EvalPlanQualNext(epqstate);

    /*
     * If we got a tuple, force the slot to materialize the tuple so that it
     * is not dependent on any local state in the EPQ query.
     */
    if !TupIsNull(slot) {
        ExecMaterializeSlot(slot);
    }

    /*
     * Clear out the test tuple, and mark that no tuple is available here.
     */
    ExecClearTuple(testslot);
    *(*epqstate).relsubs_blocked.add((rti - 1) as usize) = true;

    slot
}

/*
 * EvalPlanQualInit -- initialize during creation of a plan state node
 * that might need to invoke EPQ processing.
 */
pub unsafe fn EvalPlanQualInit(
    epqstate: *mut EPQState,
    parentestate: *mut EState,
    subplan: *mut Plan,
    auxrowmarks: *mut List,
    epqParam: c_int,
    resultRelations: *mut List,
) {
    let rtsize: Index = (*parentestate).es_range_table_size;

    /* initialize data not changing over EPQState's lifetime */
    (*epqstate).parentestate = parentestate;
    (*epqstate).epqParam = epqParam;
    (*epqstate).resultRelations = resultRelations;

    /*
     * Allocate space to reference a slot for each potential rti.
     */
    (*epqstate).tuple_table = NIL;
    (*epqstate).relsubs_slot = palloc0(
        rtsize as usize * core::mem::size_of::<*mut TupleTableSlot>(),
    ) as *mut *mut TupleTableSlot;

    /* ... and remember data that EvalPlanQualBegin will need */
    (*epqstate).plan = subplan;
    (*epqstate).arowMarks = auxrowmarks;

    /* ... and mark the EPQ state inactive */
    (*epqstate).origslot = core::ptr::null_mut();
    (*epqstate).recheckestate = core::ptr::null_mut();
    (*epqstate).recheckplanstate = core::ptr::null_mut();
    (*epqstate).relsubs_rowmark = core::ptr::null_mut();
    (*epqstate).relsubs_done = core::ptr::null_mut();
    (*epqstate).relsubs_blocked = core::ptr::null_mut();
}

/*
 * EvalPlanQualSetPlan -- set or change subplan of an EPQState.
 */
pub unsafe fn EvalPlanQualSetPlan(
    epqstate: *mut EPQState,
    subplan: *mut Plan,
    auxrowmarks: *mut List,
) {
    /* If we have a live EPQ query, shut it down */
    EvalPlanQualEnd(epqstate);
    /* And set/change the plan pointer */
    (*epqstate).plan = subplan;
    /* The rowmarks depend on the plan, too */
    (*epqstate).arowMarks = auxrowmarks;
}

/*
 * Return, and create if necessary, a slot for an EPQ test tuple.
 *
 * Note this only requires EvalPlanQualInit() to have been called,
 * EvalPlanQualBegin() is not necessary.
 */
pub unsafe fn EvalPlanQualSlot(
    epqstate: *mut EPQState,
    relation: Relation,
    rti: Index,
) -> *mut TupleTableSlot {
    // Assert(relation);
    // Assert(rti > 0 && rti <= parentestate->es_range_table_size);
    let slot: *mut *mut TupleTableSlot =
        (*epqstate).relsubs_slot.add((rti - 1) as usize);

    if (*slot).is_null() {
        let oldcontext: MemoryContext = MemoryContextSwitchTo(
            (*(*epqstate).parentestate).es_query_cxt,
        );
        *slot = table_slot_create(relation, &mut (*epqstate).tuple_table);
        MemoryContextSwitchTo(oldcontext);
    }

    *slot
}

/*
 * Fetch the current row value for a non-locked relation, identified by rti,
 * that needs to be scanned by an EvalPlanQual operation.
 */
pub unsafe fn EvalPlanQualFetchRowMark(
    epqstate: *mut EPQState,
    rti: Index,
    slot: *mut TupleTableSlot,
) -> bool {
    let earm: *mut ExecAuxRowMark =
        *(*epqstate).relsubs_rowmark.add((rti - 1) as usize);
    // Assert(earm != NULL);
    // Assert(epqstate->origslot != NULL);

    let erm: *mut ExecRowMark = (*earm).rowmark;
    let mut datum: Datum;
    let mut isNull: bool = false;

    if RowMarkRequiresRowShareLock((*erm).markType) {
        elog!(ERROR, "EvalPlanQual doesn't support locking rowmarks");
    }

    /* if child rel, must check whether it produced this row */
    if (*erm).rti != (*erm).prti {
        datum = ExecGetJunkAttribute((*epqstate).origslot,
                                     (*earm).toidAttNo,
                                     &mut isNull);
        /* non-locked rels could be on the inside of outer joins */
        if isNull {
            return false;
        }

        let tableoid: Oid = DatumGetObjectId(datum);

        // Assert(OidIsValid(erm->relid));
        if tableoid != (*erm).relid {
            /* this child is inactive right now */
            return false;
        }
    }

    if (*erm).markType == ROW_MARK_REFERENCE {
        // Assert(erm->relation != NULL);

        /* fetch the tuple's ctid */
        datum = ExecGetJunkAttribute((*epqstate).origslot,
                                     (*earm).ctidAttNo,
                                     &mut isNull);
        /* non-locked rels could be on the inside of outer joins */
        if isNull {
            return false;
        }

        /* fetch requests on foreign tables must be passed to their FDW */
        if (*(*(*erm).relation).rd_rel).relkind == RELKIND_FOREIGN_TABLE {
            let fdwroutine: *mut FdwRoutine =
                GetFdwRoutineForRelation((*erm).relation, false);
            /* this should have been checked already, but let's be safe */
            // if fdwroutine->RefetchForeignRow == NULL: ereport not_supported
            // RefetchForeignRow is a function pointer in FdwRoutine; stub:
            ereport!(ERROR, errmsg!("cannot lock rows in foreign table \"{}\"",
                    CStr::from_ptr(RelationGetRelationName((*erm).relation)).to_string_lossy())
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */);
        } else {
            /* ordinary table, fetch the tuple */
            if !table_tuple_fetch_row_version(
                (*erm).relation,
                DatumGetPointer(datum) as *mut crate::storage::itemptr::ItemPointerData,
                GetActiveSnapshot(),
                slot,
            ) {
                elog!(ERROR, "failed to fetch tuple for EvalPlanQual recheck");
            }
            return true;
        }
    } else {
        // Assert(erm->markType == ROW_MARK_COPY);

        /* fetch the whole-row Var for the relation */
        datum = ExecGetJunkAttribute((*epqstate).origslot,
                                     (*earm).wholeAttNo,
                                     &mut isNull);
        /* non-locked rels could be on the inside of outer joins */
        if isNull {
            return false;
        }

        ExecStoreHeapTupleDatum(datum, slot);
        return true;
    }

    false // unreachable in non-foreign-table path
}

/*
 * Fetch the next row (if any) from EvalPlanQual testing
 */
pub unsafe fn EvalPlanQualNext(epqstate: *mut EPQState) -> *mut TupleTableSlot {
    let oldcontext: MemoryContext = MemoryContextSwitchTo(
        (*(*epqstate).recheckestate).es_query_cxt,
    );
    let slot: *mut TupleTableSlot = ExecProcNode((*epqstate).recheckplanstate);
    MemoryContextSwitchTo(oldcontext);
    slot
}

/*
 * Initialize or reset an EvalPlanQual state tree
 */
pub unsafe fn EvalPlanQualBegin(epqstate: *mut EPQState) {
    let parentestate: *mut EState = (*epqstate).parentestate;
    let recheckestate: *mut EState = (*epqstate).recheckestate;

    if recheckestate.is_null() {
        /* First time through, so create a child EState */
        EvalPlanQualStart(epqstate, (*epqstate).plan);
    } else {
        /*
         * We already have a suitable child EPQ tree, so just reset it.
         */
        let rtsize: Index = (*parentestate).es_range_table_size;
        let rcplanstate: *mut PlanState = (*epqstate).recheckplanstate;

        /*
         * Reset the relsubs_done[] flags to equal relsubs_blocked[], so that
         * the EPQ run will never attempt to fetch tuples from blocked target
         * relations.
         */
        core::ptr::copy_nonoverlapping(
            (*epqstate).relsubs_blocked,
            (*epqstate).relsubs_done,
            rtsize as usize,
        );

        /* Recopy current values of parent parameters */
        if !(*(*parentestate).es_plannedstmt).paramExecTypes.is_null() {
            /*
             * Force evaluation of any InitPlan outputs that could be needed
             * by the subplan.
             */
            ExecSetParamPlanMulti(
                (*(*rcplanstate).plan).extParam,
                GetPerTupleExprContext(parentestate),
            );

            let mut i: c_int =
                list_length((*(*parentestate).es_plannedstmt).paramExecTypes);

            while i > 0 {
                i -= 1;
                /* copy value if any, but not execPlan link */
                (*(*recheckestate).es_param_exec_vals.add(i as usize)).value =
                    (*(*parentestate).es_param_exec_vals.add(i as usize)).value;
                (*(*recheckestate).es_param_exec_vals.add(i as usize)).isnull =
                    (*(*parentestate).es_param_exec_vals.add(i as usize)).isnull;
            }
        }

        /*
         * Mark child plan tree as needing rescan at all scan nodes.
         */
        (*rcplanstate).chgParam = bms_add_member(
            (*rcplanstate).chgParam,
            (*epqstate).epqParam,
        );
    }
}


// ---------------------------------------------------------------------------
// EvalPlanQualStart
// ---------------------------------------------------------------------------

/*
 * Start execution of an EvalPlanQual plan tree.
 *
 * This is a cut-down version of ExecutorStart(): we copy some state from
 * the top-level estate rather than initializing it fresh.
 */
unsafe fn EvalPlanQualStart(epqstate: *mut EPQState, planTree: *mut Plan) {
    let parentestate: *mut EState = (*epqstate).parentestate;
    let rtsize: Index = (*parentestate).es_range_table_size;
    let rcestate: *mut EState;
    let oldcontext: MemoryContext;

    epqstate.as_mut().unwrap().recheckestate = CreateExecutorState();
    let rcestate = (*epqstate).recheckestate;

    oldcontext = MemoryContextSwitchTo((*rcestate).es_query_cxt);

    /* signal that this is an EState for executing EPQ */
    (*rcestate).es_epq_active = epqstate;

    /*
     * Child EPQ EStates share the parent's copy of unchanging state such as
     * the snapshot, rangetable, and external Param info.
     */
    (*rcestate).es_direction = crate::access::sdir::ForwardScanDirection;
    (*rcestate).es_snapshot = (*parentestate).es_snapshot;
    (*rcestate).es_crosscheck_snapshot = (*parentestate).es_crosscheck_snapshot;
    (*rcestate).es_range_table = (*parentestate).es_range_table;
    (*rcestate).es_range_table_size = (*parentestate).es_range_table_size;
    (*rcestate).es_relations = (*parentestate).es_relations;
    (*rcestate).es_rowmarks = (*parentestate).es_rowmarks;
    (*rcestate).es_rteperminfos = (*parentestate).es_rteperminfos;
    (*rcestate).es_plannedstmt = (*parentestate).es_plannedstmt;
    (*rcestate).es_junkFilter = (*parentestate).es_junkFilter;
    (*rcestate).es_output_cid = (*parentestate).es_output_cid;
    (*rcestate).es_queryEnv = (*parentestate).es_queryEnv;

    /*
     * ResultRelInfos needed by subplans are initialized from scratch when the
     * subplans themselves are initialized.
     */
    (*rcestate).es_result_relations = core::ptr::null_mut();
    /* es_trig_target_relations must NOT be copied */
    (*rcestate).es_top_eflags = (*parentestate).es_top_eflags;
    (*rcestate).es_instrument = (*parentestate).es_instrument;
    /* es_auxmodifytables must NOT be copied */

    /*
     * The external param list is simply shared from parent.  The internal
     * param workspace has to be local state, but we copy the initial values
     * from the parent.
     */
    (*rcestate).es_param_list_info = (*parentestate).es_param_list_info;
    if !(*(*parentestate).es_plannedstmt).paramExecTypes.is_null() {
        /*
         * Force evaluation of any InitPlan outputs that could be needed by
         * the subplan.
         */
        ExecSetParamPlanMulti(
            (*planTree).extParam,
            GetPerTupleExprContext(parentestate),
        );

        /* now make the internal param workspace ... */
        let mut i: c_int =
            list_length((*(*parentestate).es_plannedstmt).paramExecTypes);
        (*rcestate).es_param_exec_vals = palloc0(
            i as usize * core::mem::size_of::<ParamExecData>(),
        ) as *mut ParamExecData;
        /* ... and copy down all values, whether really needed or not */
        while i > 0 {
            i -= 1;
            /* copy value if any, but not execPlan link */
            (*(*rcestate).es_param_exec_vals.add(i as usize)).value =
                (*(*parentestate).es_param_exec_vals.add(i as usize)).value;
            (*(*rcestate).es_param_exec_vals.add(i as usize)).isnull =
                (*(*parentestate).es_param_exec_vals.add(i as usize)).isnull;
        }
    }

    /*
     * Copy es_unpruned_relids so that pruned relations are ignored.
     */
    (*rcestate).es_unpruned_relids = (*parentestate).es_unpruned_relids;

    /*
     * Also make the PartitionPruneInfo and the results of pruning available.
     */
    (*rcestate).es_part_prune_infos = (*parentestate).es_part_prune_infos;
    (*rcestate).es_part_prune_states = (*parentestate).es_part_prune_states;
    (*rcestate).es_part_prune_results = (*parentestate).es_part_prune_results;

    /* We'll also borrow the es_partition_directory from the parent state */
    (*rcestate).es_partition_directory = (*parentestate).es_partition_directory;

    /*
     * Initialize private state information for each SubPlan.
     */
    // Assert(rcestate->es_subplanstates == NIL);
    {
        let mut lc = if !(*(*parentestate).es_plannedstmt).subplans.is_null() {
            list_head((*(*parentestate).es_plannedstmt).subplans)
        } else {
            core::ptr::null_mut()
        };
        while !lc.is_null() {
            let subplan: *mut Plan = (*lc).ptr_value as *mut Plan;
            let subplanstate: *mut PlanState = ExecInitNode(subplan, rcestate, 0);
            (*rcestate).es_subplanstates = lappend(
                (*rcestate).es_subplanstates, subplanstate as *mut core::ffi::c_void);
            lc = lnext((*(*parentestate).es_plannedstmt).subplans, lc);
        }
    }

    /*
     * Build an RTI indexed array of rowmarks.
     */
    (*epqstate).relsubs_rowmark = palloc0(
        rtsize as usize * core::mem::size_of::<*mut ExecAuxRowMark>(),
    ) as *mut *mut ExecAuxRowMark;
    {
        let mut lc = if !(*epqstate).arowMarks.is_null() {
            list_head((*epqstate).arowMarks)
        } else {
            core::ptr::null_mut()
        };
        while !lc.is_null() {
            let earm: *mut ExecAuxRowMark = (*lc).ptr_value as *mut ExecAuxRowMark;
            *(*epqstate).relsubs_rowmark.add(
                ((*(*earm).rowmark).rti - 1) as usize,
            ) = earm;
            lc = lnext((*epqstate).arowMarks, lc);
        }
    }

    /*
     * Initialize per-relation EPQ tuple states.
     */
    (*epqstate).relsubs_done =
        palloc_array!(bool, rtsize);
    (*epqstate).relsubs_blocked =
        palloc0_array!(bool, rtsize);

    {
        let mut lc = if !(*epqstate).resultRelations.is_null() {
            list_head((*epqstate).resultRelations)
        } else {
            core::ptr::null_mut()
        };
        while !lc.is_null() {
            let rtindex: c_int = lfirst_int(lc);
            // Assert(rtindex > 0 && rtindex <= rtsize);
            *(*epqstate).relsubs_blocked.add((rtindex - 1) as usize) = true;
            lc = lnext((*epqstate).resultRelations, lc);
        }
    }

    core::ptr::copy_nonoverlapping(
        (*epqstate).relsubs_blocked,
        (*epqstate).relsubs_done,
        rtsize as usize,
    );

    /*
     * Initialize the private state information for all the nodes in the part
     * of the plan tree we need to run.
     */
    (*epqstate).recheckplanstate = ExecInitNode(planTree, rcestate, 0);

    MemoryContextSwitchTo(oldcontext);
}

// ---------------------------------------------------------------------------
// EvalPlanQualEnd
// ---------------------------------------------------------------------------

/*
 * EvalPlanQualEnd -- shut down at termination of parent plan state node,
 * or if we are done with the current EPQ child.
 *
 * This is a cut-down version of ExecutorEnd().
 */
pub unsafe fn EvalPlanQualEnd(epqstate: *mut EPQState) {
    let estate: *mut EState = (*epqstate).recheckestate;
    let rtsize: Index = (*(*epqstate).parentestate).es_range_table_size;
    let oldcontext: MemoryContext;

    /*
     * We may have a tuple table, even if EPQ wasn't started.
     */
    if !(*epqstate).tuple_table.is_null() {
        core::ptr::write_bytes(
            (*epqstate).relsubs_slot,
            0,
            rtsize as usize,
        );
        ExecResetTupleTable((*epqstate).tuple_table, true);
        (*epqstate).tuple_table = NIL;
    }

    /* EPQ wasn't started, nothing further to do */
    if estate.is_null() {
        return;
    }

    oldcontext = MemoryContextSwitchTo((*estate).es_query_cxt);

    ExecEndNode((*epqstate).recheckplanstate);

    {
        let mut lc = if !(*estate).es_subplanstates.is_null() {
            list_head((*estate).es_subplanstates)
        } else {
            core::ptr::null_mut()
        };
        while !lc.is_null() {
            let subplanstate: *mut PlanState = (*lc).ptr_value as *mut PlanState;
            ExecEndNode(subplanstate);
            lc = lnext((*estate).es_subplanstates, lc);
        }
    }

    /* throw away the per-estate tuple table, some node may have used it */
    ExecResetTupleTable((*estate).es_tupleTable, false);

    /* Close any result and trigger target relations attached to this EState */
    ExecCloseResultRelations(estate);

    MemoryContextSwitchTo(oldcontext);

    /*
     * NULLify the partition directory before freeing the executor state.
     * Since EvalPlanQualStart() just borrowed the parent EState's directory,
     * we'd better leave it up to the parent to delete it.
     */
    (*estate).es_partition_directory = core::ptr::null_mut();

    FreeExecutorState(estate);

    /* Mark EPQState idle */
    (*epqstate).origslot = core::ptr::null_mut();
    (*epqstate).recheckestate = core::ptr::null_mut();
    (*epqstate).recheckplanstate = core::ptr::null_mut();
    (*epqstate).relsubs_rowmark = core::ptr::null_mut();
    (*epqstate).relsubs_done = core::ptr::null_mut();
    (*epqstate).relsubs_blocked = core::ptr::null_mut();
}

