//! executor/executor.h - support for the POSTGRES executor module.

use std::ffi::{c_char, c_int, c_void};

use crate::c::{int64, uint32, uint64, Index, Size};
use crate::postgres::{Datum, DatumGetBool, DatumGetPointer, PointerGetDatum};
use crate::postgres_ext::Oid;

use crate::access::attnum::AttrNumber;
use crate::access::htup_details::HeapTupleHeader;
use crate::access::sdir::ScanDirection;

use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::lockoptions::LockTupleMode;
use crate::nodes::nodes::{CmdType, Node, NodeTag, OnConflictAction};
use crate::nodes::params::ParamListInfo;
use crate::nodes::parsenodes::{RTEPermissionInfo, RangeTblEntry, WCOKind};
use crate::nodes::pg_list::{list_nth, List};
use crate::nodes::plannodes::Plan;
use crate::nodes::primnodes::{CurrentOfExpr, Expr};

use crate::nodes::execnodes::{
    AggState, AggStatePerPhaseData, EPQState, EState, ExecAuxRowMark, ExecProcNodeMtd, ExecRowMark,
    ExprContext, ExprContextCallbackFunction, ExprDoneCond, ExprState, IndexInfo, JunkFilter,
    ModifyTableState, PlanState, ProjectionInfo, ResultRelInfo, ScanState, SetExprState,
    TupleHashEntry, TupleHashEntryData, TupleHashTable,
};

use crate::access::common::tupconvert::TupleConversionMap;
use crate::access::common::tupdesc::TupleDesc;
use crate::access::htup_details::MinimalTuple;
use crate::executor::execdesc::QueryDesc;
use crate::executor::tuptable::{
    slot_getattr, ExecClearTuple, TupleTableSlot, TupleTableSlotOps, TTS_FLAG_EMPTY,
};
use crate::storage::itemptr::ItemPointer;
use crate::tcop::dest::DestReceiver;
use crate::utils::fmgr::FmgrInfo;
use crate::utils::mmgr::memnodes::MemoryContext;
use crate::utils::palloc::MemoryContextSwitchTo;
use crate::utils::adt::varlena::cstring_to_text;

use crate::utils::rel::Relation;

// Tuplestorestate is an opaque forward-declared type used here only by pointer.
use crate::nodes::execnodes::Tuplestorestate;

// EEO_FLAG_IS_QUAL lives in execnodes (expression eval flags).
use crate::nodes::execnodes::EEO_FLAG_IS_QUAL;

// MemoryContextReset from mcxt (real impl).
use crate::utils::mmgr::mcxt::MemoryContextReset;

// pfree from mcxt.
use crate::utils::mmgr::mcxt::pfree;

/*
 * The "eflags" argument to ExecutorStart and the various ExecInitNode routines
 * is a bitwise OR of the following flag bits.
 */
pub const EXEC_FLAG_EXPLAIN_ONLY: c_int = 0x0001; /* EXPLAIN, no ANALYZE */
pub const EXEC_FLAG_EXPLAIN_GENERIC: c_int = 0x0002; /* EXPLAIN (GENERIC_PLAN) */
pub const EXEC_FLAG_REWIND: c_int = 0x0004; /* need efficient rescan */
pub const EXEC_FLAG_BACKWARD: c_int = 0x0008; /* need backward scan */
pub const EXEC_FLAG_MARK: c_int = 0x0010; /* need mark/restore */
pub const EXEC_FLAG_SKIP_TRIGGERS: c_int = 0x0020; /* skip AfterTrigger setup */
pub const EXEC_FLAG_WITH_NO_DATA: c_int = 0x0040; /* REFRESH ... WITH NO DATA */

/* Hook for plugins to get control in ExecutorStart() */
pub type ExecutorStart_hook_type =
    Option<unsafe fn(queryDesc: *mut QueryDesc, eflags: c_int)>;
pub static mut ExecutorStart_hook: ExecutorStart_hook_type = None;

/* Hook for plugins to get control in ExecutorRun() */
pub type ExecutorRun_hook_type =
    Option<unsafe fn(queryDesc: *mut QueryDesc, direction: ScanDirection, count: uint64)>;
pub static mut ExecutorRun_hook: ExecutorRun_hook_type = None;

/* Hook for plugins to get control in ExecutorFinish() */
pub type ExecutorFinish_hook_type = Option<unsafe fn(queryDesc: *mut QueryDesc)>;
pub static mut ExecutorFinish_hook: ExecutorFinish_hook_type = None;

/* Hook for plugins to get control in ExecutorEnd() */
pub type ExecutorEnd_hook_type = Option<unsafe fn(queryDesc: *mut QueryDesc)>;
pub static mut ExecutorEnd_hook: ExecutorEnd_hook_type = None;

/* Hook for plugins to get control in ExecCheckPermissions() */
pub type ExecutorCheckPerms_hook_type = Option<
    unsafe fn(
        rangeTable: *mut List,
        rtePermInfos: *mut List,
        ereport_on_violation: bool,
    ) -> bool,
>;
pub static mut ExecutorCheckPerms_hook: ExecutorCheckPerms_hook_type = None;

/*
 * prototypes from functions in execAmi.c
 */
// struct Path; - avoid including pathnodes.h here.  Forward-declared, used by
// pointer only in ExecSupportsMarkRestore below.
#[repr(C)]
pub struct Path {
    _private: [u8; 0],
}

pub unsafe fn ExecReScan(_node: *mut PlanState) {
    crate::executor::execAmi::ExecReScan(_node as _)
}
pub unsafe fn ExecMarkPos(_node: *mut PlanState) {
    crate::executor::execAmi::ExecMarkPos(_node as _)
}
pub unsafe fn ExecRestrPos(_node: *mut PlanState) {
    crate::executor::execAmi::ExecRestrPos(_node as _)
}
pub unsafe fn ExecSupportsMarkRestore(_pathnode: *mut Path) -> bool {
    unimplemented!()
}
pub unsafe fn ExecSupportsBackwardScan(_node: *mut Plan) -> bool {
    crate::executor::execAmi::ExecSupportsBackwardScan(_node as _) as _
}
pub unsafe fn ExecMaterializesOutput(_plantype: NodeTag) -> bool {
    unimplemented!()
}

/*
 * prototypes from functions in execCurrent.c
 */
pub unsafe fn execCurrentOf(
    _cexpr: *mut CurrentOfExpr,
    _econtext: *mut ExprContext,
    _table_oid: Oid,
    _current_tid: ItemPointer,
) -> bool {
    crate::executor::execCurrent::execCurrentOf(_cexpr as _, _econtext as _, _table_oid as _, _current_tid as _) as _
}

/*
 * prototypes from functions in execGrouping.c
 */
pub unsafe fn execTuplesMatchPrepare(
    _desc: TupleDesc,
    _numCols: c_int,
    _keyColIdx: *const AttrNumber,
    _eqOperators: *const Oid,
    _collations: *const Oid,
    _parent: *mut PlanState,
) -> *mut ExprState {
    crate::executor::execGrouping::execTuplesMatchPrepare(_desc as _, _numCols as _, _keyColIdx as _, _eqOperators as _, _collations as _, _parent as _) as _
}
pub unsafe fn execTuplesHashPrepare(
    _numCols: c_int,
    _eqOperators: *const Oid,
    _eqFuncOids: *mut *mut Oid,
    _hashFunctions: *mut *mut FmgrInfo,
) {
    crate::executor::execGrouping::execTuplesHashPrepare(_numCols as _, _eqOperators as _, _eqFuncOids as _, _hashFunctions as _)
}
pub unsafe fn BuildTupleHashTable(
    _parent: *mut PlanState,
    _inputDesc: TupleDesc,
    _inputOps: *const TupleTableSlotOps,
    _numCols: c_int,
    _keyColIdx: *mut AttrNumber,
    _eqfuncoids: *const Oid,
    _hashfunctions: *mut FmgrInfo,
    _collations: *mut Oid,
    _nbuckets: std::ffi::c_long,
    _additionalsize: Size,
    _metacxt: MemoryContext,
    _tablecxt: MemoryContext,
    _tempcxt: MemoryContext,
    _use_variable_hash_iv: bool,
) -> TupleHashTable {
    crate::executor::execGrouping::BuildTupleHashTable(_parent as _, _inputDesc as _, _inputOps as _, _numCols as _, _keyColIdx as _, _eqfuncoids as _, _hashfunctions as _, _collations as _, _nbuckets as _, _additionalsize as _, _metacxt as _, _tablecxt as _, _tempcxt as _, _use_variable_hash_iv as _)
}
pub unsafe fn LookupTupleHashEntry(
    _hashtable: TupleHashTable,
    _slot: *mut TupleTableSlot,
    _isnew: *mut bool,
    _hash: *mut uint32,
) -> TupleHashEntry {
    unimplemented!()
}
pub unsafe fn TupleHashTableHash(
    _hashtable: TupleHashTable,
    _slot: *mut TupleTableSlot,
) -> uint32 {
    unimplemented!()
}
pub unsafe fn LookupTupleHashEntryHash(
    _hashtable: TupleHashTable,
    _slot: *mut TupleTableSlot,
    _isnew: *mut bool,
    _hash: uint32,
) -> TupleHashEntry {
    unimplemented!()
}
pub unsafe fn FindTupleHashEntry(
    _hashtable: TupleHashTable,
    _slot: *mut TupleTableSlot,
    _eqcomp: *mut ExprState,
    _hashexpr: *mut ExprState,
) -> TupleHashEntry {
    unimplemented!()
}
pub unsafe fn ResetTupleHashTable(_hashtable: TupleHashTable) {
    unimplemented!()
}

/*
 * Return size of the hash bucket. Useful for estimating memory usage.
 */
#[inline]
pub fn TupleHashEntrySize() -> usize {
    std::mem::size_of::<TupleHashEntryData>()
}

/*
 * Return tuple from hash entry.
 */
#[inline]
pub unsafe fn TupleHashEntryGetTuple(entry: TupleHashEntry) -> MinimalTuple {
    (*entry).firstTuple
}

/*
 * Get a pointer into the additional space allocated for this entry. The memory
 * will be maxaligned and zeroed.
 */
#[inline]
pub unsafe fn TupleHashEntryGetAdditional(
    hashtable: TupleHashTable,
    entry: TupleHashEntry,
) -> *mut c_void {
    if (*hashtable).additionalsize > 0 {
        ((*entry).firstTuple as *mut c_char).offset(-((*hashtable).additionalsize as isize))
            as *mut c_void
    } else {
        std::ptr::null_mut()
    }
}

/*
 * prototypes from functions in execJunk.c
 */
pub unsafe fn ExecInitJunkFilter(
    _targetList: *mut List,
    _slot: *mut TupleTableSlot,
) -> *mut JunkFilter {
    crate::executor::execJunk::ExecInitJunkFilter(_targetList as _, _slot as _) as _
}
pub unsafe fn ExecInitJunkFilterConversion(
    _targetList: *mut List,
    _cleanTupType: TupleDesc,
    _slot: *mut TupleTableSlot,
) -> *mut JunkFilter {
    crate::executor::execJunk::ExecInitJunkFilterConversion(_targetList as _, _cleanTupType as _, _slot as _) as _
}
pub unsafe fn ExecFindJunkAttribute(
    _junkfilter: *mut JunkFilter,
    _attrName: *const c_char,
) -> AttrNumber {
    crate::executor::execJunk::ExecFindJunkAttribute(_junkfilter as _, _attrName as _) as _
}
pub unsafe fn ExecFindJunkAttributeInTlist(
    _targetlist: *mut List,
    _attrName: *const c_char,
) -> AttrNumber {
    crate::executor::execJunk::ExecFindJunkAttributeInTlist(_targetlist as _, _attrName as _) as _
}
pub unsafe fn ExecFilterJunk(
    _junkfilter: *mut JunkFilter,
    _slot: *mut TupleTableSlot,
) -> *mut TupleTableSlot {
    crate::executor::execJunk::ExecFilterJunk(_junkfilter as _, _slot as _) as _
}

/*
 * ExecGetJunkAttribute
 *
 * Given a junk filter's input tuple (slot) and a junk attribute's number
 * previously found by ExecFindJunkAttribute, extract & return the value and
 * isNull flag of the attribute.
 */
#[inline]
pub unsafe fn ExecGetJunkAttribute(
    slot: *mut TupleTableSlot,
    attno: AttrNumber,
    isNull: *mut bool,
) -> Datum {
    debug_assert!(attno > 0);
    slot_getattr(slot, attno as c_int, isNull)
}

/*
 * prototypes from functions in execMain.c
 */
pub unsafe fn ExecutorStart(_queryDesc: *mut QueryDesc, _eflags: c_int) {
    crate::executor::execMain::ExecutorStart(_queryDesc as _, _eflags as _)
}
pub unsafe fn standard_ExecutorStart(_queryDesc: *mut QueryDesc, _eflags: c_int) {
    crate::executor::execMain::standard_ExecutorStart(_queryDesc as _, _eflags as _)
}
pub unsafe fn ExecutorRun(
    queryDesc: *mut QueryDesc,
    direction: ScanDirection,
    count: uint64,
) {
    crate::executor::execMain::ExecutorRun(queryDesc as _, direction as _, count as _)
}
pub unsafe fn standard_ExecutorRun(
    queryDesc: *mut QueryDesc,
    direction: ScanDirection,
    count: uint64,
) {
    crate::executor::execMain::standard_ExecutorRun(queryDesc as _, direction as _, count as _)
}
pub unsafe fn ExecutorFinish(_queryDesc: *mut QueryDesc) {
    crate::executor::execMain::ExecutorFinish(_queryDesc as _)
}
pub unsafe fn standard_ExecutorFinish(_queryDesc: *mut QueryDesc) {
    crate::executor::execMain::standard_ExecutorFinish(_queryDesc as _)
}
pub unsafe fn ExecutorEnd(_queryDesc: *mut QueryDesc) {
    crate::executor::execMain::ExecutorEnd(_queryDesc as _)
}
pub unsafe fn standard_ExecutorEnd(_queryDesc: *mut QueryDesc) {
    crate::executor::execMain::standard_ExecutorEnd(_queryDesc as _)
}
pub unsafe fn ExecutorRewind(_queryDesc: *mut QueryDesc) {
    crate::executor::execMain::ExecutorRewind(_queryDesc as _)
}
pub unsafe fn ExecCheckPermissions(
    _rangeTable: *mut List,
    _rteperminfos: *mut List,
    _ereport_on_violation: bool,
) -> bool {
    crate::executor::execMain::ExecCheckPermissions(_rangeTable as _, _rteperminfos as _, _ereport_on_violation as _) as _
}
pub unsafe fn ExecCheckOneRelPerms(_perminfo: *mut RTEPermissionInfo) -> bool {
    crate::executor::execMain::ExecCheckOneRelPerms(_perminfo as _) as _
}
pub unsafe fn CheckValidResultRel(
    resultRelInfo: *mut ResultRelInfo,
    operation: CmdType,
    onConflictAction: OnConflictAction,
    mergeActions: *mut List,
) {
    crate::executor::execMain::CheckValidResultRel(resultRelInfo as _, operation as _, onConflictAction as _, mergeActions as _)
}
#[no_mangle]
pub unsafe fn InitResultRelInfo(
    _resultRelInfo: *mut ResultRelInfo,
    _resultRelationDesc: Relation,
    _resultRelationIndex: Index,
    _partition_root_rri: *mut ResultRelInfo,
    _instrument_options: c_int,
) {
    crate::executor::execMain::InitResultRelInfo(_resultRelInfo as _, _resultRelationDesc as _, _resultRelationIndex as _, _partition_root_rri as _, _instrument_options as _)
}
pub unsafe fn ExecGetTriggerResultRel(
    _estate: *mut EState,
    _relid: Oid,
    _rootRelInfo: *mut ResultRelInfo,
) -> *mut ResultRelInfo {
    crate::executor::execMain::ExecGetTriggerResultRel(_estate as _, _relid as _, _rootRelInfo as _) as _
}
pub unsafe fn ExecGetAncestorResultRels(
    _estate: *mut EState,
    _resultRelInfo: *mut ResultRelInfo,
) -> *mut List {
    crate::executor::execMain::ExecGetAncestorResultRels(_estate as _, _resultRelInfo as _) as _
}
pub unsafe fn ExecConstraints(
    _resultRelInfo: *mut ResultRelInfo,
    _slot: *mut TupleTableSlot,
    _estate: *mut EState,
) {
    crate::executor::execMain::ExecConstraints(_resultRelInfo as _, _slot as _, _estate as _)
}
pub unsafe fn ExecRelGenVirtualNotNull(
    _resultRelInfo: *mut ResultRelInfo,
    _slot: *mut TupleTableSlot,
    _estate: *mut EState,
    _notnull_virtual_attrs: *mut List,
) -> AttrNumber {
    crate::executor::execMain::ExecRelGenVirtualNotNull(_resultRelInfo as _, _slot as _, _estate as _, _notnull_virtual_attrs as _) as _
}
#[no_mangle]
pub unsafe fn ExecPartitionCheck(
    _resultRelInfo: *mut ResultRelInfo,
    _slot: *mut TupleTableSlot,
    _estate: *mut EState,
    _emitError: bool,
) -> bool {
    crate::executor::execMain::ExecPartitionCheck(_resultRelInfo as _, _slot as _, _estate as _, _emitError as _) as _
}
pub unsafe fn ExecPartitionCheckEmitError(
    _resultRelInfo: *mut ResultRelInfo,
    _slot: *mut TupleTableSlot,
    _estate: *mut EState,
) {
    crate::executor::execMain::ExecPartitionCheckEmitError(_resultRelInfo as _, _slot as _, _estate as _)
}
pub unsafe fn ExecWithCheckOptions(
    _kind: WCOKind,
    _resultRelInfo: *mut ResultRelInfo,
    _slot: *mut TupleTableSlot,
    _estate: *mut EState,
) {
    unimplemented!()
}
pub unsafe fn ExecBuildSlotValueDescription(
    _reloid: Oid,
    _slot: *mut TupleTableSlot,
    _tupdesc: TupleDesc,
    _modifiedCols: *mut Bitmapset,
    _maxfieldlen: c_int,
) -> *mut c_char {
    crate::executor::execMain::ExecBuildSlotValueDescription(_reloid as _, _slot as _, _tupdesc as _, _modifiedCols as _, _maxfieldlen as _) as _
}
pub unsafe fn ExecUpdateLockMode(
    _estate: *mut EState,
    _relinfo: *mut ResultRelInfo,
) -> LockTupleMode {
    unimplemented!()
}
pub unsafe fn ExecFindRowMark(
    _estate: *mut EState,
    _rti: Index,
    _missing_ok: bool,
) -> *mut ExecRowMark {
    crate::executor::execMain::ExecFindRowMark(_estate as _, _rti as _, _missing_ok as _) as _
}
pub unsafe fn ExecBuildAuxRowMark(
    _erm: *mut ExecRowMark,
    _targetlist: *mut List,
) -> *mut ExecAuxRowMark {
    crate::executor::execMain::ExecBuildAuxRowMark(_erm as _, _targetlist as _) as _
}
pub unsafe fn EvalPlanQual(
    _epqstate: *mut EPQState,
    _relation: Relation,
    _rti: Index,
    _inputslot: *mut TupleTableSlot,
) -> *mut TupleTableSlot {
    crate::executor::execMain::EvalPlanQual(_epqstate as _, _relation as _, _rti as _, _inputslot as _) as _
}
#[no_mangle]
pub unsafe fn EvalPlanQualInit(
    _epqstate: *mut EPQState,
    _parentestate: *mut EState,
    _subplan: *mut Plan,
    _auxrowmarks: *mut List,
    _epqParam: c_int,
    _resultRelations: *mut List,
) {
    crate::executor::execMain::EvalPlanQualInit(_epqstate as _, _parentestate as _, _subplan as _, _auxrowmarks as _, _epqParam as _, _resultRelations as _)
}
pub unsafe fn EvalPlanQualSetPlan(
    _epqstate: *mut EPQState,
    _subplan: *mut Plan,
    _auxrowmarks: *mut List,
) {
    crate::executor::execMain::EvalPlanQualSetPlan(_epqstate as _, _subplan as _, _auxrowmarks as _)
}
pub unsafe fn EvalPlanQualSlot(
    _epqstate: *mut EPQState,
    _relation: Relation,
    _rti: Index,
) -> *mut TupleTableSlot {
    crate::executor::execMain::EvalPlanQualSlot(_epqstate as _, _relation as _, _rti as _) as _
}

/* #define EvalPlanQualSetSlot(epqstate, slot) ((epqstate)->origslot = (slot)) */
#[inline]
#[no_mangle]
pub unsafe fn EvalPlanQualSetSlot(epqstate: *mut EPQState, slot: *mut TupleTableSlot) {
    (*epqstate).origslot = slot;
}

pub unsafe fn EvalPlanQualFetchRowMark(
    _epqstate: *mut EPQState,
    _rti: Index,
    _slot: *mut TupleTableSlot,
) -> bool {
    crate::executor::execMain::EvalPlanQualFetchRowMark(_epqstate as _, _rti as _, _slot as _) as _
}
pub unsafe fn EvalPlanQualNext(_epqstate: *mut EPQState) -> *mut TupleTableSlot {
    crate::executor::execMain::EvalPlanQualNext(_epqstate as _) as _
}
pub unsafe fn EvalPlanQualBegin(_epqstate: *mut EPQState) {
    crate::executor::execMain::EvalPlanQualBegin(_epqstate as _)
}
#[no_mangle]
pub unsafe fn EvalPlanQualEnd(_epqstate: *mut EPQState) {
    crate::executor::execMain::EvalPlanQualEnd(_epqstate as _)
}

/*
 * functions in execProcnode.c
 */
pub unsafe fn ExecInitNode(
    _node: *mut Plan,
    _estate: *mut EState,
    _eflags: c_int,
) -> *mut PlanState {
    crate::executor::execProcnode::ExecInitNode(_node as _, _estate as _, _eflags as _) as _
}
pub unsafe fn ExecSetExecProcNode(_node: *mut PlanState, _function: ExecProcNodeMtd) {
    unimplemented!()
}
pub unsafe fn MultiExecProcNode(_node: *mut PlanState) -> *mut Node {
    crate::executor::execProcnode::MultiExecProcNode(_node as _) as _
}
pub unsafe fn ExecEndNode(_node: *mut PlanState) {
    crate::executor::execProcnode::ExecEndNode(_node as _)
}
pub unsafe fn ExecShutdownNode(_node: *mut PlanState) {
    crate::executor::execProcnode::ExecShutdownNode(_node as _)
}
pub unsafe fn ExecSetTupleBound(_tuples_needed: int64, _child_node: *mut PlanState) {
    crate::executor::execProcnode::ExecSetTupleBound(_tuples_needed as _, _child_node as _)
}

/* ----------------------------------------------------------------
 *		ExecProcNode
 *
 *		Execute the given node to return a(nother) tuple.
 * ----------------------------------------------------------------
 */
#[inline]
pub unsafe fn ExecProcNode(node: *mut PlanState) -> *mut TupleTableSlot {
    if !(*node).chgParam.is_null() {
        /* something changed? */
        ExecReScan(node); /* let ReScan handle this */
    }

    ((*node).ExecProcNode.expect("ExecProcNode method not set"))(node)
}

/*
 * prototypes from functions in execExpr.c
 */
#[no_mangle]
pub unsafe fn ExecInitExpr(_node: *mut Expr, _parent: *mut PlanState) -> *mut ExprState {
    crate::executor::execExpr::ExecInitExpr(_node as _, _parent as _) as _
}
pub unsafe fn ExecInitExprWithParams(
    _node: *mut Expr,
    _ext_params: ParamListInfo,
) -> *mut ExprState {
    crate::executor::execExpr::ExecInitExprWithParams(_node as _, _ext_params as _) as _
}
pub unsafe fn ExecInitQual(_qual: *mut List, _parent: *mut PlanState) -> *mut ExprState {
    crate::executor::execExpr::ExecInitQual(_qual as _, _parent as _) as _
}
pub unsafe fn ExecInitCheck(_qual: *mut List, _parent: *mut PlanState) -> *mut ExprState {
    crate::executor::execExpr::ExecInitCheck(_qual as _, _parent as _) as _
}
pub unsafe fn ExecInitExprList(_nodes: *mut List, _parent: *mut PlanState) -> *mut List {
    crate::executor::execExpr::ExecInitExprList(_nodes as _, _parent as _) as _
}
pub unsafe fn ExecBuildAggTrans(
    _aggstate: *mut AggState,
    _phase: *mut AggStatePerPhaseData,
    _doSort: bool,
    _doHash: bool,
    _nullcheck: bool,
) -> *mut ExprState {
    crate::executor::execExpr::ExecBuildAggTrans(_aggstate as _, _phase as _, _doSort as _, _doHash as _, _nullcheck as _) as _
}
pub unsafe fn ExecBuildHash32FromAttrs(
    _desc: TupleDesc,
    _ops: *const TupleTableSlotOps,
    _hashfunctions: *mut FmgrInfo,
    _collations: *mut Oid,
    _numCols: c_int,
    _keyColIdx: *mut AttrNumber,
    _parent: *mut PlanState,
    _init_value: uint32,
) -> *mut ExprState {
    crate::executor::execExpr::ExecBuildHash32FromAttrs(_desc as _, _ops as _, _hashfunctions as _, _collations as _, _numCols as _, _keyColIdx as _, _parent as _, _init_value as _) as _
}
pub unsafe fn ExecBuildHash32Expr(
    _desc: TupleDesc,
    _ops: *const TupleTableSlotOps,
    _hashfunc_oids: *const Oid,
    _collations: *const List,
    _hash_exprs: *const List,
    _opstrict: *const bool,
    _parent: *mut PlanState,
    _init_value: uint32,
    _keep_nulls: bool,
) -> *mut ExprState {
    crate::executor::execExpr::ExecBuildHash32Expr(_desc as _, _ops as _, _hashfunc_oids as _, _collations as _, _hash_exprs as _, _opstrict as _, _parent as _, _init_value as _, _keep_nulls as _) as _
}
pub unsafe fn ExecBuildGroupingEqual(
    _ldesc: TupleDesc,
    _rdesc: TupleDesc,
    _lops: *const TupleTableSlotOps,
    _rops: *const TupleTableSlotOps,
    _numCols: c_int,
    _keyColIdx: *const AttrNumber,
    _eqfunctions: *const Oid,
    _collations: *const Oid,
    _parent: *mut PlanState,
) -> *mut ExprState {
    crate::executor::execExpr::ExecBuildGroupingEqual(_ldesc as _, _rdesc as _, _lops as _, _rops as _, _numCols as _, _keyColIdx as _, _eqfunctions as _, _collations as _, _parent as _) as _
}
pub unsafe fn ExecBuildParamSetEqual(
    _desc: TupleDesc,
    _lops: *const TupleTableSlotOps,
    _rops: *const TupleTableSlotOps,
    _eqfunctions: *const Oid,
    _collations: *const Oid,
    _param_exprs: *const List,
    _parent: *mut PlanState,
) -> *mut ExprState {
    crate::executor::execExpr::ExecBuildParamSetEqual(_desc as _, _lops as _, _rops as _, _eqfunctions as _, _collations as _, _param_exprs as _, _parent as _) as _
}
pub unsafe fn ExecBuildProjectionInfo(
    _targetList: *mut List,
    _econtext: *mut ExprContext,
    _slot: *mut TupleTableSlot,
    _parent: *mut PlanState,
    _inputDesc: TupleDesc,
) -> *mut ProjectionInfo {
    crate::executor::execExpr::ExecBuildProjectionInfo(_targetList as _, _econtext as _, _slot as _, _parent as _, _inputDesc as _) as _
}
pub unsafe fn ExecBuildUpdateProjection(
    _targetList: *mut List,
    _evalTargetList: bool,
    _targetColnos: *mut List,
    _relDesc: TupleDesc,
    _econtext: *mut ExprContext,
    _slot: *mut TupleTableSlot,
    _parent: *mut PlanState,
) -> *mut ProjectionInfo {
    crate::executor::execExpr::ExecBuildUpdateProjection(_targetList as _, _evalTargetList as _, _targetColnos as _, _relDesc as _, _econtext as _, _slot as _, _parent as _) as _
}
pub unsafe fn ExecPrepareExpr(_node: *mut Expr, _estate: *mut EState) -> *mut ExprState {
    crate::executor::execExpr::ExecPrepareExpr(_node as _, _estate as _) as _
}
pub unsafe fn ExecPrepareQual(_qual: *mut List, _estate: *mut EState) -> *mut ExprState {
    crate::executor::execExpr::ExecPrepareQual(_qual as _, _estate as _) as _
}
pub unsafe fn ExecPrepareCheck(_qual: *mut List, _estate: *mut EState) -> *mut ExprState {
    crate::executor::execExpr::ExecPrepareCheck(_qual as _, _estate as _) as _
}
pub unsafe fn ExecPrepareExprList(_nodes: *mut List, _estate: *mut EState) -> *mut List {
    crate::executor::execExpr::ExecPrepareExprList(_nodes as _, _estate as _) as _
}

/*
 * ExecEvalExpr
 *
 * Evaluate expression identified by "state" in the execution context given by
 * "econtext".
 */
#[inline]
#[no_mangle]
pub unsafe fn ExecEvalExpr(
    state: *mut ExprState,
    econtext: *mut ExprContext,
    isNull: *mut bool,
) -> Datum {
    ((*state).evalfunc.expect("evalfunc not set"))(state, econtext, isNull)
}

/*
 * ExecEvalExprNoReturn
 *
 * Like ExecEvalExpr(), but for cases where no return value is expected.
 */
#[inline]
pub unsafe fn ExecEvalExprNoReturn(state: *mut ExprState, econtext: *mut ExprContext) {
    let retDatum: Datum = ((*state).evalfunc.expect("evalfunc not set"))(
        state,
        econtext,
        std::ptr::null_mut(),
    );

    debug_assert!(retDatum == 0 as Datum);
    let _ = retDatum;
}

/*
 * ExecEvalExprSwitchContext
 *
 * Same as ExecEvalExpr, but get into the right allocation context explicitly.
 */
#[inline]
pub unsafe fn ExecEvalExprSwitchContext(
    state: *mut ExprState,
    econtext: *mut ExprContext,
    isNull: *mut bool,
) -> Datum {
    // palloc's MemoryContextSwitchTo uses a distinct stub MemoryContextData from
    // memnodes' (the ExprContext field type); cast the opaque pointers across.
    // TODO(pg-port): drops out once task #8 unifies palloc/memnodes MemoryContext.
    let oldContext = MemoryContextSwitchTo((*econtext).ecxt_per_tuple_memory as *mut _);
    let retDatum: Datum = ((*state).evalfunc.expect("evalfunc not set"))(state, econtext, isNull);
    MemoryContextSwitchTo(oldContext as *mut _);
    retDatum
}

/*
 * ExecEvalExprNoReturnSwitchContext
 *
 * Same as ExecEvalExprNoReturn, but get into the right allocation context.
 */
#[inline]
pub unsafe fn ExecEvalExprNoReturnSwitchContext(
    state: *mut ExprState,
    econtext: *mut ExprContext,
) {
    // palloc's MemoryContextSwitchTo uses a distinct stub MemoryContextData from
    // memnodes' (the ExprContext field type); cast the opaque pointers across.
    // TODO(pg-port): drops out once task #8 unifies palloc/memnodes MemoryContext.
    let oldContext = MemoryContextSwitchTo((*econtext).ecxt_per_tuple_memory as *mut _);
    ExecEvalExprNoReturn(state, econtext);
    MemoryContextSwitchTo(oldContext as *mut _);
}

/*
 * ExecProject
 *
 * Projects a tuple based on projection info and stores it in the slot passed
 * to ExecBuildProjectionInfo().
 */
#[inline]
pub unsafe fn ExecProject(projInfo: *mut ProjectionInfo) -> *mut TupleTableSlot {
    let econtext: *mut ExprContext = (*projInfo).pi_exprContext;
    let state: *mut ExprState = &mut (*projInfo).pi_state;
    let slot: *mut TupleTableSlot = (*state).resultslot;

    /*
     * Clear any former contents of the result slot.  This makes it safe for us
     * to use the slot's Datum/isnull arrays as workspace.
     */
    ExecClearTuple(slot);

    /* Run the expression */
    ExecEvalExprNoReturnSwitchContext(state, econtext);

    /*
     * Successfully formed a result row.  Mark the result slot as containing a
     * valid virtual tuple (inlined version of ExecStoreVirtualTuple()).
     */
    (*slot).tts_flags &= !TTS_FLAG_EMPTY;
    (*slot).tts_nvalid = (*(*slot).tts_tupleDescriptor).natts as AttrNumber;

    slot
}

/*
 * ExecQual - evaluate a qual prepared with ExecInitQual (possibly via
 * ExecPrepareQual).  Returns true if qual is satisfied, else false.
 */
#[inline]
pub unsafe fn ExecQual(state: *mut ExprState, econtext: *mut ExprContext) -> bool {
    /* short-circuit (here and in ExecInitQual) for empty restriction list */
    if state.is_null() {
        return true;
    }

    /* verify that expression was compiled using ExecInitQual */
    debug_assert!((*state).flags & EEO_FLAG_IS_QUAL != 0);

    let mut isnull: bool = false;
    let ret: Datum = ExecEvalExprSwitchContext(state, econtext, &mut isnull);

    /* EEOP_QUAL should never return NULL */
    debug_assert!(!isnull);

    DatumGetBool(ret)
}

/*
 * ExecQualAndReset() - evaluate qual with ExecQual() and reset expression
 * context.
 */
#[inline]
pub unsafe fn ExecQualAndReset(state: *mut ExprState, econtext: *mut ExprContext) -> bool {
    let ret: bool = ExecQual(state, econtext);

    /* inline ResetExprContext, to avoid ordering issue in this file */
    MemoryContextReset((*econtext).ecxt_per_tuple_memory as *mut _);
    ret
}

pub unsafe fn ExecCheck(_state: *mut ExprState, _econtext: *mut ExprContext) -> bool {
    crate::executor::execExpr::ExecCheck(_state as _, _econtext as _) as _
}

/*
 * prototypes from functions in execSRF.c
 */
pub unsafe fn ExecInitTableFunctionResult(
    _expr: *mut Expr,
    _econtext: *mut ExprContext,
    _parent: *mut PlanState,
) -> *mut SetExprState {
    crate::executor::execSRF::ExecInitTableFunctionResult(_expr as _, _econtext as _, _parent as _) as _
}
pub unsafe fn ExecMakeTableFunctionResult(
    _setexpr: *mut SetExprState,
    _econtext: *mut ExprContext,
    _argContext: MemoryContext,
    _expectedDesc: TupleDesc,
    _randomAccess: bool,
) -> *mut Tuplestorestate {
    crate::executor::execSRF::ExecMakeTableFunctionResult(_setexpr as _, _econtext as _, _argContext as _, _expectedDesc as _, _randomAccess as _) as _
}
pub unsafe fn ExecInitFunctionResultSet(
    _expr: *mut Expr,
    _econtext: *mut ExprContext,
    _parent: *mut PlanState,
) -> *mut SetExprState {
    crate::executor::execSRF::ExecInitFunctionResultSet(_expr as _, _econtext as _, _parent as _) as _
}
pub unsafe fn ExecMakeFunctionResultSet(
    _fcache: *mut SetExprState,
    _econtext: *mut ExprContext,
    _argContext: MemoryContext,
    _isNull: *mut bool,
    _isDone: *mut ExprDoneCond,
) -> Datum {
    crate::executor::execSRF::ExecMakeFunctionResultSet(_fcache as _, _econtext as _, _argContext as _, _isNull as _, _isDone as _) as _
}

/*
 * prototypes from functions in execScan.c
 */
pub type ExecScanAccessMtd =
    Option<unsafe fn(node: *mut ScanState) -> *mut TupleTableSlot>;
pub type ExecScanRecheckMtd =
    Option<unsafe fn(node: *mut ScanState, slot: *mut TupleTableSlot) -> bool>;

pub unsafe fn ExecScan(
    node: *mut ScanState,
    accessMtd: ExecScanAccessMtd,
    recheckMtd: ExecScanRecheckMtd,
) -> *mut TupleTableSlot {
    crate::executor::execScan::ExecScan(
        node as _,
        core::mem::transmute(accessMtd),
        core::mem::transmute(recheckMtd),
    )
}
pub unsafe fn ExecAssignScanProjectionInfo(_node: *mut ScanState) {
    crate::executor::execScan::ExecAssignScanProjectionInfo(_node as _)
}
pub unsafe fn ExecAssignScanProjectionInfoWithVarno(_node: *mut ScanState, _varno: c_int) {
    crate::executor::execScan::ExecAssignScanProjectionInfoWithVarno(_node as _, _varno as _)
}
pub unsafe fn ExecScanReScan(_node: *mut ScanState) {
    crate::executor::execScan::ExecScanReScan(_node as _)
}

/*
 * prototypes from functions in execTuples.c
 */
pub unsafe fn ExecInitResultTypeTL(_planstate: *mut PlanState) {
    crate::executor::execTuples::ExecInitResultTypeTL(_planstate as _)
}
pub unsafe fn ExecInitResultSlot(
    _planstate: *mut PlanState,
    _tts_ops: *const TupleTableSlotOps,
) {
    crate::executor::execTuples::ExecInitResultSlot(_planstate as _, _tts_ops as _)
}
pub unsafe fn ExecInitResultTupleSlotTL(
    _planstate: *mut PlanState,
    _tts_ops: *const TupleTableSlotOps,
) {
    crate::executor::execTuples::ExecInitResultTupleSlotTL(_planstate as _, _tts_ops as _)
}
pub unsafe fn ExecInitScanTupleSlot(
    _estate: *mut EState,
    _scanstate: *mut ScanState,
    _tupledesc: TupleDesc,
    _tts_ops: *const TupleTableSlotOps,
) {
    crate::executor::execTuples::ExecInitScanTupleSlot(_estate as _, _scanstate as _, _tupledesc as _, _tts_ops as _)
}
#[no_mangle]
pub unsafe fn ExecInitExtraTupleSlot(
    _estate: *mut EState,
    _tupledesc: TupleDesc,
    _tts_ops: *const TupleTableSlotOps,
) -> *mut TupleTableSlot {
    crate::executor::execTuples::ExecInitExtraTupleSlot(_estate as _, _tupledesc as _, _tts_ops as _) as _
}
pub unsafe fn ExecInitNullTupleSlot(
    _estate: *mut EState,
    _tupType: TupleDesc,
    _tts_ops: *const TupleTableSlotOps,
) -> *mut TupleTableSlot {
    crate::executor::execTuples::ExecInitNullTupleSlot(_estate as _, _tupType as _, _tts_ops as _) as _
}
pub unsafe fn ExecTypeFromTL(_targetList: *mut List) -> TupleDesc {
    crate::executor::execTuples::ExecTypeFromTL(_targetList as _) as _
}
pub unsafe fn ExecCleanTypeFromTL(_targetList: *mut List) -> TupleDesc {
    crate::executor::execTuples::ExecCleanTypeFromTL(_targetList as _) as _
}
pub unsafe fn ExecTypeFromExprList(_exprList: *mut List) -> TupleDesc {
    crate::executor::execTuples::ExecTypeFromExprList(_exprList as _) as _
}
pub unsafe fn ExecTypeSetColNames(_typeInfo: TupleDesc, _namesList: *mut List) {
    crate::executor::execTuples::ExecTypeSetColNames(_typeInfo as _, _namesList as _)
}
pub unsafe fn UpdateChangedParamSet(_node: *mut PlanState, _newchg: *mut Bitmapset) {
    unimplemented!()
}

#[repr(C)]
pub struct TupOutputState {
    pub slot: *mut TupleTableSlot,
    pub dest: *mut DestReceiver,
}

pub unsafe fn begin_tup_output_tupdesc(
    _dest: *mut DestReceiver,
    _tupdesc: TupleDesc,
    _tts_ops: *const TupleTableSlotOps,
) -> *mut TupOutputState {
    crate::executor::execTuples::begin_tup_output_tupdesc(_dest as _, _tupdesc as _, _tts_ops as _) as _
}
pub unsafe fn do_tup_output(
    _tstate: *mut TupOutputState,
    _values: *const Datum,
    _isnull: *const bool,
) {
    crate::executor::execTuples::do_tup_output(_tstate as _, _values as _, _isnull as _)
}
pub unsafe fn do_text_output_multiline(_tstate: *mut TupOutputState, _txt: *const c_char) {
    crate::executor::execTuples::do_text_output_multiline(_tstate as _, _txt as _)
}
pub unsafe fn end_tup_output(_tstate: *mut TupOutputState) {
    crate::executor::execTuples::end_tup_output(_tstate as _)
}

/*
 * Write a single line of text given as a C string.
 *
 * Should only be used with a single-TEXT-attribute tupdesc.
 */
#[inline]
pub unsafe fn do_text_output_oneline(tstate: *mut TupOutputState, str_to_emit: *const c_char) {
    let mut values_: [Datum; 1] = [0 as Datum; 1];
    let mut isnull_: [bool; 1] = [false; 1];
    values_[0] = PointerGetDatum(cstring_to_text(str_to_emit) as *const c_void);
    isnull_[0] = false;
    do_tup_output(tstate, values_.as_ptr(), isnull_.as_ptr());
    pfree(DatumGetPointer(values_[0]) as *mut c_void);
}

/*
 * prototypes from functions in execUtils.c
 */
pub unsafe fn CreateExecutorState() -> *mut EState {
    crate::executor::execUtils::CreateExecutorState() as _
}
pub unsafe fn FreeExecutorState(_estate: *mut EState) {
    crate::executor::execUtils::FreeExecutorState(_estate as _)
}
pub unsafe fn CreateExprContext(_estate: *mut EState) -> *mut ExprContext {
    crate::executor::execUtils::CreateExprContext(_estate as _) as _
}
pub unsafe fn CreateWorkExprContext(_estate: *mut EState) -> *mut ExprContext {
    crate::executor::execUtils::CreateWorkExprContext(_estate as _) as _
}
pub unsafe fn CreateStandaloneExprContext() -> *mut ExprContext {
    crate::executor::execUtils::CreateStandaloneExprContext() as _
}
pub unsafe fn FreeExprContext(_econtext: *mut ExprContext, _isCommit: bool) {
    crate::executor::execUtils::FreeExprContext(_econtext as _, _isCommit as _)
}
pub unsafe fn ReScanExprContext(_econtext: *mut ExprContext) {
    crate::executor::execUtils::ReScanExprContext(_econtext as _)
}

/* #define ResetExprContext(econtext)
 *     MemoryContextReset((econtext)->ecxt_per_tuple_memory) */
#[inline]
pub unsafe fn ResetExprContext(econtext: *mut ExprContext) {
    MemoryContextReset((*econtext).ecxt_per_tuple_memory as *mut _);
}

pub unsafe fn MakePerTupleExprContext(_estate: *mut EState) -> *mut ExprContext {
    crate::executor::execUtils::MakePerTupleExprContext(_estate as _) as _
}

/* Get an EState's per-output-tuple exprcontext, making it if first use */
#[inline]
pub unsafe fn GetPerTupleExprContext(estate: *mut EState) -> *mut ExprContext {
    if !(*estate).es_per_tuple_exprcontext.is_null() {
        (*estate).es_per_tuple_exprcontext
    } else {
        MakePerTupleExprContext(estate)
    }
}

#[inline]
pub unsafe fn GetPerTupleMemoryContext(estate: *mut EState) -> MemoryContext {
    (*GetPerTupleExprContext(estate)).ecxt_per_tuple_memory as *mut _
}

/* Reset an EState's per-output-tuple exprcontext, if one's been created */
#[inline]
pub unsafe fn ResetPerTupleExprContext(estate: *mut EState) {
    if !(*estate).es_per_tuple_exprcontext.is_null() {
        ResetExprContext((*estate).es_per_tuple_exprcontext);
    }
}

pub unsafe fn ExecAssignExprContext(estate: *mut EState, planstate: *mut PlanState) {
    crate::executor::execUtils::ExecAssignExprContext(estate, planstate)
}
pub unsafe fn ExecGetResultType(planstate: *mut PlanState) -> TupleDesc {
    crate::executor::execUtils::ExecGetResultType(planstate)
}
pub unsafe fn ExecGetResultSlotOps(
    planstate: *mut PlanState,
    isfixed: *mut bool,
) -> *const TupleTableSlotOps {
    crate::executor::execUtils::ExecGetResultSlotOps(planstate as _, isfixed) as _
}
pub unsafe fn ExecGetCommonSlotOps(
    planstates: *mut *mut PlanState,
    nplans: c_int,
) -> *const TupleTableSlotOps {
    crate::executor::execUtils::ExecGetCommonSlotOps(planstates as _, nplans) as _
}
pub unsafe fn ExecGetCommonChildSlotOps(ps: *mut PlanState) -> *const TupleTableSlotOps {
    crate::executor::execUtils::ExecGetCommonChildSlotOps(ps as _) as _
}
pub unsafe fn ExecAssignProjectionInfo(planstate: *mut PlanState, inputDesc: TupleDesc) {
    crate::executor::execUtils::ExecAssignProjectionInfo(planstate, inputDesc)
}
pub unsafe fn ExecConditionalAssignProjectionInfo(
    planstate: *mut PlanState,
    inputDesc: TupleDesc,
    varno: c_int,
) {
    crate::executor::execUtils::ExecConditionalAssignProjectionInfo(planstate, inputDesc, varno)
}
pub unsafe fn ExecAssignScanType(scanstate: *mut ScanState, tupDesc: TupleDesc) {
    crate::executor::execUtils::ExecAssignScanType(scanstate as _, tupDesc)
}
pub unsafe fn ExecCreateScanSlotFromOuterPlan(
    estate: *mut EState,
    scanstate: *mut ScanState,
    tts_ops: *const TupleTableSlotOps,
) {
    crate::executor::execUtils::ExecCreateScanSlotFromOuterPlan(estate as _, scanstate as _, tts_ops as _)
}

pub unsafe fn ExecRelationIsTargetRelation(_estate: *mut EState, _scanrelid: Index) -> bool {
    crate::executor::execUtils::ExecRelationIsTargetRelation(_estate as _, _scanrelid as _)
}

pub unsafe fn ExecOpenScanRelation(
    _estate: *mut EState,
    _scanrelid: Index,
    _eflags: c_int,
) -> Relation {
    crate::executor::execUtils::ExecOpenScanRelation(_estate as _, _scanrelid as _, _eflags as _)
        as _
}

pub unsafe fn ExecInitRangeTable(
    _estate: *mut EState,
    _rangeTable: *mut List,
    _permInfos: *mut List,
    _unpruned_relids: *mut Bitmapset,
) {
    crate::executor::execUtils::ExecInitRangeTable(
        _estate as _,
        _rangeTable as _,
        _permInfos as _,
        _unpruned_relids as _,
    )
}
pub unsafe fn ExecCloseRangeTableRelations(_estate: *mut EState) {
    crate::executor::execMain::ExecCloseRangeTableRelations(_estate as _)
}
pub unsafe fn ExecCloseResultRelations(_estate: *mut EState) {
    crate::executor::execMain::ExecCloseResultRelations(_estate as _)
}

#[inline]
pub unsafe fn exec_rt_fetch(rti: Index, estate: *mut EState) -> *mut RangeTblEntry {
    list_nth((*estate).es_range_table, (rti as c_int) - 1) as *mut RangeTblEntry
}

pub unsafe fn ExecGetRangeTableRelation(
    _estate: *mut EState,
    _rti: Index,
    _isResultRel: bool,
) -> Relation {
    crate::executor::execUtils::ExecGetRangeTableRelation(_estate as _, _rti as _, _isResultRel)
        as _
}
pub unsafe fn ExecInitResultRelation(
    _estate: *mut EState,
    _resultRelInfo: *mut ResultRelInfo,
    _rti: Index,
) {
    crate::executor::execUtils::ExecInitResultRelation(
        _estate as _,
        _resultRelInfo as _,
        _rti as _,
    )
}

pub unsafe fn executor_errposition(_estate: *mut EState, _location: c_int) -> c_int {
    crate::executor::execUtils::executor_errposition(_estate as _, _location as _) as _
}

pub unsafe fn RegisterExprContextCallback(
    _econtext: *mut ExprContext,
    _function: ExprContextCallbackFunction,
    _arg: Datum,
) {
    unimplemented!()
}
pub unsafe fn UnregisterExprContextCallback(
    _econtext: *mut ExprContext,
    _function: ExprContextCallbackFunction,
    _arg: Datum,
) {
    unimplemented!()
}

#[no_mangle]
pub unsafe fn GetAttributeByName(
    _tuple: HeapTupleHeader,
    _attname: *const c_char,
    _isNull: *mut bool,
) -> Datum {
    unimplemented!()
}
pub unsafe fn GetAttributeByNum(
    _tuple: HeapTupleHeader,
    _attrno: AttrNumber,
    _isNull: *mut bool,
) -> Datum {
    unimplemented!()
}

pub unsafe fn ExecTargetListLength(_targetlist: *mut List) -> c_int {
    crate::executor::execUtils::ExecTargetListLength(_targetlist as _) as _
}
pub unsafe fn ExecCleanTargetListLength(_targetlist: *mut List) -> c_int {
    unimplemented!()
}

pub unsafe fn ExecGetTriggerOldSlot(
    _estate: *mut EState,
    _relInfo: *mut ResultRelInfo,
) -> *mut TupleTableSlot {
    unimplemented!()
}
pub unsafe fn ExecGetTriggerNewSlot(
    _estate: *mut EState,
    _relInfo: *mut ResultRelInfo,
) -> *mut TupleTableSlot {
    unimplemented!()
}
pub unsafe fn ExecGetReturningSlot(
    _estate: *mut EState,
    _relInfo: *mut ResultRelInfo,
) -> *mut TupleTableSlot {
    unimplemented!()
}
pub unsafe fn ExecGetAllNullSlot(
    _estate: *mut EState,
    _relInfo: *mut ResultRelInfo,
) -> *mut TupleTableSlot {
    unimplemented!()
}
pub unsafe fn ExecGetChildToRootMap(
    _resultRelInfo: *mut ResultRelInfo,
) -> *mut TupleConversionMap {
    unimplemented!()
}
pub unsafe fn ExecGetRootToChildMap(
    _resultRelInfo: *mut ResultRelInfo,
    _estate: *mut EState,
) -> *mut TupleConversionMap {
    unimplemented!()
}

pub unsafe fn ExecGetResultRelCheckAsUser(
    _relInfo: *mut ResultRelInfo,
    _estate: *mut EState,
) -> Oid {
    unimplemented!()
}
pub unsafe fn ExecGetInsertedCols(
    _relinfo: *mut ResultRelInfo,
    _estate: *mut EState,
) -> *mut Bitmapset {
    unimplemented!()
}
pub unsafe fn ExecGetUpdatedCols(
    _relinfo: *mut ResultRelInfo,
    _estate: *mut EState,
) -> *mut Bitmapset {
    unimplemented!()
}
pub unsafe fn ExecGetExtraUpdatedCols(
    _relinfo: *mut ResultRelInfo,
    _estate: *mut EState,
) -> *mut Bitmapset {
    unimplemented!()
}
pub unsafe fn ExecGetAllUpdatedCols(
    _relinfo: *mut ResultRelInfo,
    _estate: *mut EState,
) -> *mut Bitmapset {
    unimplemented!()
}

/*
 * prototypes from functions in execIndexing.c
 */
#[no_mangle]
pub unsafe fn ExecOpenIndices(_resultRelInfo: *mut ResultRelInfo, _speculative: bool) {
    crate::executor::execIndexing::ExecOpenIndices(_resultRelInfo as _, _speculative as _)
}
#[no_mangle]
pub unsafe fn ExecCloseIndices(_resultRelInfo: *mut ResultRelInfo) {
    crate::executor::execIndexing::ExecCloseIndices(_resultRelInfo as _)
}
pub unsafe fn ExecInsertIndexTuples(
    _resultRelInfo: *mut ResultRelInfo,
    _slot: *mut TupleTableSlot,
    _estate: *mut EState,
    _update: bool,
    _noDupErr: bool,
    _specConflict: *mut bool,
    _arbiterIndexes: *mut List,
    _onlySummarizing: bool,
) -> *mut List {
    crate::executor::execIndexing::ExecInsertIndexTuples(_resultRelInfo as _, _slot as _, _estate as _, _update as _, _noDupErr as _, _specConflict as _, _arbiterIndexes as _, _onlySummarizing as _) as _
}
pub unsafe fn ExecCheckIndexConstraints(
    _resultRelInfo: *mut ResultRelInfo,
    _slot: *mut TupleTableSlot,
    _estate: *mut EState,
    _conflictTid: ItemPointer,
    _tupleid: ItemPointer,
    _arbiterIndexes: *mut List,
) -> bool {
    crate::executor::execIndexing::ExecCheckIndexConstraints(_resultRelInfo as _, _slot as _, _estate as _, _conflictTid as _, _tupleid as _, _arbiterIndexes as _) as _
}
pub unsafe fn check_exclusion_constraint(
    _heap: Relation,
    _index: Relation,
    _indexInfo: *mut IndexInfo,
    _tupleid: ItemPointer,
    _values: *const Datum,
    _isnull: *const bool,
    _estate: *mut EState,
    _newIndex: bool,
) {
    crate::executor::execIndexing::check_exclusion_constraint(_heap as _, _index as _, _indexInfo as _, _tupleid as _, _values as _, _isnull as _, _estate as _, _newIndex as _)
}

/*
 * prototypes from functions in execReplication.c
 */
pub unsafe fn RelationFindReplTupleByIndex(
    _rel: Relation,
    _idxoid: Oid,
    _lockmode: LockTupleMode,
    _searchslot: *mut TupleTableSlot,
    _outslot: *mut TupleTableSlot,
) -> bool {
    unimplemented!()
}
pub unsafe fn RelationFindReplTupleSeq(
    _rel: Relation,
    _lockmode: LockTupleMode,
    _searchslot: *mut TupleTableSlot,
    _outslot: *mut TupleTableSlot,
) -> bool {
    unimplemented!()
}

pub unsafe fn ExecSimpleRelationInsert(
    _resultRelInfo: *mut ResultRelInfo,
    _estate: *mut EState,
    _slot: *mut TupleTableSlot,
) {
    crate::executor::execReplication::ExecSimpleRelationInsert(_resultRelInfo as _, _estate as _, _slot as _)
}
pub unsafe fn ExecSimpleRelationUpdate(
    _resultRelInfo: *mut ResultRelInfo,
    _estate: *mut EState,
    _epqstate: *mut EPQState,
    _searchslot: *mut TupleTableSlot,
    _slot: *mut TupleTableSlot,
) {
    crate::executor::execReplication::ExecSimpleRelationUpdate(_resultRelInfo as _, _estate as _, _epqstate as _, _searchslot as _, _slot as _)
}
pub unsafe fn ExecSimpleRelationDelete(
    _resultRelInfo: *mut ResultRelInfo,
    _estate: *mut EState,
    _epqstate: *mut EPQState,
    _searchslot: *mut TupleTableSlot,
) {
    crate::executor::execReplication::ExecSimpleRelationDelete(_resultRelInfo as _, _estate as _, _epqstate as _, _searchslot as _)
}
pub unsafe fn CheckCmdReplicaIdentity(_rel: Relation, _cmd: CmdType) {
    unimplemented!()
}

pub unsafe fn CheckSubscriptionRelkind(
    _relkind: c_char,
    _nspname: *const c_char,
    _relname: *const c_char,
) {
    crate::executor::execReplication::CheckSubscriptionRelkind(_relkind as _, _nspname as _, _relname as _)
}

/*
 * prototypes from functions in nodeModifyTable.c
 * Real implementations live in executor::nodeModifyTable; re-export here.
 */
pub use crate::executor::nodeModifyTable::{ExecGetUpdateNewTuple, ExecLookupResultRelByOid};
