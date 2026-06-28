//! Translated from PostgreSQL src/include/executor/executor.h
//! Support for the POSTGRES executor module.
//!
//! In-memory executor API. Stubs for the .c-implemented functions; the hot
//! `static inline` wrappers are translated through the ExprState/PlanState
//! function pointers.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "TODO(error-migration): pre-existing backlog; new code uses OrElog/?/crate::assert!"
)]

#![allow(non_snake_case, non_camel_case_types, deprecated)]
#![allow(clippy::boxed_local, reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1; real impl consumes params")]

use bitflags::bitflags;

use crate::postgres::{Datum, DatumGetBool};
use crate::postgres_ext::Oid;

use crate::access::attnum::AttrNumber;
use crate::access::sdir::ScanDirection;
use crate::c::Index;
use crate::fmgr::FmgrInfo;
use crate::nodes::lockoptions::LockTupleMode;
use crate::nodes::nodes::{CmdType, Node, OnConflictAction};
use crate::nodes::parsenodes::{RTEPermissionInfo, WCOKind};
use crate::tcop::dest::DestReceiver;
use crate::utils::memutils::MemoryContext;
use crate::utils::rel::Relation;

use crate::access::tupdesc::TupleDesc;
use crate::access::tupconvert::TupleConversionMap;
use crate::executor::execdesc::QueryDesc;
use crate::executor::tuptable::{TupleTableSlot, TupleTableSlotOps};
use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::execnodes::{
    AggState, EPQState, EState, ExecAuxRowMark, ExecRowMark, ExprContext,
    ExprContextCallbackFunction, ExprDoneCond, ExprState, JunkFilter,
    ProjectionInfo, ResultRelInfo, ScanState, SetExprState,
    TupleHashEntry, TupleHashTable,
};
use crate::utils::tuplestore::Tuplestorestate;

// ---------------------------------------------------------------------------
// eflags - bitwise OR of these flags passed to ExecutorStart / ExecInitNode
// ---------------------------------------------------------------------------
// bitflags-port appendix A: GOOD, clean single-bit set.
bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ExecFlag: i32 {
        /// EXPLAIN, no ANALYZE.
        const EXPLAIN_ONLY    = 0x0001;
        /// EXPLAIN (GENERIC_PLAN).
        const EXPLAIN_GENERIC = 0x0002;
        /// need efficient rescan.
        const REWIND          = 0x0004;
        /// need backward scan.
        const BACKWARD        = 0x0008;
        /// need mark/restore.
        const MARK            = 0x0010;
        /// skip AfterTrigger setup.
        const SKIP_TRIGGERS   = 0x0020;
        /// REFRESH ... WITH NO DATA.
        const WITH_NO_DATA    = 0x0040;
    }
}

// ---------------------------------------------------------------------------
// Plugin hooks (function-pointer globals)
// ---------------------------------------------------------------------------
pub type ExecutorStart_hook_type = fn(query_desc: &mut QueryDesc, eflags: i32);
pub type ExecutorRun_hook_type =
    fn(query_desc: &mut QueryDesc, direction: ScanDirection, count: u64);
pub type ExecutorFinish_hook_type = fn(query_desc: &mut QueryDesc);
pub type ExecutorEnd_hook_type = fn(query_desc: &mut QueryDesc);
pub type ExecutorCheckPerms_hook_type =
    fn(range_table: &[Node], rte_perm_infos: &[Node], ereport_on_violation: bool) -> bool;

// extern globals; live process state goes in a later phase.
pub static mut ExecutorStart_hook: Option<ExecutorStart_hook_type> = None;
pub static mut ExecutorRun_hook: Option<ExecutorRun_hook_type> = None;
pub static mut ExecutorFinish_hook: Option<ExecutorFinish_hook_type> = None;
pub static mut ExecutorEnd_hook: Option<ExecutorEnd_hook_type> = None;
pub static mut ExecutorCheckPerms_hook: Option<ExecutorCheckPerms_hook_type> = None;

// ---------------------------------------------------------------------------
// execAmi.c
// ---------------------------------------------------------------------------
pub fn ExecReScan(_node: &mut crate::nodes::execnodes::PlanState) {
    unimplemented!()
}
pub fn ExecMarkPos(_node: &mut crate::nodes::execnodes::PlanState) {
    unimplemented!()
}
pub fn ExecRestrPos(_node: &mut crate::nodes::execnodes::PlanState) {
    unimplemented!()
}
/// `struct Path` is forward-declared in the header to avoid pathnodes.h.
pub fn ExecSupportsMarkRestore(_pathnode: &crate::nodes::pathnodes::Path) -> bool {
    unimplemented!()
}
pub fn ExecSupportsBackwardScan(_node: &crate::nodes::plannodes::Plan) -> bool {
    unimplemented!()
}
pub fn ExecMaterializesOutput(_plantype: i32) -> bool {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// execCurrent.c
// ---------------------------------------------------------------------------
pub fn execCurrentOf(
    _cexpr: &crate::nodes::primnodes::CurrentOfExpr,
    _econtext: &mut ExprContext,
    _table_oid: Oid,
    _current_tid: crate::access::heapam::ItemPointer,
) -> bool {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// execGrouping.c
// ---------------------------------------------------------------------------
pub fn execTuplesMatchPrepare(
    _desc: TupleDesc,
    _num_cols: i32,
    _key_col_idx: &[AttrNumber],
    _eq_operators: &[Oid],
    _collations: &[Oid],
    _parent: &mut crate::nodes::execnodes::PlanState,
) -> Box<ExprState> {
    unimplemented!()
}

/// Returns `(eqFuncOids, hashFunctions)` - the two out-params folded into a tuple.
pub fn execTuplesHashPrepare(_num_cols: i32, _eq_operators: &[Oid]) -> (Vec<Oid>, Vec<FmgrInfo>) {
    unimplemented!()
}

#[allow(clippy::too_many_arguments)]
pub fn BuildTupleHashTable(
    _parent: &mut crate::nodes::execnodes::PlanState,
    _input_desc: TupleDesc,
    _input_ops: &'static dyn TupleTableSlotOps,
    _num_cols: i32,
    _key_col_idx: &[AttrNumber],
    _eqfuncoids: &[Oid],
    _hashfunctions: &[FmgrInfo],
    _collations: &[Oid],
    _nbuckets: i64,
    _additionalsize: usize,
    _metacxt: MemoryContext,
    _tablecxt: MemoryContext,
    _tempcxt: MemoryContext,
    _use_variable_hash_iv: bool,
) -> TupleHashTable {
    unimplemented!()
}

/// Returns the entry plus `(isnew, hash)` out-params folded into the tuple.
pub fn LookupTupleHashEntry(
    _hashtable: &mut TupleHashTable,
    _slot: &mut TupleTableSlot,
) -> (TupleHashEntry, bool, u32) {
    unimplemented!()
}

pub fn TupleHashTableHash(_hashtable: &mut TupleHashTable, _slot: &mut TupleTableSlot) -> u32 {
    unimplemented!()
}

/// `isnew` out-param folded into the tuple.
pub fn LookupTupleHashEntryHash(
    _hashtable: &mut TupleHashTable,
    _slot: &mut TupleTableSlot,
    _hash: u32,
) -> (TupleHashEntry, bool) {
    unimplemented!()
}

pub fn FindTupleHashEntry(
    _hashtable: &mut TupleHashTable,
    _slot: &mut TupleTableSlot,
    _eqcomp: &mut ExprState,
    _hashexpr: &mut ExprState,
) -> TupleHashEntry {
    unimplemented!()
}

pub fn ResetTupleHashTable(_hashtable: &mut TupleHashTable) {
    unimplemented!()
}

/// Size of the hash bucket (useful for estimating memory usage).
pub fn TupleHashEntrySize() -> usize {
    core::mem::size_of::<crate::nodes::execnodes::TupleHashEntryData>()
}

/// Tuple from a hash entry (`entry->firstTuple`).
pub fn TupleHashEntryGetTuple(
    entry: &crate::nodes::execnodes::TupleHashEntryData,
) -> crate::nodes::execnodes::MinimalTuple {
    entry.first_tuple.clone()
}

/// Pointer into the additional space allocated for this entry, or None when
/// `additionalsize` was zero (C returns NULL).
pub fn TupleHashEntryGetAdditional(
    _hashtable: &TupleHashTable,
    _entry: &crate::nodes::execnodes::TupleHashEntryData,
) -> crate::nodes::execnodes::OpaqueState {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// execJunk.c
// ---------------------------------------------------------------------------
pub fn ExecInitJunkFilter(
    _target_list: &[Node],
    _slot: &mut TupleTableSlot,
) -> Box<JunkFilter> {
    unimplemented!()
}
pub fn ExecInitJunkFilterConversion(
    _target_list: &[Node],
    _clean_tup_type: TupleDesc,
    _slot: &mut TupleTableSlot,
) -> Box<JunkFilter> {
    unimplemented!()
}
/// Returns None when the named attribute is not found (was InvalidAttrNumber).
pub fn ExecFindJunkAttribute(_junkfilter: &JunkFilter, _attr_name: &str) -> Option<AttrNumber> {
    unimplemented!()
}
pub fn ExecFindJunkAttributeInTlist(
    _targetlist: &[Node],
    _attr_name: &str,
) -> Option<AttrNumber> {
    unimplemented!()
}
pub fn ExecFilterJunk(_junkfilter: &mut JunkFilter, _slot: &mut TupleTableSlot) -> Box<TupleTableSlot> {
    unimplemented!()
}

/// Extract value (and is-null) for a junk attribute. C's `bool *isNull`
/// out-param folds into the Option (None == SQL NULL).
pub fn ExecGetJunkAttribute(slot: &mut TupleTableSlot, attno: AttrNumber) -> Option<Datum> {
    debug_assert!(attno > 0);
    crate::executor::tuptable::slot_getattr(slot, i32::from(attno))
}

// ---------------------------------------------------------------------------
// execMain.c
// ---------------------------------------------------------------------------
/// PG `ExecutorStart`: dispatches to the hook or `standard_ExecutorStart`. M1
/// always uses the standard path (no plugin hook installed).
pub fn ExecutorStart(query_desc: &mut QueryDesc, eflags: i32) {
    standard_ExecutorStart(query_desc, eflags);
}
pub use crate::backend::executor::execMain::standard_executor_start as standard_ExecutorStart;

/// PG `ExecutorRun`: hook or `standard_ExecutorRun`. Async since M2 (the scan
/// path reaches the table AM's buffer reads, rules.md s5); takes the SharedState
/// the table AM needs. `shared` is `Option` so the childless-const path can run
/// without one (it reaches no I/O leaf); scan/insert plans require `Some`.
#[allow(
    clippy::future_not_send,
    reason = "rules.md s5: the executor's per-query state is !Send and task-confined (one backend task per plan); see standard_executor_run."
)]
pub async fn ExecutorRun(
    shared: Option<&std::sync::Arc<crate::shared_state::SharedState>>,
    query_desc: &mut QueryDesc,
    direction: ScanDirection,
    count: u64,
) {
    standard_ExecutorRun(shared, query_desc, direction, count).await;
}
pub use crate::backend::executor::execMain::standard_executor_run as standard_ExecutorRun;

/// PG `ExecutorFinish`: hook or `standard_ExecutorFinish`.
pub fn ExecutorFinish(query_desc: &mut QueryDesc) {
    standard_ExecutorFinish(query_desc);
}
pub use crate::backend::executor::execMain::standard_executor_finish as standard_ExecutorFinish;

/// PG `ExecutorEnd`: hook or `standard_ExecutorEnd`. Takes the SharedState so
/// node teardown can release buffers/scans (`Option`; the const path needs none).
pub fn ExecutorEnd(
    shared: Option<&std::sync::Arc<crate::shared_state::SharedState>>,
    query_desc: &mut QueryDesc,
) {
    standard_ExecutorEnd(shared, query_desc);
}
pub use crate::backend::executor::execMain::standard_executor_end as standard_ExecutorEnd;
pub fn ExecutorRewind(_query_desc: &mut QueryDesc) {
    unimplemented!()
}
pub fn ExecCheckPermissions(
    _range_table: &[Node],
    _rteperminfos: &[Node],
    _ereport_on_violation: bool,
) -> bool {
    unimplemented!()
}
pub fn ExecCheckOneRelPerms(_perminfo: &RTEPermissionInfo) -> bool {
    unimplemented!()
}
pub fn CheckValidResultRel(
    _result_rel_info: &mut ResultRelInfo,
    _operation: CmdType,
    _on_conflict_action: OnConflictAction,
    _merge_actions: &[Node],
) {
    unimplemented!()
}
pub fn InitResultRelInfo(
    _result_rel_info: &mut ResultRelInfo,
    _result_relation_desc: Relation,
    _result_relation_index: Index,
    _partition_root_rri: Option<&mut ResultRelInfo>,
    _instrument_options: i32,
) {
    unimplemented!()
}
pub fn ExecGetTriggerResultRel(
    _estate: &mut EState,
    _relid: Oid,
    _root_rel_info: Option<&mut ResultRelInfo>,
) -> *mut ResultRelInfo {
    unimplemented!()
}
pub fn ExecGetAncestorResultRels(
    _estate: &mut EState,
    _result_rel_info: &mut ResultRelInfo,
) -> Vec<Box<ResultRelInfo>> {
    unimplemented!()
}
pub fn ExecConstraints(
    _result_rel_info: &mut ResultRelInfo,
    _slot: &mut TupleTableSlot,
    _estate: &mut EState,
) {
    unimplemented!()
}
/// Returns None when all virtual NOT NULL attrs pass (was InvalidAttrNumber).
pub fn ExecRelGenVirtualNotNull(
    _result_rel_info: &mut ResultRelInfo,
    _slot: &mut TupleTableSlot,
    _estate: &mut EState,
    _notnull_virtual_attrs: &[Node],
) -> Option<AttrNumber> {
    unimplemented!()
}
pub fn ExecPartitionCheck(
    _result_rel_info: &mut ResultRelInfo,
    _slot: &mut TupleTableSlot,
    _estate: &mut EState,
    _emit_error: bool,
) -> bool {
    unimplemented!()
}
pub fn ExecPartitionCheckEmitError(
    _result_rel_info: &mut ResultRelInfo,
    _slot: &mut TupleTableSlot,
    _estate: &mut EState,
) {
    unimplemented!()
}
pub fn ExecWithCheckOptions(
    _kind: WCOKind,
    _result_rel_info: &mut ResultRelInfo,
    _slot: &mut TupleTableSlot,
    _estate: &mut EState,
) {
    unimplemented!()
}
pub fn ExecBuildSlotValueDescription(
    _reloid: Oid,
    _slot: &mut TupleTableSlot,
    _tupdesc: TupleDesc,
    _modified_cols: &Bitmapset,
    _maxfieldlen: i32,
) -> String {
    unimplemented!()
}
pub fn ExecUpdateLockMode(_estate: &mut EState, _relinfo: &mut ResultRelInfo) -> LockTupleMode {
    unimplemented!()
}
/// Returns None when not found and `missing_ok` (was NULL).
pub fn ExecFindRowMark(_estate: &mut EState, _rti: Index, _missing_ok: bool) -> Option<Box<ExecRowMark>> {
    unimplemented!()
}
pub fn ExecBuildAuxRowMark(_erm: &mut ExecRowMark, _targetlist: &[Node]) -> Box<ExecAuxRowMark> {
    unimplemented!()
}
pub fn EvalPlanQual(
    _epqstate: &mut EPQState,
    _relation: Relation,
    _rti: Index,
    _inputslot: &mut TupleTableSlot,
) -> Box<TupleTableSlot> {
    unimplemented!()
}
pub fn EvalPlanQualInit(
    _epqstate: &mut EPQState,
    _parentestate: &mut EState,
    _subplan: Option<&crate::nodes::plannodes::Plan>,
    _auxrowmarks: &[Node],
    _epq_param: i32,
    _result_relations: &[Node],
) {
    unimplemented!()
}
pub fn EvalPlanQualSetPlan(
    _epqstate: &mut EPQState,
    _subplan: Option<&crate::nodes::plannodes::Plan>,
    _auxrowmarks: &[Node],
) {
    unimplemented!()
}
pub fn EvalPlanQualSlot(
    _epqstate: &mut EPQState,
    _relation: Relation,
    _rti: Index,
) -> Box<TupleTableSlot> {
    unimplemented!()
}

/// `#define EvalPlanQualSetSlot(epqstate, slot) ((epqstate)->origslot = (slot))`
pub fn EvalPlanQualSetSlot(epqstate: &mut EPQState, slot: Option<Box<TupleTableSlot>>) {
    let _ = (epqstate, slot);
    unimplemented!()
}

pub fn EvalPlanQualFetchRowMark(_epqstate: &mut EPQState, _rti: Index, _slot: &mut TupleTableSlot) -> bool {
    unimplemented!()
}
pub fn EvalPlanQualNext(_epqstate: &mut EPQState) -> Box<TupleTableSlot> {
    unimplemented!()
}
pub fn EvalPlanQualBegin(_epqstate: &mut EPQState) {
    unimplemented!()
}
pub fn EvalPlanQualEnd(_epqstate: &mut EPQState) {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// execProcnode.c
// ---------------------------------------------------------------------------
/// PG `ExecInitNode`. The plan-state tree is a downcast-free enum in this port
/// (execProcnode.rs), so this takes `&Node` (the plan tree) and returns the
/// `PlanStateNode` enum rather than a `PlanState*`.
pub use crate::backend::executor::execProcnode::exec_init_node as ExecInitNode;
pub fn ExecSetExecProcNode(
    node: &mut crate::nodes::execnodes::PlanState,
    function: crate::nodes::execnodes::ExecProcNodeMtd,
) {
    node.exec_proc_node = Some(function);
}
pub fn MultiExecProcNode(_node: &mut crate::nodes::execnodes::PlanState) -> Option<Node> {
    unimplemented!()
}
/// PG `ExecEndNode`. Takes the `PlanStateNode` enum (downcast-free dispatch).
pub use crate::backend::executor::execProcnode::exec_end_node as ExecEndNode;
pub fn ExecShutdownNode(_node: &mut crate::nodes::execnodes::PlanState) {
    unimplemented!()
}
pub fn ExecSetTupleBound(_tuples_needed: i64, _child_node: &mut crate::nodes::execnodes::PlanState) {
    unimplemented!()
}

/// ExecProcNode - execute the given node to return a(nother) tuple. Dispatches
/// on the `PlanStateNode` enum (PG's per-node function pointer + downcast).
pub use crate::backend::executor::execProcnode::exec_proc_node as ExecProcNode;

// ---------------------------------------------------------------------------
// execExpr.c
// ---------------------------------------------------------------------------
/// PG `ExecInitExpr`. Returns None for a null node (C returns NULL).
pub use crate::backend::executor::execExpr::exec_init_expr as ExecInitExpr;
pub fn ExecInitExprWithParams(
    _node: Option<&crate::nodes::primnodes::Expr>,
    _ext_params: crate::nodes::params::ParamListInfo,
) -> Box<ExprState> {
    unimplemented!()
}
/// PG `ExecInitQual`. Returns None for an empty qual (C returns NULL =
/// always-true).
pub use crate::backend::executor::execExpr::exec_init_qual as ExecInitQual;
pub fn ExecInitCheck(
    _qual: &[Node],
    _parent: Option<&mut crate::nodes::execnodes::PlanState>,
) -> Box<ExprState> {
    unimplemented!()
}
pub fn ExecInitExprList(
    _nodes: &[Node],
    _parent: Option<&mut crate::nodes::execnodes::PlanState>,
) -> Vec<Box<ExprState>> {
    unimplemented!()
}
pub fn ExecBuildAggTrans(
    _aggstate: &mut AggState,
    _phase: &mut crate::executor::nodeAgg::AggStatePerPhaseData,
    _do_sort: bool,
    _do_hash: bool,
    _nullcheck: bool,
) -> Box<ExprState> {
    unimplemented!()
}
#[allow(clippy::too_many_arguments)]
pub fn ExecBuildHash32FromAttrs(
    _desc: TupleDesc,
    _ops: &'static dyn TupleTableSlotOps,
    _hashfunctions: &[FmgrInfo],
    _collations: &[Oid],
    _num_cols: i32,
    _key_col_idx: &[AttrNumber],
    _parent: &mut crate::nodes::execnodes::PlanState,
    _init_value: u32,
) -> Box<ExprState> {
    unimplemented!()
}
#[allow(clippy::too_many_arguments)]
pub fn ExecBuildHash32Expr(
    _desc: TupleDesc,
    _ops: &'static dyn TupleTableSlotOps,
    _hashfunc_oids: &[Oid],
    _collations: &[Node],
    _hash_exprs: &[Node],
    _opstrict: &[bool],
    _parent: &mut crate::nodes::execnodes::PlanState,
    _init_value: u32,
    _keep_nulls: bool,
) -> Box<ExprState> {
    unimplemented!()
}
#[allow(clippy::too_many_arguments)]
pub fn ExecBuildGroupingEqual(
    _ldesc: TupleDesc,
    _rdesc: TupleDesc,
    _lops: &'static dyn TupleTableSlotOps,
    _rops: &'static dyn TupleTableSlotOps,
    _num_cols: i32,
    _key_col_idx: &[AttrNumber],
    _eqfunctions: &[Oid],
    _collations: &[Oid],
    _parent: &mut crate::nodes::execnodes::PlanState,
) -> Box<ExprState> {
    unimplemented!()
}
#[allow(clippy::too_many_arguments)]
pub fn ExecBuildParamSetEqual(
    _desc: TupleDesc,
    _lops: &'static dyn TupleTableSlotOps,
    _rops: &'static dyn TupleTableSlotOps,
    _eqfunctions: &[Oid],
    _collations: &[Oid],
    _param_exprs: &[Node],
    _parent: &mut crate::nodes::execnodes::PlanState,
) -> Box<ExprState> {
    unimplemented!()
}
pub fn ExecBuildProjectionInfo(
    _target_list: &[Node],
    _econtext: &mut ExprContext,
    _slot: &mut TupleTableSlot,
    _parent: Option<&mut crate::nodes::execnodes::PlanState>,
    _input_desc: TupleDesc,
) -> Box<ProjectionInfo> {
    unimplemented!()
}
#[allow(clippy::too_many_arguments)]
pub fn ExecBuildUpdateProjection(
    _target_list: &[Node],
    _eval_target_list: bool,
    _target_colnos: &[Node],
    _rel_desc: TupleDesc,
    _econtext: &mut ExprContext,
    _slot: &mut TupleTableSlot,
    _parent: &mut crate::nodes::execnodes::PlanState,
) -> Box<ProjectionInfo> {
    unimplemented!()
}
pub fn ExecPrepareExpr(_node: &crate::nodes::primnodes::Expr, _estate: &mut EState) -> Box<ExprState> {
    unimplemented!()
}
pub fn ExecPrepareQual(_qual: &[Node], _estate: &mut EState) -> Box<ExprState> {
    unimplemented!()
}
pub fn ExecPrepareCheck(_qual: &[Node], _estate: &mut EState) -> Box<ExprState> {
    unimplemented!()
}
pub fn ExecPrepareExprList(_nodes: &[Node], _estate: &mut EState) -> Vec<Box<ExprState>> {
    unimplemented!()
}

/// ExecEvalExpr - evaluate expression `state` in `econtext`. The C `bool
/// *isNull` out-param folds into the returned Option (None == SQL NULL).
/// Dispatches through `ExprState.evalfunc` (was a function pointer).
pub fn ExecEvalExpr(state: &mut ExprState, econtext: &mut ExprContext) -> Option<Datum> {
    let mut is_null = false;
    let f = state.evalfunc.expect("ExecEvalExpr evalfunc unset");
    let v = f(state, econtext, &mut is_null);
    (!is_null).then_some(v)
}

/// Like ExecEvalExpr, but for side-effect-only evaluation (no return value).
pub fn ExecEvalExprNoReturn(state: &mut ExprState, econtext: &mut ExprContext) {
    let mut is_null = false;
    let f = state.evalfunc.expect("ExecEvalExprNoReturn evalfunc unset");
    let _ = f(state, econtext, &mut is_null);
}

/// Same as ExecEvalExpr, but switches into the right allocation context.
pub fn ExecEvalExprSwitchContext(state: &mut ExprState, econtext: &mut ExprContext) -> Option<Datum> {
    // TODO(memory): ExprContext.ecxt_per_tuple_memory needs to be a MemoryContext
    // handle (execnodes currently types it as MemoryContextKind); defer body.
    let _ = (state, econtext);
    unimplemented!()
}

/// Same as ExecEvalExprNoReturn, but switches the allocation context.
pub fn ExecEvalExprNoReturnSwitchContext(state: &mut ExprState, econtext: &mut ExprContext) {
    // TODO(memory): see ExecEvalExprSwitchContext.
    let _ = (state, econtext);
    unimplemented!()
}

/// ExecProject - project a tuple per `projInfo`, into its result slot.
pub fn ExecProject(proj_info: &mut ProjectionInfo) -> &mut TupleTableSlot {
    // Inlined ExecStoreVirtualTuple: clear, run, mark valid virtual tuple.
    // Faithful body needs ExecClearTuple + slot/desc plumbing not yet wired;
    // keep the signature and defer the body.
    let _ = &mut proj_info.state;
    unimplemented!()
}

/// ExecQual - evaluate a qual prepared with ExecInitQual. Returns true if the
/// qual is satisfied (resultForNull == false semantics).
pub fn ExecQual(state: Option<&mut ExprState>, econtext: &mut ExprContext) -> bool {
    // short-circuit for empty restriction list
    let Some(state) = state else { return true };
    debug_assert!(state.flags.contains(crate::nodes::execnodes::EeoFlag::IS_QUAL));
    // QUAL never returns NULL.
    let ret = ExecEvalExprSwitchContext(state, econtext).expect("QUAL returned NULL");
    DatumGetBool(ret)
}

/// ExecQualAndReset - evaluate a qual then reset the per-tuple context.
pub fn ExecQualAndReset(state: Option<&mut ExprState>, econtext: &mut ExprContext) -> bool {
    // TODO(memory): per-tuple memory context type unification (see SwitchContext).
    let _ = (state, econtext);
    unimplemented!()
}

pub fn ExecCheck(_state: &mut ExprState, _econtext: &mut ExprContext) -> bool {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// execSRF.c
// ---------------------------------------------------------------------------
pub fn ExecInitTableFunctionResult(
    _expr: &crate::nodes::primnodes::Expr,
    _econtext: &mut ExprContext,
    _parent: &mut crate::nodes::execnodes::PlanState,
) -> Box<SetExprState> {
    unimplemented!()
}
pub fn ExecMakeTableFunctionResult(
    _setexpr: &mut SetExprState,
    _econtext: &mut ExprContext,
    _arg_context: MemoryContext,
    _expected_desc: TupleDesc,
    _random_access: bool,
) -> Box<Tuplestorestate> {
    unimplemented!()
}
pub fn ExecInitFunctionResultSet(
    _expr: &crate::nodes::primnodes::Expr,
    _econtext: &mut ExprContext,
    _parent: &mut crate::nodes::execnodes::PlanState,
) -> Box<SetExprState> {
    unimplemented!()
}
/// Returns `(Option<Datum>, ExprDoneCond)` - `isNull` folds into the Option,
/// `isDone` returned alongside.
pub fn ExecMakeFunctionResultSet(
    _fcache: &mut SetExprState,
    _econtext: &mut ExprContext,
    _arg_context: MemoryContext,
) -> (Option<Datum>, ExprDoneCond) {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// execScan.c
// ---------------------------------------------------------------------------
pub type ExecScanAccessMtd = fn(node: &mut ScanState) -> Option<Box<TupleTableSlot>>;
pub type ExecScanRecheckMtd = fn(node: &mut ScanState, slot: &mut TupleTableSlot) -> bool;

pub fn ExecScan(
    _node: &mut ScanState,
    _access_mtd: ExecScanAccessMtd,
    _recheck_mtd: ExecScanRecheckMtd,
) -> Option<Box<TupleTableSlot>> {
    // The C fn-pointer-driven generic loop is replaced by per-node drivers
    // (ExecSeqScan) over the plan-state enum; the qual+project tail lives in
    // backend/executor/execScan.rs::exec_scan, called by those drivers. This
    // pointer-signature entry stays a grow guard (rules.md s3/s4).
    unimplemented!("ExecScan: use the per-node driver (ExecSeqScan); see backend execScan::exec_scan")
}
/// PG `ExecAssignScanProjectionInfo`: build the scan node's projection (input desc
/// = the scan tuple descriptor).
pub use crate::backend::executor::execScan::exec_assign_scan_projection_info as ExecAssignScanProjectionInfo;
pub fn ExecAssignScanProjectionInfoWithVarno(_node: &mut ScanState, _varno: i32) {
    unimplemented!()
}
pub fn ExecScanReScan(_node: &mut ScanState) {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// execTuples.c
// ---------------------------------------------------------------------------
pub fn ExecInitResultTypeTL(_planstate: &mut crate::nodes::execnodes::PlanState) {
    unimplemented!()
}
pub fn ExecInitResultSlot(
    _planstate: &mut crate::nodes::execnodes::PlanState,
    _tts_ops: &'static dyn TupleTableSlotOps,
) {
    unimplemented!()
}
pub fn ExecInitResultTupleSlotTL(
    _planstate: &mut crate::nodes::execnodes::PlanState,
    _tts_ops: &'static dyn TupleTableSlotOps,
) {
    unimplemented!()
}
pub fn ExecInitScanTupleSlot(
    _estate: &mut EState,
    _scanstate: &mut ScanState,
    _tupledesc: TupleDesc,
    _tts_ops: &'static dyn TupleTableSlotOps,
) {
    unimplemented!()
}
pub fn ExecInitExtraTupleSlot(
    _estate: &mut EState,
    _tupledesc: TupleDesc,
    _tts_ops: &'static dyn TupleTableSlotOps,
) -> Box<TupleTableSlot> {
    unimplemented!()
}
pub fn ExecInitNullTupleSlot(
    _estate: &mut EState,
    _tup_type: TupleDesc,
    _tts_ops: &'static dyn TupleTableSlotOps,
) -> Box<TupleTableSlot> {
    unimplemented!()
}
pub use crate::backend::executor::execTuples::exec_type_from_tl as ExecTypeFromTL;
pub use crate::backend::executor::execTuples::exec_clean_type_from_tl as ExecCleanTypeFromTL;
pub fn ExecTypeFromExprList(_expr_list: &[Node]) -> TupleDesc {
    unimplemented!()
}
pub fn ExecTypeSetColNames(_type_info: TupleDesc, _names_list: &[Node]) {
    unimplemented!()
}
pub fn UpdateChangedParamSet(_node: &mut crate::nodes::execnodes::PlanState, _newchg: &Bitmapset) {
    unimplemented!()
}

pub struct TupOutputState {
    pub slot: Option<Box<TupleTableSlot>>,
    pub dest: Box<dyn DestReceiver>,
}

pub fn begin_tup_output_tupdesc(
    _dest: Box<dyn DestReceiver>,
    _tupdesc: TupleDesc,
    _tts_ops: &'static dyn TupleTableSlotOps,
) -> Box<TupOutputState> {
    unimplemented!()
}
pub fn do_tup_output(_tstate: &mut TupOutputState, _values: &[Datum], _isnull: &[bool]) {
    unimplemented!()
}
pub fn do_text_output_multiline(_tstate: &mut TupOutputState, _txt: &str) {
    unimplemented!()
}
pub fn end_tup_output(_tstate: &mut TupOutputState) {
    unimplemented!()
}

/// `do_text_output_oneline` - write a single line of text (single-TEXT-attr
/// tupdesc). Was a `do { } while(0)` macro.
pub fn do_text_output_oneline(tstate: &mut TupOutputState, str_to_emit: &str) {
    let values = [crate::postgres::PointerGetDatum(str_to_emit.as_ptr())];
    let isnull = [false];
    do_tup_output(tstate, &values, &isnull);
}

// ---------------------------------------------------------------------------
// execUtils.c
// ---------------------------------------------------------------------------
pub use crate::backend::executor::execUtils::create_executor_state as CreateExecutorState;
pub use crate::backend::executor::execUtils::free_executor_state as FreeExecutorState;
pub use crate::backend::executor::execUtils::create_expr_context as CreateExprContext;
pub fn CreateWorkExprContext(_estate: &mut EState) -> Box<ExprContext> {
    unimplemented!()
}
pub fn CreateStandaloneExprContext() -> Box<ExprContext> {
    unimplemented!()
}
pub fn FreeExprContext(_econtext: &mut ExprContext, _is_commit: bool) {
    unimplemented!()
}
pub fn ReScanExprContext(_econtext: &mut ExprContext) {
    unimplemented!()
}

/// `#define ResetExprContext(econtext) MemoryContextReset(...per_tuple_memory)`.
/// Memory is tombstoned (rules.md s6.4); see the backend body.
pub use crate::backend::executor::execUtils::reset_expr_context as ResetExprContext;

pub use crate::backend::executor::execUtils::make_per_tuple_expr_context as MakePerTupleExprContext;

/// `GetPerTupleExprContext` - get (creating if needed) the per-output-tuple
/// exprcontext. Was a macro selecting the cached one or making it.
pub fn GetPerTupleExprContext(estate: &mut EState) -> &mut ExprContext {
    if estate.per_tuple_exprcontext.is_none() {
        return MakePerTupleExprContext(estate);
    }
    estate.per_tuple_exprcontext.as_mut().unwrap()
}

/// `GetPerTupleMemoryContext` - per-tuple exprcontext's memory context.
pub fn GetPerTupleMemoryContext(estate: &mut EState) -> MemoryContext {
    // TODO(memory): ecxt_per_tuple_memory type unification.
    let _ = estate;
    unimplemented!()
}

/// `ResetPerTupleExprContext` - reset the per-tuple exprcontext if it exists.
pub fn ResetPerTupleExprContext(estate: &mut EState) {
    if let Some(ec) = estate.per_tuple_exprcontext.as_mut() {
        ResetExprContext(ec);
    }
}

pub use crate::backend::executor::execUtils::exec_assign_expr_context as ExecAssignExprContext;
pub use crate::backend::executor::execUtils::exec_get_result_type as ExecGetResultType;
/// `isfixed` out-param folded into the tuple.
pub fn ExecGetResultSlotOps(
    _planstate: &mut crate::nodes::execnodes::PlanState,
) -> (&'static dyn TupleTableSlotOps, bool) {
    unimplemented!()
}
pub fn ExecGetCommonSlotOps(
    _planstates: &mut [&mut crate::nodes::execnodes::PlanState],
) -> &'static dyn TupleTableSlotOps {
    unimplemented!()
}
pub fn ExecGetCommonChildSlotOps(
    _ps: &mut crate::nodes::execnodes::PlanState,
) -> &'static dyn TupleTableSlotOps {
    unimplemented!()
}
pub use crate::backend::executor::execUtils::exec_assign_projection_info as ExecAssignProjectionInfo;
pub fn ExecConditionalAssignProjectionInfo(
    _planstate: &mut crate::nodes::execnodes::PlanState,
    _input_desc: TupleDesc,
    _varno: i32,
) {
    unimplemented!()
}
pub fn ExecAssignScanType(_scanstate: &mut ScanState, _tup_desc: TupleDesc) {
    unimplemented!()
}
pub fn ExecCreateScanSlotFromOuterPlan(
    _estate: &mut EState,
    _scanstate: &mut ScanState,
    _tts_ops: &'static dyn TupleTableSlotOps,
) {
    unimplemented!()
}
pub fn ExecRelationIsTargetRelation(_estate: &mut EState, _scanrelid: Index) -> bool {
    unimplemented!()
}
pub fn ExecOpenScanRelation(_estate: &mut EState, _scanrelid: Index, _eflags: i32) -> Relation {
    unimplemented!()
}
pub fn ExecInitRangeTable(
    _estate: &mut EState,
    _range_table: &[Node],
    _perm_infos: &[Node],
    _unpruned_relids: &Bitmapset,
) {
    unimplemented!()
}
pub fn ExecCloseRangeTableRelations(_estate: &mut EState) {
    unimplemented!()
}
pub fn ExecCloseResultRelations(_estate: &mut EState) {
    unimplemented!()
}

/// `exec_rt_fetch(rti, estate)` - `list_nth(range_table, rti - 1)`.
pub fn exec_rt_fetch(rti: Index, estate: &EState) -> &crate::nodes::parsenodes::RangeTblEntry {
    let _ = &estate.range_table[rti - 1];
    unimplemented!()
}

pub fn ExecGetRangeTableRelation(_estate: &mut EState, _rti: Index, _is_result_rel: bool) -> Relation {
    unimplemented!()
}
pub fn ExecInitResultRelation(_estate: &mut EState, _result_rel_info: &mut ResultRelInfo, _rti: Index) {
    unimplemented!()
}

pub fn executor_errposition(_estate: &mut EState, _location: i32) -> i32 {
    unimplemented!()
}

pub fn RegisterExprContextCallback(
    _econtext: &mut ExprContext,
    _function: ExprContextCallbackFunction,
    _arg: Datum,
) {
    unimplemented!()
}
pub fn UnregisterExprContextCallback(
    _econtext: &mut ExprContext,
    _function: ExprContextCallbackFunction,
    _arg: Datum,
) {
    unimplemented!()
}

/// `bool *isNull` out-param folds into the Option (None == SQL NULL).
pub fn GetAttributeByName(
    _tuple: *mut crate::access::htup::HeapTupleHeaderData,
    _attname: &str,
) -> Option<Datum> {
    unimplemented!()
}
pub fn GetAttributeByNum(
    _tuple: *mut crate::access::htup::HeapTupleHeaderData,
    _attrno: AttrNumber,
) -> Option<Datum> {
    unimplemented!()
}

pub use crate::backend::executor::execTuples::exec_target_list_length as ExecTargetListLength;
pub use crate::backend::executor::execTuples::exec_clean_target_list_length as ExecCleanTargetListLength;

pub fn ExecGetTriggerOldSlot(_estate: &mut EState, _rel_info: &mut ResultRelInfo) -> Box<TupleTableSlot> {
    unimplemented!()
}
pub fn ExecGetTriggerNewSlot(_estate: &mut EState, _rel_info: &mut ResultRelInfo) -> Box<TupleTableSlot> {
    unimplemented!()
}
pub fn ExecGetReturningSlot(_estate: &mut EState, _rel_info: &mut ResultRelInfo) -> Box<TupleTableSlot> {
    unimplemented!()
}
pub fn ExecGetAllNullSlot(_estate: &mut EState, _rel_info: &mut ResultRelInfo) -> Box<TupleTableSlot> {
    unimplemented!()
}
pub fn ExecGetChildToRootMap(_result_rel_info: &mut ResultRelInfo) -> Option<Box<TupleConversionMap>> {
    unimplemented!()
}
pub fn ExecGetRootToChildMap(
    _result_rel_info: &mut ResultRelInfo,
    _estate: &mut EState,
) -> Option<Box<TupleConversionMap>> {
    unimplemented!()
}

pub fn ExecGetResultRelCheckAsUser(_rel_info: &mut ResultRelInfo, _estate: &mut EState) -> Oid {
    unimplemented!()
}
pub fn ExecGetInsertedCols(_relinfo: &mut ResultRelInfo, _estate: &mut EState) -> Box<Bitmapset> {
    unimplemented!()
}
pub fn ExecGetUpdatedCols(_relinfo: &mut ResultRelInfo, _estate: &mut EState) -> Box<Bitmapset> {
    unimplemented!()
}
pub fn ExecGetExtraUpdatedCols(_relinfo: &mut ResultRelInfo, _estate: &mut EState) -> Box<Bitmapset> {
    unimplemented!()
}
pub fn ExecGetAllUpdatedCols(_relinfo: &mut ResultRelInfo, _estate: &mut EState) -> Box<Bitmapset> {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// execIndexing.c
// ---------------------------------------------------------------------------
pub fn ExecOpenIndices(_result_rel_info: &mut ResultRelInfo, _speculative: bool) {
    unimplemented!()
}
pub fn ExecCloseIndices(_result_rel_info: &mut ResultRelInfo) {
    unimplemented!()
}
#[allow(clippy::too_many_arguments)]
/// Returns the recheck-index list plus the `specConflict` out-param folded in.
pub fn ExecInsertIndexTuples(
    _result_rel_info: &mut ResultRelInfo,
    _slot: &mut TupleTableSlot,
    _estate: &mut EState,
    _update: bool,
    _no_dup_err: bool,
    _arbiter_indexes: &[Node],
    _only_summarizing: bool,
) -> (Vec<Node>, bool) {
    unimplemented!()
}
pub fn ExecCheckIndexConstraints(
    _result_rel_info: &mut ResultRelInfo,
    _slot: &mut TupleTableSlot,
    _estate: &mut EState,
    _conflict_tid: crate::access::heapam::ItemPointer,
    _tupleid: crate::access::heapam::ItemPointer,
    _arbiter_indexes: &[Node],
) -> bool {
    unimplemented!()
}
#[allow(clippy::too_many_arguments)]
pub fn check_exclusion_constraint(
    _heap: Relation,
    _index: Relation,
    _index_info: &crate::nodes::execnodes::IndexInfo,
    _tupleid: crate::access::heapam::ItemPointer,
    _values: &[Datum],
    _isnull: &[bool],
    _estate: &mut EState,
    _new_index: bool,
) {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// execReplication.c
// ---------------------------------------------------------------------------
pub fn RelationFindReplTupleByIndex(
    _rel: Relation,
    _idxoid: Oid,
    _lockmode: LockTupleMode,
    _searchslot: &mut TupleTableSlot,
    _outslot: &mut TupleTableSlot,
) -> bool {
    unimplemented!()
}
pub fn RelationFindReplTupleSeq(
    _rel: Relation,
    _lockmode: LockTupleMode,
    _searchslot: &mut TupleTableSlot,
    _outslot: &mut TupleTableSlot,
) -> bool {
    unimplemented!()
}
pub fn ExecSimpleRelationInsert(
    _result_rel_info: &mut ResultRelInfo,
    _estate: &mut EState,
    _slot: &mut TupleTableSlot,
) {
    unimplemented!()
}
pub fn ExecSimpleRelationUpdate(
    _result_rel_info: &mut ResultRelInfo,
    _estate: &mut EState,
    _epqstate: &mut EPQState,
    _searchslot: &mut TupleTableSlot,
    _slot: &mut TupleTableSlot,
) {
    unimplemented!()
}
pub fn ExecSimpleRelationDelete(
    _result_rel_info: &mut ResultRelInfo,
    _estate: &mut EState,
    _epqstate: &mut EPQState,
    _searchslot: &mut TupleTableSlot,
) {
    unimplemented!()
}
pub fn CheckCmdReplicaIdentity(_rel: Relation, _cmd: CmdType) {
    unimplemented!()
}
pub fn CheckSubscriptionRelkind(_relkind: char, _nspname: &str, _relname: &str) {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// nodeModifyTable.c
// ---------------------------------------------------------------------------
pub fn ExecGetUpdateNewTuple(
    _relinfo: &mut ResultRelInfo,
    _plan_slot: &mut TupleTableSlot,
    _old_slot: &mut TupleTableSlot,
) -> Box<TupleTableSlot> {
    unimplemented!()
}
/// Returns None when not found and `missing_ok` (was NULL).
pub fn ExecLookupResultRelByOid(
    _node: &mut crate::nodes::execnodes::ModifyTableState,
    _resultoid: Oid,
    _missing_ok: bool,
    _update_cache: bool,
) -> Option<*mut ResultRelInfo> {
    unimplemented!()
}
