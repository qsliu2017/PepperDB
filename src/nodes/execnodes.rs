//! Translated from PostgreSQL src/include/nodes/execnodes.h
//!
//! Executor state nodes. These are all *in-memory* structs (planner/executor
//! state) - no on-disk layout contract, so no `#[repr(C)]`. Concrete node
//! structs drop the leading `NodeTag type` header; the `*State` nodes keep their
//! embedded base (`PlanState ps` / `ScanState ss` / `JoinState js`) as the first
//! field, which carries the real shared data (plan, lefttree, qual, ...).
//!
//! Node pointers map to `Option<Node>` / `Node`; `List*` of nodes ->
//! `Vec<Node>`; fn-ptr exec callbacks -> Rust fn pointers; `void *arg` ->
//! closures. Many fields reference types from modules not yet translated; those
//! are opaque aliases below.

use std::sync::Arc;

use crate::access::attnum::AttrNumber;
use crate::nodes::bitmapset::Bitmapset;
use crate::utils::rel::RelationData;
use crate::nodes::nodes::{
    AggSplit, AggStrategy, CmdType, JoinType, LimitOption, Node,
};
use crate::postgres::{Datum, NullableDatum};
use crate::postgres_ext::Oid;
use bitflags::bitflags;

// ---------------------------------------------------------------------------
// Opaque forward-declared types referenced here but owned by other modules.
// Kept as local opaque placeholders for the header skeleton.
// ---------------------------------------------------------------------------

macro_rules! opaque_forward {
    ($($name:ident),* $(,)?) => {
        $(
            #[derive(Debug, Clone, PartialEq, Eq, Default)]
            pub struct $name;
        )*
    };
}

mod fwd {
    opaque_forward! {
        FmgrInfo, FunctionCallInfo, MemoryContext, Tuplestorestate,
        ParamListInfo, ParamExecData,
        QueryEnvironment, PartitionDirectory, HeapTuple, MinimalTuple,
        ItemPointerData, OffsetNumber, Buffer, TriggerDesc, Instrumentation,
        WorkerInstrumentation, ErrorSaveContext, SortSupport,
        TupleConversionMap, FdwRoutine, TIDBitmap, ConditionVariable, DlistHead,
        Pairingheap, TuplesortInstrumentation, IndexScanInstrumentation,
        SharedIndexScanInstrumentation, RowMarkType, LockClauseStrength,
        LockWaitPolicy, MergeAction, SubPlan, WindowFunc, JsonExpr,
        JsonPathVariable, ScanKeyData, IndexScanDescData, TableScanDescData, Htab,
    }
}

pub use fwd::*;

// Executor-spine types wired to their real homes (step 08). The placeholders
// these replace were stand-ins until the executor was translated; the spine now
// threads real slots/descriptors/steps/snapshots through EState/PlanState/etc.
pub use crate::access::sdir::ScanDirection;
pub use crate::access::tupdesc::TupleDesc;
pub use crate::executor::execExpr::ExprEvalStep;
pub use crate::executor::tuptable::{TupleTableSlot, TupleTableSlotOps};
pub use crate::nodes::plannodes::PlannedStmt;
pub use crate::utils::snapshot::Snapshot;

/// Opaque private/foreign struct only referenced via pointer in the header.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Opaque;

/// C `void *` handle whose concrete type lives in a private .c file (hash join
/// table, tuplesort state, FDW/AM state, parallel coordination, etc).
pub type OpaqueState = Option<Box<Opaque>>;

/// simplehash-generated tuple hash table (`tuplehash_hash`); maps to HashMap.
#[derive(Debug, Default)]
pub struct TuplehashHash;

pub type TupleHashIterator = usize;

// ---------------------------------------------------------------------------
// ExprState
// ---------------------------------------------------------------------------

/// Function that actually evaluates an ExprState.
#[allow(deprecated)]
pub type ExprStateEvalFunc =
    fn(expression: &mut ExprState, econtext: &mut ExprContext, is_null: &mut bool) -> Datum;

bitflags! {
    /// Bits in `ExprState::flags` (EEO_FLAG_*). See also execExpr.h for private bits.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct EeoFlag: u8 {
        /// expression is for use with ExecQual().
        const IS_QUAL     = 1 << 0;
        /// expression refers to OLD table columns.
        const HAS_OLD     = 1 << 1;
        /// expression refers to NEW table columns.
        const HAS_NEW     = 1 << 2;
        /// OLD table row is NULL in RETURNING list.
        const OLD_IS_NULL = 1 << 3;
        /// NEW table row is NULL in RETURNING list.
        const NEW_IS_NULL = 1 << 4;
    }
}

impl Default for EeoFlag {
    fn default() -> Self {
        Self::empty()
    }
}

/// Evaluation state for a whole expression tree.
#[allow(deprecated)]
#[derive(Default)]
pub struct ExprState {
    pub flags: EeoFlag,
    /// result value of a scalar expression / individual column result.
    pub resnull: bool,
    pub resvalue: Datum,
    /// if projecting a tuple result, holds the result; else None.
    pub resultslot: Option<Box<TupleTableSlot>>,
    /// instructions to compute the expression's return value.
    pub steps: Vec<ExprEvalStep>,
    /// function that actually evaluates the expression.
    pub evalfunc: Option<ExprStateEvalFunc>,
    /// original expression tree, for debugging only.
    pub expr: Option<Node>,
    /// private state for an evalfunc (C `void *`).
    pub evalfunc_private: OpaqueState,
    pub steps_len: i32,
    pub steps_alloc: i32,
    /// parent PlanState node, if any.
    pub parent: Option<Box<PlanState>>,
    pub ext_params: Option<Box<ParamListInfo>>,
    pub innermost_caseval: Option<Box<Datum>>,
    pub innermost_casenull: Option<Box<bool>>,
    pub innermost_domainval: Option<Box<Datum>>,
    pub innermost_domainnull: Option<Box<bool>>,
    /// soft-error sink; None means errors are thrown.
    pub escontext: Option<Box<ErrorSaveContext>>,
}

// ---------------------------------------------------------------------------
// IndexInfo
// ---------------------------------------------------------------------------

/// Information needed to construct index entries for a particular index.
#[allow(deprecated)]
#[derive(Default)]
pub struct IndexInfo {
    pub num_index_attrs: i32,
    pub num_index_key_attrs: i32,
    /// underlying-rel attribute numbers used as keys (0 = expression).
    pub index_attr_numbers: Vec<AttrNumber>,
    pub expressions: Vec<Node>,
    pub expressions_state: Vec<ExprState>,
    pub predicate: Vec<Node>,
    pub predicate_state: Option<Box<ExprState>>,
    pub exclusion_ops: Vec<Oid>,
    pub exclusion_procs: Vec<Oid>,
    pub exclusion_strats: Vec<u16>,
    pub unique_ops: Vec<Oid>,
    pub unique_procs: Vec<Oid>,
    pub unique_strats: Vec<u16>,
    pub unique: bool,
    pub nulls_not_distinct: bool,
    pub ready_for_inserts: bool,
    pub checked_unchanged: bool,
    pub index_unchanged: bool,
    pub concurrent: bool,
    pub broken_hot_chain: bool,
    pub summarizing: bool,
    pub without_overlaps: bool,
    pub parallel_workers: i32,
    pub am: Oid,
    /// private cache area for index AM (C `void *`).
    pub am_cache: OpaqueState,
    pub context: MemoryContext,
}

// ---------------------------------------------------------------------------
// ExprContext
// ---------------------------------------------------------------------------

/// Callback to run at ExprContext shutdown. The C `void *arg` becomes the
/// closure's captured state. `+ Send + Sync`: the backend drives the executor
/// future on a dedicated task and shared `&IndexInfo`/`&ExprContext` refs cross
/// awaits, so the callback must be Send+Sync (keeps those types Send via Sync).
pub type ExprContextCallbackFunction = Box<dyn FnMut() + Send + Sync>;

/// One entry on the ExprContext shutdown callback list.
pub struct ExprContextCb {
    pub function: ExprContextCallbackFunction,
}

/// "Current context" for evaluating expressions during quals/projections.
#[allow(deprecated)]
#[derive(Default)]
pub struct ExprContext {
    /// tuples that Var nodes in the expression may refer to.
    pub ecxt_scantuple: Option<Box<TupleTableSlot>>,
    pub ecxt_innertuple: Option<Box<TupleTableSlot>>,
    pub ecxt_outertuple: Option<Box<TupleTableSlot>>,
    /// query-lifespan / per-tuple memory contexts.
    pub ecxt_per_query_memory: MemoryContext,
    pub ecxt_per_tuple_memory: MemoryContext,
    /// values to substitute for Param nodes.
    pub ecxt_param_exec_vals: Option<Box<ParamExecData>>,
    pub ecxt_param_list_info: Option<Box<ParamListInfo>>,
    /// precomputed values/nulls for aggs/windowfuncs.
    pub ecxt_aggvalues: Vec<Datum>,
    pub ecxt_aggnulls: Vec<bool>,
    /// value to substitute for CaseTestExpr nodes.
    pub case_value_datum: Datum,
    pub case_value_is_null: bool,
    /// value to substitute for CoerceToDomainValue nodes.
    pub domain_value_datum: Datum,
    pub domain_value_is_null: bool,
    /// tuples that OLD/NEW Var nodes in RETURNING may refer to.
    pub ecxt_oldtuple: Option<Box<TupleTableSlot>>,
    pub ecxt_newtuple: Option<Box<TupleTableSlot>>,
    /// link to containing EState (None if standalone). A back-pointer never
    /// populated on the M2 live path; pinned `'static` so `ExprContext` stays
    /// lifetime-free (the borrowed range-table lives on the EState, not here).
    pub ecxt_estate: Option<Box<EState<'static>>>,
    /// callbacks to run when the ExprContext is shut down or rescanned.
    pub ecxt_callbacks: Vec<ExprContextCb>,
}

// ---------------------------------------------------------------------------
// Set-returning function support
// ---------------------------------------------------------------------------

/// Set-result status when evaluating functions that may return a set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExprDoneCond {
    /// expression does not return a set.
    ExprSingleResult,
    /// this result is an element of a set.
    ExprMultipleResult,
    /// there are no more elements in the set.
    ExprEndResult,
}

bitflags! {
    /// Return modes for set-returning functions. Values are distinct bits so a
    /// bitmask of supported modes can be formed.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct SetFunctionReturnMode: u8 {
        /// one value returned per call.
        const VALUE_PER_CALL        = 0x01;
        /// result set instantiated in a Tuplestore.
        const MATERIALIZE           = 0x02;
        /// Tuplestore needs randomAccess.
        const MATERIALIZE_RANDOM    = 0x04;
        /// caller prefers Tuplestore.
        const MATERIALIZE_PREFERRED = 0x08;
    }
}

impl Default for SetFunctionReturnMode {
    fn default() -> Self {
        Self::empty()
    }
}

/// Passed as `fcinfo->resultinfo` to functions that might return a set.
#[allow(deprecated)]
#[derive(Default)]
pub struct ReturnSetInfo {
    // set by caller:
    pub econtext: Option<Box<ExprContext>>,
    pub expected_desc: Option<TupleDesc>,
    /// bitmask of return modes the caller can handle.
    pub allowed_modes: i32,
    // result status from function (pre-initialized by caller):
    pub return_mode: SetFunctionReturnMode,
    pub is_done: Option<ExprDoneCond>,
    // filled by the function in Materialize mode:
    pub set_result: Option<Box<Tuplestorestate>>,
    pub set_desc: Option<TupleDesc>,
}

// ---------------------------------------------------------------------------
// ProjectionInfo / JunkFilter / conflict / merge
// ---------------------------------------------------------------------------

/// Info to perform projections (form new tuples from a targetlist).
#[allow(deprecated)]
#[derive(Default)]
pub struct ProjectionInfo {
    /// instructions to evaluate the projection.
    pub state: ExprState,
    /// expression context in which to evaluate.
    pub expr_context: Option<Box<ExprContext>>,
}

/// Stores information about junk attributes to strip from output tuples.
#[allow(deprecated)]
#[derive(Default)]
pub struct JunkFilter {
    pub target_list: Vec<Node>,
    pub clean_tup_type: Option<TupleDesc>,
    pub clean_map: Vec<AttrNumber>,
    pub result_slot: Option<Box<TupleTableSlot>>,
}

/// Executor state of an ON CONFLICT DO UPDATE operation.
#[allow(deprecated)]
#[derive(Default)]
pub struct OnConflictSetState {
    pub existing: Option<Box<TupleTableSlot>>,
    pub proj_slot: Option<Box<TupleTableSlot>>,
    pub proj_info: Option<Box<ProjectionInfo>>,
    pub where_clause: Option<Box<ExprState>>,
}

/// Executor state for a MERGE action.
#[allow(deprecated)]
#[derive(Default)]
pub struct MergeActionState {
    pub action: Option<Box<MergeAction>>,
    pub proj: Option<Box<ProjectionInfo>>,
    pub whenqual: Option<Box<ExprState>>,
}

/// Number of MergeMatchKind values (mirrors parsenodes NUM_MERGE_MATCH_KINDS).
pub const NUM_MERGE_MATCH_KINDS: usize = 3;

// ---------------------------------------------------------------------------
// ResultRelInfo
// ---------------------------------------------------------------------------

/// All information needed about a result relation, including indexes.
#[allow(deprecated)]
#[derive(Default)]
pub struct ResultRelInfo {
    /// range table index, or 0 if not in range table.
    pub range_table_index: usize,
    pub relation_desc: Option<Arc<RelationData>>,
    pub num_indices: i32,
    pub index_relation_descs: Vec<Arc<RelationData>>,
    pub index_relation_info: Vec<Box<IndexInfo>>,
    pub row_id_att_no: AttrNumber,
    pub extra_updated_cols: Option<Bitmapset>,
    pub extra_updated_cols_valid: bool,
    pub project_new: Option<Box<ProjectionInfo>>,
    pub new_tuple_slot: Option<Box<TupleTableSlot>>,
    pub old_tuple_slot: Option<Box<TupleTableSlot>>,
    pub project_new_info_valid: bool,
    pub need_lock_tag_tuple: bool,
    pub trig_desc: Option<Box<TriggerDesc>>,
    pub trig_functions: Vec<FmgrInfo>,
    pub trig_when_exprs: Vec<Box<ExprState>>,
    pub trig_instrument: Option<Box<Instrumentation>>,
    pub returning_slot: Option<Box<TupleTableSlot>>,
    pub trig_old_slot: Option<Box<TupleTableSlot>>,
    pub trig_new_slot: Option<Box<TupleTableSlot>>,
    pub all_null_slot: Option<Box<TupleTableSlot>>,
    pub fdw_routine: Option<Box<FdwRoutine>>,
    /// available to save private state of FDW (C `void *`).
    pub fdw_state: OpaqueState,
    pub uses_fdw_direct_modify: bool,
    pub num_slots: i32,
    pub num_slots_initialized: i32,
    pub batch_size: i32,
    pub slots: Vec<Box<TupleTableSlot>>,
    pub plan_slots: Vec<Box<TupleTableSlot>>,
    pub with_check_options: Vec<Node>,
    pub with_check_option_exprs: Vec<Box<ExprState>>,
    pub check_constraint_exprs: Vec<Box<ExprState>>,
    pub gen_virtual_not_null_constraint_exprs: Vec<Box<ExprState>>,
    pub generated_exprs_i: Vec<Box<ExprState>>,
    pub generated_exprs_u: Vec<Box<ExprState>>,
    pub num_generated_needed_i: i32,
    pub num_generated_needed_u: i32,
    pub returning_list: Vec<Node>,
    pub project_returning: Option<Box<ProjectionInfo>>,
    pub on_conflict_arbiter_indexes: Vec<Oid>,
    pub on_conflict: Option<Box<OnConflictSetState>>,
    /// for MERGE, one list of MergeActionState per MergeMatchKind.
    pub merge_actions: [Vec<Box<MergeActionState>>; NUM_MERGE_MATCH_KINDS],
    pub merge_join_condition: Option<Box<ExprState>>,
    pub partition_check_expr: Option<Box<ExprState>>,
    pub child_to_root_map: Option<Box<TupleConversionMap>>,
    pub child_to_root_map_valid: bool,
    pub root_to_child_map: Option<Box<TupleConversionMap>>,
    pub root_to_child_map_valid: bool,
    pub root_result_rel_info: Option<Box<Self>>,
    pub partition_tuple_slot: Option<Box<TupleTableSlot>>,
    /// for copyfrom.c multi-inserts (C `void *` to CopyMultiInsertBuffer).
    pub copy_multi_insert_buffer: OpaqueState,
    pub ancestor_result_rels: Vec<Box<Self>>,
}

/// State for an asynchronous tuple request. (Not itself a Node.)
#[allow(deprecated)]
pub struct AsyncRequest {
    pub requestor: Option<Box<PlanState>>,
    pub requestee: Option<Box<PlanState>>,
    pub request_index: i32,
    pub callback_pending: bool,
    pub request_complete: bool,
    pub result: Option<Box<TupleTableSlot>>,
}

// ---------------------------------------------------------------------------
// EState
// ---------------------------------------------------------------------------

/// The executor's open range-table relations, indexed by 0-based range-table
/// position (RT index `rti` reads slot `rti - 1`). A slot is `None` for a
/// non-RELATION RTE (e.g. the `RTE_RESULT` of `SELECT 1`). PG's `EState` holds the
/// open relations as `es_relations` (a `Arc<RelationData>*` array indexed by RTI); this is
/// the faithful borrow form: the `Arc<RelationData>` owners live in the
/// command/statement frame (the `'rel` root) and the executor BORROWS them.
pub type RangeTableRels<'rel> = &'rel [Option<&'rel crate::utils::rel::RelationData>];

/// Working state for an Executor invocation.
///
/// `'rel` is the lifetime of the open range-table relations the executor borrows
/// (`es_range_table_rels`): the `Arc<RelationData>` owners are bindings in the
/// command/statement frame that strictly enclose ExecutorStart..ExecutorEnd
/// (relation-ownership-plan §1.2). The borrow rides every scan `.await` because its
/// owner is a suspended ancestor stack frame, never a `task_local`.
#[allow(deprecated)]
pub struct EState<'rel> {
    pub direction: ScanDirection,
    pub snapshot: Snapshot,
    pub crosscheck_snapshot: Snapshot,
    /// List of RangeTblEntry.
    pub range_table: Vec<Node>,
    pub range_table_size: usize,
    /// The open relations for the range table (PG `es_relations`), borrowed from
    /// the command frame's `Arc<RelationData>` owners. `ExecGetRangeTableRelation`
    /// indexes this by RT index.
    pub es_range_table_rels: RangeTableRels<'rel>,
    /// The open index relations a scan may use, borrowed from the command frame
    /// (PG resolves these via `index_open(indexid)` against the relcache; until the
    /// relcache index-open path is wired, the command frame publishes the borrows
    /// here and `ExecOpenIndexRelation` looks one up by OID). M6 index scan.
    pub es_index_rels: &'rel [Option<&'rel crate::utils::rel::RelationData>],
    /// The query snapshot a scan reads under, borrowed from the command frame's
    /// `Arc<SnapshotData>` owner (PG's `es_snapshot`, reachable to the scan nodes).
    /// `snapshot` below keeps the owned `Arc` copy for the non-scan paths; the scan
    /// node borrows THIS so its stored descriptor does not self-reference.
    pub es_snapshot_ref: Option<&'rel crate::utils::snapshot::SnapshotData>,
    pub relations: Vec<Arc<RelationData>>,
    pub rowmarks: Vec<Box<ExecRowMark>>,
    /// List of RTEPermissionInfo.
    pub rteperminfos: Vec<Node>,
    pub plannedstmt: Option<Box<PlannedStmt>>,
    /// List of PartitionPruneInfo.
    pub part_prune_infos: Vec<Node>,
    /// List of PartitionPruneState.
    pub part_prune_states: Vec<Node>,
    /// List of Bitmapset.
    pub part_prune_results: Vec<Bitmapset>,
    pub unpruned_relids: Option<Bitmapset>,
    pub source_text: Option<String>,
    pub junk_filter: Option<Box<JunkFilter>>,
    /// command ID to mark inserted/deleted tuples with.
    pub output_cid: crate::c::CommandId,
    pub result_relations: Vec<Box<ResultRelInfo>>,
    pub opened_result_relations: Vec<Box<ResultRelInfo>>,
    pub partition_directory: PartitionDirectory,
    pub tuple_routing_result_relations: Vec<Box<ResultRelInfo>>,
    pub trig_target_relations: Vec<Box<ResultRelInfo>>,
    pub param_list_info: Option<Box<ParamListInfo>>,
    pub param_exec_vals: Option<Box<ParamExecData>>,
    pub query_env: Option<Box<QueryEnvironment>>,
    pub query_cxt: MemoryContext,
    /// List of TupleTableSlots.
    pub tuple_table: Vec<Box<TupleTableSlot>>,
    pub processed: u64,
    pub total_processed: u64,
    pub top_eflags: i32,
    /// OR of InstrumentOption flags.
    pub instrument: i32,
    pub finished: bool,
    pub exprcontexts: Vec<Box<ExprContext>>,
    /// List of PlanState for SubPlans.
    pub subplanstates: Vec<Box<PlanState>>,
    pub auxmodifytables: Vec<Node>,
    pub per_tuple_exprcontext: Option<Box<ExprContext>>,
    pub epq_active: Option<Box<EPQState>>,
    pub use_parallel_mode: bool,
    pub parallel_workers_to_launch: i32,
    pub parallel_workers_launched: i32,
    /// per-query shared memory area (C `dsa_area *`).
    pub query_dsa: OpaqueState,
    pub jit_flags: i32,
    /// JIT context, created on-demand (C `void *`).
    pub jit: OpaqueState,
    pub jit_worker_instr: OpaqueState,
    pub insert_pending_result_relations: Vec<Box<ResultRelInfo>>,
    pub insert_pending_modifytables: Vec<Node>,
}

#[allow(deprecated)]
impl Default for EState<'_> {
    /// PG `CreateExecutorState` zero-inits the struct then sets es_direction =
    /// ForwardScanDirection; the real ScanDirection enum has no Default, so the
    /// derive is hand-written to mirror that one initialized field.
    fn default() -> Self {
        Self {
            direction: ScanDirection::Forward,
            snapshot: Snapshot::default(),
            crosscheck_snapshot: Snapshot::default(),
            range_table: Vec::default(),
            range_table_size: usize::default(),
            es_range_table_rels: &[],
            es_index_rels: &[],
            es_snapshot_ref: None,
            relations: Vec::default(),
            rowmarks: Vec::default(),
            rteperminfos: Vec::default(),
            plannedstmt: Option::default(),
            part_prune_infos: Vec::default(),
            part_prune_states: Vec::default(),
            part_prune_results: Vec::default(),
            unpruned_relids: Option::default(),
            source_text: Option::default(),
            junk_filter: Option::default(),
            output_cid: crate::c::CommandId::default(),
            result_relations: Vec::default(),
            opened_result_relations: Vec::default(),
            partition_directory: PartitionDirectory,
            tuple_routing_result_relations: Vec::default(),
            trig_target_relations: Vec::default(),
            param_list_info: Option::default(),
            param_exec_vals: Option::default(),
            query_env: Option::default(),
            query_cxt: MemoryContext,
            tuple_table: Vec::default(),
            processed: u64::default(),
            total_processed: u64::default(),
            top_eflags: i32::default(),
            instrument: i32::default(),
            finished: bool::default(),
            exprcontexts: Vec::default(),
            subplanstates: Vec::default(),
            auxmodifytables: Vec::default(),
            per_tuple_exprcontext: Option::default(),
            epq_active: Option::default(),
            use_parallel_mode: bool::default(),
            parallel_workers_to_launch: i32::default(),
            parallel_workers_launched: i32::default(),
            query_dsa: OpaqueState::default(),
            jit_flags: i32::default(),
            jit: OpaqueState::default(),
            jit_worker_instr: OpaqueState::default(),
            insert_pending_result_relations: Vec::default(),
            insert_pending_modifytables: Vec::default(),
        }
    }
}

// ---------------------------------------------------------------------------
// ExecRowMark / ExecAuxRowMark (not Nodes)
// ---------------------------------------------------------------------------

/// Runtime representation of FOR [KEY] UPDATE/SHARE clauses.
#[allow(deprecated)]
pub struct ExecRowMark {
    pub relation: Arc<RelationData>,
    pub relid: Oid,
    pub rti: usize,
    pub prti: usize,
    pub rowmark_id: usize,
    pub mark_type: RowMarkType,
    pub strength: LockClauseStrength,
    pub wait_policy: LockWaitPolicy,
    pub erm_active: bool,
    pub cur_ctid: ItemPointerData,
    /// available for use by the relation source node (C `void *`).
    pub erm_extra: OpaqueState,
}

/// Per-LockRows/ModifyTable rowmark with resjunk column numbers.
#[allow(deprecated)]
pub struct ExecAuxRowMark {
    pub rowmark: Option<Box<ExecRowMark>>,
    pub ctid_att_no: AttrNumber,
    pub toid_att_no: AttrNumber,
    pub whole_att_no: AttrNumber,
}

// ---------------------------------------------------------------------------
// Tuple Hash Tables (not Nodes)
// ---------------------------------------------------------------------------

#[allow(deprecated)]
pub struct TupleHashEntryData {
    /// copy of first tuple in this group.
    pub first_tuple: MinimalTuple,
    pub status: u32,
    pub hash: u32,
}

#[allow(deprecated)]
pub struct TupleHashTableData {
    /// underlying hash table.
    pub hashtab: Box<TuplehashHash>,
    pub num_cols: i32,
    pub key_col_idx: Vec<AttrNumber>,
    pub tab_hash_expr: Option<Box<ExprState>>,
    pub tab_eq_func: Option<Box<ExprState>>,
    pub tab_collations: Vec<Oid>,
    pub tablecxt: MemoryContext,
    pub tempcxt: MemoryContext,
    pub additionalsize: usize,
    pub tableslot: Option<Box<TupleTableSlot>>,
    // set transiently for each table search:
    pub inputslot: Option<Box<TupleTableSlot>>,
    pub in_hash_expr: Option<Box<ExprState>>,
    pub cur_eq_func: Option<Box<ExprState>>,
    pub exprcontext: Option<Box<ExprContext>>,
}

#[allow(deprecated)]
pub type TupleHashEntry = Option<Box<TupleHashEntryData>>;
#[allow(deprecated)]
pub type TupleHashTable = Option<Box<TupleHashTableData>>;

// ---------------------------------------------------------------------------
// Expression State Nodes (selected expr types whose state is shared)
// ---------------------------------------------------------------------------

#[allow(deprecated)]
pub struct WindowFuncExprState {
    pub wfunc: Option<Box<WindowFunc>>,
    /// ExprStates for argument expressions.
    pub args: Vec<Box<ExprState>>,
    pub aggfilter: Option<Box<ExprState>>,
    pub wfuncno: i32,
}

/// State for evaluating a potentially set-returning expression.
#[allow(deprecated)]
pub struct SetExprState {
    pub expr: Option<Node>,
    pub args: Vec<Box<ExprState>>,
    pub elided_func_state: Option<Box<ExprState>>,
    pub func: FmgrInfo,
    pub func_result_store: Option<Box<Tuplestorestate>>,
    pub func_result_slot: Option<Box<TupleTableSlot>>,
    pub func_result_desc: Option<TupleDesc>,
    pub func_returns_tuple: bool,
    pub func_returns_set: bool,
    pub set_args_valid: bool,
    pub shutdown_reg: bool,
    pub fcinfo: FunctionCallInfo,
}

#[allow(deprecated)]
pub struct SubPlanState {
    pub subplan: Option<Box<SubPlan>>,
    pub planstate: Option<Box<PlanState>>,
    pub parent: Option<Box<PlanState>>,
    pub testexpr: Option<Box<ExprState>>,
    pub cur_tuple: HeapTuple,
    pub cur_array: Datum,
    pub desc_right: Option<TupleDesc>,
    pub proj_left: Option<Box<ProjectionInfo>>,
    pub proj_right: Option<Box<ProjectionInfo>>,
    pub hashtable: TupleHashTable,
    pub hashnulls: TupleHashTable,
    pub havehashrows: bool,
    pub havenullrows: bool,
    pub hashtablecxt: MemoryContext,
    pub hashtempcxt: MemoryContext,
    pub innerecontext: Option<Box<ExprContext>>,
    pub num_cols: i32,
    pub key_col_idx: Vec<AttrNumber>,
    pub tab_eq_funcoids: Vec<Oid>,
    pub tab_collations: Vec<Oid>,
    pub tab_hash_funcs: Vec<FmgrInfo>,
    pub lhs_hash_expr: Option<Box<ExprState>>,
    pub cur_eq_funcs: Vec<FmgrInfo>,
    pub cur_eq_comp: Option<Box<ExprState>>,
}

/// One constraint to check during CoerceToDomain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DomainConstraintType {
    NOTNULL,
    CHECK,
}

#[allow(deprecated)]
pub struct DomainConstraintState {
    pub constrainttype: DomainConstraintType,
    /// name of constraint (for error msgs).
    pub name: Option<String>,
    /// for CHECK, a boolean expression.
    pub check_expr: Option<Node>,
    pub check_exprstate: Option<Box<ExprState>>,
}

/// State for JsonExpr evaluation (too big to inline). Not itself a Node.
#[allow(deprecated)]
pub struct JsonExprState {
    pub jsexpr: Option<Box<JsonExpr>>,
    pub formatted_expr: NullableDatum,
    pub pathspec: NullableDatum,
    /// JsonPathVariable entries for passing_values.
    pub args: Vec<Box<JsonPathVariable>>,
    pub error: NullableDatum,
    pub empty: NullableDatum,
    pub jump_empty: i32,
    pub jump_error: i32,
    pub jump_eval_coercion: i32,
    pub jump_end: i32,
    pub input_fcinfo: FunctionCallInfo,
    pub escontext: ErrorSaveContext,
}

// ---------------------------------------------------------------------------
// Executor State Trees: PlanState (abstract base) + concrete *State nodes
// ---------------------------------------------------------------------------

/// Returns the next tuple from an executor node (None if no more tuples).
#[allow(deprecated)]
pub type ExecProcNodeMtd = fn(pstate: &mut PlanState) -> Option<Box<TupleTableSlot>>;

/// Common abstract superclass for all PlanState-type nodes. Never instantiated
/// directly; carries the shared structural/runtime state.
#[allow(deprecated)]
#[derive(Default)]
pub struct PlanState {
    /// associated Plan node.
    pub plan: Option<Node>,
    /// the one EState for the whole top-level plan. A back-pointer never populated
    /// on the M2 live path (the run-state wrappers carry the executable tree);
    /// pinned `'static` so `PlanState` stays lifetime-free.
    pub state: Option<Box<EState<'static>>>,
    pub exec_proc_node: Option<ExecProcNodeMtd>,
    pub exec_proc_node_real: Option<ExecProcNodeMtd>,
    pub instrument: Option<Box<Instrumentation>>,
    pub worker_instrument: Option<Box<WorkerInstrumentation>>,
    /// per-worker JIT instrumentation (C `void *`).
    pub worker_jit_instrument: OpaqueState,
    /// boolean qual condition.
    pub qual: Option<Box<ExprState>>,
    pub lefttree: Option<Box<Self>>,
    pub righttree: Option<Box<Self>>,
    /// Init SubPlanState nodes (un-correlated expr subselects).
    pub init_plan: Vec<Box<SubPlanState>>,
    /// SubPlanState nodes in my expressions.
    pub sub_plan: Vec<Box<SubPlanState>>,
    /// set of IDs of changed Params.
    pub chg_param: Option<Bitmapset>,
    pub ps_result_tuple_desc: Option<TupleDesc>,
    pub ps_result_tuple_slot: Option<Box<TupleTableSlot>>,
    pub ps_expr_context: Option<Box<ExprContext>>,
    pub ps_proj_info: Option<Box<ProjectionInfo>>,
    pub async_capable: bool,
    pub scandesc: Option<TupleDesc>,
    pub scanops: Option<&'static dyn TupleTableSlotOps>,
    pub outerops: Option<&'static dyn TupleTableSlotOps>,
    pub innerops: Option<&'static dyn TupleTableSlotOps>,
    pub resultops: Option<&'static dyn TupleTableSlotOps>,
    pub scanopsfixed: bool,
    pub outeropsfixed: bool,
    pub inneropsfixed: bool,
    pub resultopsfixed: bool,
    pub scanopsset: bool,
    pub outeropsset: bool,
    pub inneropsset: bool,
    pub resultopsset: bool,
}

/// State for an EvalPlanQual recheck (in ModifyTable or LockRows). Not a Node.
#[allow(deprecated)]
#[derive(Default)]
pub struct EPQState {
    /// back-pointer; never populated on the M2 live path, pinned `'static`.
    pub parentestate: Option<Box<EState<'static>>>,
    pub epq_param: i32,
    /// integer list of RT indexes, or empty.
    pub result_relations: Vec<i32>,
    /// tuple table for relsubs_slot.
    pub tuple_table: Vec<Box<TupleTableSlot>>,
    pub relsubs_slot: Vec<Box<TupleTableSlot>>,
    pub plan: Option<Node>,
    /// ExecAuxRowMarks (non-locking only).
    pub arow_marks: Vec<Box<ExecAuxRowMark>>,
    pub origslot: Option<Box<TupleTableSlot>>,
    pub recheckestate: Option<Box<EState<'static>>>,
    pub relsubs_rowmark: Vec<Box<ExecAuxRowMark>>,
    pub relsubs_done: Vec<bool>,
    pub relsubs_blocked: Vec<bool>,
    pub recheckplanstate: Option<Box<PlanState>>,
}

/// ResultState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct ResultState {
    pub ps: PlanState,
    pub resconstantqual: Option<Box<ExprState>>,
    pub rs_done: bool,
    pub rs_checkqual: bool,
}

bitflags! {
    /// Flags for `ModifyTableState::mt_merge_subcommands`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct MergeSubcommands: i32 {
        const INSERT = 0x01;
        const UPDATE = 0x02;
        const DELETE = 0x04;
    }
}

/// ProjectSetState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct ProjectSetState {
    pub ps: PlanState,
    /// array of expression states.
    pub elems: Vec<Node>,
    /// per-SRF is-done states.
    pub elemdone: Vec<ExprDoneCond>,
    pub nelems: i32,
    pub pending_srf_tuples: bool,
    pub argcontext: MemoryContext,
}

/// ModifyTableState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct ModifyTableState {
    pub ps: PlanState,
    pub operation: Option<CmdType>,
    pub can_set_tag: bool,
    pub mt_done: bool,
    pub mt_nrels: i32,
    pub result_rel_info: Vec<Box<ResultRelInfo>>,
    pub root_result_rel_info: Option<Box<ResultRelInfo>>,
    pub mt_epqstate: EPQState,
    pub fire_bs_triggers: bool,
    pub mt_result_oid_attno: i32,
    pub mt_last_result_oid: Oid,
    pub mt_last_result_index: i32,
    /// optional hash table to speed lookups (C `HTAB *`).
    pub mt_result_oid_hash: Option<Box<Htab>>,
    pub mt_root_tuple_slot: Option<Box<TupleTableSlot>>,
    /// tuple-routing support (C `void *`).
    pub mt_partition_tuple_routing: OpaqueState,
    pub mt_transition_capture: OpaqueState,
    pub mt_oc_transition_capture: OpaqueState,
    pub mt_merge_subcommands: i32,
    pub mt_merge_action: Option<Box<MergeActionState>>,
    pub mt_merge_pending_not_matched: Option<Box<TupleTableSlot>>,
    pub mt_merge_inserted: f64,
    pub mt_merge_updated: f64,
    pub mt_merge_deleted: f64,
    pub mt_update_colnos_lists: Vec<Node>,
    pub mt_merge_action_lists: Vec<Node>,
    pub mt_merge_join_conditions: Vec<Node>,
}

/// Chooses the next subplan for an Append node.
#[allow(deprecated)]
pub type ChooseNextSubplanFn = fn(state: &mut AppendState) -> bool;

/// AppendState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct AppendState {
    pub ps: PlanState,
    pub appendplans: Vec<Box<PlanState>>,
    pub as_nplans: i32,
    pub as_whichplan: i32,
    pub as_begun: bool,
    pub as_asyncplans: Option<Bitmapset>,
    pub as_nasyncplans: i32,
    pub as_asyncrequests: Vec<Box<AsyncRequest>>,
    pub as_asyncresults: Vec<Box<TupleTableSlot>>,
    pub as_nasyncresults: i32,
    pub as_syncdone: bool,
    pub as_nasyncremain: i32,
    pub as_needrequest: Option<Bitmapset>,
    /// WaitEventSet to configure fd wait events (C `void *`).
    pub as_eventset: OpaqueState,
    pub as_first_partial_plan: i32,
    /// parallel coordination info (C `void *`).
    pub as_pstate: OpaqueState,
    pub pstate_len: usize,
    /// PartitionPruneState (C `void *`).
    pub as_prune_state: OpaqueState,
    pub as_valid_subplans_identified: bool,
    pub as_valid_subplans: Option<Bitmapset>,
    pub as_valid_asyncplans: Option<Bitmapset>,
    pub choose_next_subplan: Option<ChooseNextSubplanFn>,
}

/// MergeAppendState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct MergeAppendState {
    pub ps: PlanState,
    pub mergeplans: Vec<Box<PlanState>>,
    pub ms_nplans: i32,
    pub ms_nkeys: i32,
    pub ms_sortkeys: Vec<SortSupport>,
    pub ms_slots: Vec<Box<TupleTableSlot>>,
    /// binary heap of slot indices (C `void *`).
    pub ms_heap: OpaqueState,
    pub ms_initialized: bool,
    pub ms_prune_state: OpaqueState,
    pub ms_valid_subplans: Option<Bitmapset>,
}

/// RecursiveUnionState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct RecursiveUnionState {
    pub ps: PlanState,
    pub recursing: bool,
    pub intermediate_empty: bool,
    pub working_table: Option<Box<Tuplestorestate>>,
    pub intermediate_table: Option<Box<Tuplestorestate>>,
    pub eqfuncoids: Vec<Oid>,
    pub hashfunctions: Vec<FmgrInfo>,
    pub temp_context: MemoryContext,
    pub hashtable: TupleHashTable,
    pub table_context: MemoryContext,
}

/// BitmapAndState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct BitmapAndState {
    pub ps: PlanState,
    pub bitmapplans: Vec<Box<PlanState>>,
    pub nplans: i32,
}

/// BitmapOrState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct BitmapOrState {
    pub ps: PlanState,
    pub bitmapplans: Vec<Box<PlanState>>,
    pub nplans: i32,
}

// ----- Scan State -----

/// ScanState extends PlanState for node types that scan a relation.
#[allow(deprecated)]
#[derive(Default)]
pub struct ScanState {
    pub ps: PlanState,
    pub ss_current_relation: Option<Arc<RelationData>>,
    pub ss_current_scan_desc: Option<Box<TableScanDescData>>,
    pub ss_scan_tuple_slot: Option<Box<TupleTableSlot>>,
}

/// SeqScanState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct SeqScanState {
    pub ss: ScanState,
    pub pscan_len: usize,
}

/// SampleScanState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct SampleScanState {
    pub ss: ScanState,
    /// expr states for TABLESAMPLE params.
    pub args: Vec<Box<ExprState>>,
    pub repeatable: Option<Box<ExprState>>,
    /// tablesample method descriptor (C `void *`).
    pub tsmroutine: OpaqueState,
    /// tablesample method state (C `void *`).
    pub tsm_state: OpaqueState,
    pub use_bulkread: bool,
    pub use_pagemode: bool,
    pub begun: bool,
    pub seed: u32,
    pub donetuples: i64,
    pub haveblock: bool,
    pub done: bool,
}

/// Index qual with a non-constant right-hand side (runtime key).
#[allow(deprecated)]
pub struct IndexRuntimeKeyInfo {
    pub scan_key: Option<Box<ScanKeyData>>,
    pub key_expr: Option<Box<ExprState>>,
    pub key_toastable: bool,
}

/// Index qual derived from a ScalarArrayOpExpr (array key).
#[allow(deprecated)]
pub struct IndexArrayKeyInfo {
    pub scan_key: Option<Box<ScanKeyData>>,
    pub array_expr: Option<Box<ExprState>>,
    pub next_elem: i32,
    pub num_elems: i32,
    pub elem_values: Vec<Datum>,
    pub elem_nulls: Vec<bool>,
}

/// IndexScanState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct IndexScanState {
    pub ss: ScanState,
    pub indexqualorig: Option<Box<ExprState>>,
    pub indexorderbyorig: Vec<Node>,
    pub iss_scan_keys: Vec<ScanKeyData>,
    pub iss_num_scan_keys: i32,
    pub iss_order_by_keys: Vec<ScanKeyData>,
    pub iss_num_order_by_keys: i32,
    pub iss_runtime_keys: Vec<IndexRuntimeKeyInfo>,
    pub iss_num_runtime_keys: i32,
    pub iss_runtime_keys_ready: bool,
    pub iss_runtime_context: Option<Box<ExprContext>>,
    pub iss_relation_desc: Option<Arc<RelationData>>,
    pub iss_scan_desc: Option<Box<IndexScanDescData>>,
    pub iss_instrument: IndexScanInstrumentation,
    pub iss_shared_info: Option<Box<SharedIndexScanInstrumentation>>,
    /// tuples needing reordering due to recheck (C `pairingheap *`).
    pub iss_reorder_queue: Option<Box<Pairingheap>>,
    pub iss_reached_end: bool,
    pub iss_order_by_values: Vec<Datum>,
    pub iss_order_by_nulls: Vec<bool>,
    pub iss_sort_support: SortSupport,
    pub iss_order_by_typ_by_vals: Vec<bool>,
    pub iss_order_by_typ_lens: Vec<i16>,
    pub iss_pscan_len: usize,
}

/// IndexOnlyScanState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct IndexOnlyScanState {
    pub ss: ScanState,
    pub recheckqual: Option<Box<ExprState>>,
    pub ioss_scan_keys: Vec<ScanKeyData>,
    pub ioss_num_scan_keys: i32,
    pub ioss_order_by_keys: Vec<ScanKeyData>,
    pub ioss_num_order_by_keys: i32,
    pub ioss_runtime_keys: Vec<IndexRuntimeKeyInfo>,
    pub ioss_num_runtime_keys: i32,
    pub ioss_runtime_keys_ready: bool,
    pub ioss_runtime_context: Option<Box<ExprContext>>,
    pub ioss_relation_desc: Option<Arc<RelationData>>,
    pub ioss_scan_desc: Option<Box<IndexScanDescData>>,
    pub ioss_instrument: IndexScanInstrumentation,
    pub ioss_shared_info: Option<Box<SharedIndexScanInstrumentation>>,
    pub ioss_table_slot: Option<Box<TupleTableSlot>>,
    pub ioss_vm_buffer: Buffer,
    pub ioss_pscan_len: usize,
    pub ioss_name_cstring_att_nums: Vec<AttrNumber>,
    pub ioss_name_cstring_count: i32,
}

/// BitmapIndexScanState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct BitmapIndexScanState {
    pub ss: ScanState,
    pub biss_result: Option<Box<TIDBitmap>>,
    pub biss_scan_keys: Vec<ScanKeyData>,
    pub biss_num_scan_keys: i32,
    pub biss_runtime_keys: Vec<IndexRuntimeKeyInfo>,
    pub biss_num_runtime_keys: i32,
    pub biss_array_keys: Vec<IndexArrayKeyInfo>,
    pub biss_num_array_keys: i32,
    pub biss_runtime_keys_ready: bool,
    pub biss_runtime_context: Option<Box<ExprContext>>,
    pub biss_relation_desc: Option<Arc<RelationData>>,
    pub biss_scan_desc: Option<Box<IndexScanDescData>>,
    pub biss_instrument: IndexScanInstrumentation,
    pub biss_shared_info: Option<Box<SharedIndexScanInstrumentation>>,
}

/// Instrumentation for a bitmap heap scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BitmapHeapScanInstrumentation {
    pub exact_pages: u64,
    pub lossy_pages: u64,
}

/// Shared-state phase of a parallel bitmap heap scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedBitmapState {
    INITIAL,
    INPROGRESS,
    FINISHED,
}

/// Parallel coordination state for a bitmap heap scan. (Not a Node.)
#[allow(deprecated)]
pub struct ParallelBitmapHeapState {
    /// dsa_pointer to the shared iterator.
    pub tbmiterator: usize,
    /// mutual exclusion for state (was slock_t).
    pub mutex: parking_lot::Mutex<()>,
    pub state: SharedBitmapState,
    pub cv: ConditionVariable,
}

/// Per-worker bitmap heap scan instrumentation (shared).
#[allow(deprecated)]
pub struct SharedBitmapHeapInstrumentation {
    pub num_workers: i32,
    pub sinstrument: Vec<BitmapHeapScanInstrumentation>,
}

/// BitmapHeapScanState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct BitmapHeapScanState {
    pub ss: ScanState,
    pub bitmapqualorig: Option<Box<ExprState>>,
    pub tbm: Option<Box<TIDBitmap>>,
    pub stats: BitmapHeapScanInstrumentation,
    pub initialized: bool,
    /// shared state for parallel bitmap scan (C `void *`).
    pub pstate: OpaqueState,
    /// statistics for parallel workers (C `void *`).
    pub sinstrument: OpaqueState,
    pub recheck: bool,
}

/// TidScanState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct TidScanState {
    pub ss: ScanState,
    pub tss_tidexprs: Vec<Node>,
    pub tss_is_current_of: bool,
    pub tss_num_tids: i32,
    pub tss_tid_ptr: i32,
    pub tss_tid_list: Vec<ItemPointerData>,
}

/// TidRangeScanState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct TidRangeScanState {
    pub ss: ScanState,
    pub trss_tidexprs: Vec<Node>,
    pub trss_mintid: ItemPointerData,
    pub trss_maxtid: ItemPointerData,
    pub trss_in_scan: bool,
}

/// SubqueryScanState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct SubqueryScanState {
    pub ss: ScanState,
    pub subplan: Option<Box<PlanState>>,
}

/// FunctionScanState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct FunctionScanState {
    pub ss: ScanState,
    pub eflags: i32,
    pub ordinality: bool,
    pub simple: bool,
    pub ordinal: i64,
    pub nfuncs: i32,
    /// per-function execution states (C `void *`, private to nodeFunctionscan.c).
    pub funcstates: OpaqueState,
    pub argcontext: MemoryContext,
}

/// ValuesScanState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct ValuesScanState {
    pub ss: ScanState,
    pub rowcontext: Option<Box<ExprContext>>,
    /// array of expression lists being evaluated.
    pub exprlists: Vec<Vec<Node>>,
    /// array of expression state lists (for SubPlans only).
    pub exprstatelists: Vec<Vec<Node>>,
    pub array_len: i32,
    pub curr_idx: i32,
}

/// TableFuncScanState node (XMLTABLE etc).
#[allow(deprecated)]
#[derive(Default)]
pub struct TableFuncScanState {
    pub ss: ScanState,
    pub docexpr: Option<Box<ExprState>>,
    pub rowexpr: Option<Box<ExprState>>,
    pub colexprs: Vec<Node>,
    pub coldefexprs: Vec<Node>,
    pub colvalexprs: Vec<Node>,
    pub passingvalexprs: Vec<Node>,
    pub ns_names: Vec<Node>,
    pub ns_uris: Vec<Node>,
    pub notnulls: Option<Bitmapset>,
    /// table builder private space (C `void *`).
    pub opaque: OpaqueState,
    /// table builder methods (C `void *`).
    pub routine: OpaqueState,
    pub in_functions: Vec<FmgrInfo>,
    pub typioparams: Vec<Oid>,
    pub ordinal: i64,
    pub per_table_cxt: MemoryContext,
    pub tupstore: Option<Box<Tuplestorestate>>,
}

/// CteScanState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct CteScanState {
    pub ss: ScanState,
    pub eflags: i32,
    pub readptr: i32,
    pub cteplanstate: Option<Box<PlanState>>,
    pub leader: Option<Box<Self>>,
    pub cte_table: Option<Box<Tuplestorestate>>,
    pub eof_cte: bool,
}

/// NamedTuplestoreScanState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct NamedTuplestoreScanState {
    pub ss: ScanState,
    pub readptr: i32,
    pub tupdesc: Option<TupleDesc>,
    pub relation: Option<Box<Tuplestorestate>>,
}

/// WorkTableScanState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct WorkTableScanState {
    pub ss: ScanState,
    pub rustate: Option<Box<RecursiveUnionState>>,
}

/// ForeignScanState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct ForeignScanState {
    pub ss: ScanState,
    pub fdw_recheck_quals: Option<Box<ExprState>>,
    pub pscan_len: usize,
    pub result_rel_info: Option<Box<ResultRelInfo>>,
    pub fdwroutine: Option<Box<FdwRoutine>>,
    /// FDW private state (C `void *`).
    pub fdw_state: OpaqueState,
}

/// CustomScanState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct CustomScanState {
    pub ss: ScanState,
    /// mask of CUSTOMPATH_* flags (nodes/extensible.h).
    pub flags: u32,
    /// list of child PlanState nodes, if any.
    pub custom_ps: Vec<Box<PlanState>>,
    pub pscan_len: usize,
    /// custom exec methods (C `void *`).
    pub methods: OpaqueState,
    pub slot_ops: Option<&'static dyn TupleTableSlotOps>,
}

// ----- Join State -----

/// Superclass for state nodes of join plans.
#[allow(deprecated)]
#[derive(Default)]
pub struct JoinState {
    pub ps: PlanState,
    pub jointype: Option<JoinType>,
    pub single_match: bool,
    /// JOIN quals (in addition to ps.qual).
    pub joinqual: Option<Box<ExprState>>,
}

/// NestLoopState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct NestLoopState {
    pub js: JoinState,
    pub nl_need_new_outer: bool,
    pub nl_matched_outer: bool,
    pub nl_null_inner_tuple_slot: Option<Box<TupleTableSlot>>,
}

/// MergeJoinState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct MergeJoinState {
    pub js: JoinState,
    pub mj_num_clauses: i32,
    /// array of mergejoinable clauses (C `void *`, private to nodeMergejoin.c).
    pub mj_clauses: OpaqueState,
    pub mj_join_state: i32,
    pub mj_skip_mark_restore: bool,
    pub mj_extra_marks: bool,
    pub mj_const_false_join: bool,
    pub mj_fill_outer: bool,
    pub mj_fill_inner: bool,
    pub mj_matched_outer: bool,
    pub mj_matched_inner: bool,
    pub mj_outer_tuple_slot: Option<Box<TupleTableSlot>>,
    pub mj_inner_tuple_slot: Option<Box<TupleTableSlot>>,
    pub mj_marked_tuple_slot: Option<Box<TupleTableSlot>>,
    pub mj_null_outer_tuple_slot: Option<Box<TupleTableSlot>>,
    pub mj_null_inner_tuple_slot: Option<Box<TupleTableSlot>>,
    pub mj_outer_econtext: Option<Box<ExprContext>>,
    pub mj_inner_econtext: Option<Box<ExprContext>>,
}

/// HashJoinState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct HashJoinState {
    pub js: JoinState,
    pub hashclauses: Option<Box<ExprState>>,
    pub hj_outer_hash: Option<Box<ExprState>>,
    /// hash table for the hashjoin (C `void *`, private to executor/hashjoin.h).
    pub hj_hash_table: OpaqueState,
    pub hj_cur_hash_value: u32,
    pub hj_cur_bucket_no: i32,
    pub hj_cur_skew_bucket_no: i32,
    /// last inner tuple matched (C `void *`).
    pub hj_cur_tuple: OpaqueState,
    pub hj_outer_tuple_slot: Option<Box<TupleTableSlot>>,
    pub hj_hash_tuple_slot: Option<Box<TupleTableSlot>>,
    pub hj_null_outer_tuple_slot: Option<Box<TupleTableSlot>>,
    pub hj_null_inner_tuple_slot: Option<Box<TupleTableSlot>>,
    pub hj_first_outer_tuple_slot: Option<Box<TupleTableSlot>>,
    pub hj_join_state: i32,
    pub hj_matched_outer: bool,
    pub hj_outer_not_empty: bool,
}

// ----- Materialization State -----

/// MaterialState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct MaterialState {
    pub ss: ScanState,
    pub eflags: i32,
    pub eof_underlying: bool,
    pub tuplestorestate: Option<Box<Tuplestorestate>>,
}

/// Per-worker memoize instrumentation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MemoizeInstrumentation {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_evictions: u64,
    pub cache_overflows: u64,
    pub mem_peak: u64,
}

/// Shared-memory container for per-worker memoize info.
pub struct SharedMemoizeInfo {
    pub num_workers: i32,
    pub sinstrument: Vec<MemoizeInstrumentation>,
}

/// MemoizeState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct MemoizeState {
    pub ss: ScanState,
    pub mstatus: i32,
    pub nkeys: i32,
    /// hash table for cache entries (C `void *`).
    pub hashtable: OpaqueState,
    pub hashkeydesc: Option<TupleDesc>,
    pub tableslot: Option<Box<TupleTableSlot>>,
    pub probeslot: Option<Box<TupleTableSlot>>,
    pub cache_eq_expr: Option<Box<ExprState>>,
    pub param_exprs: Vec<Box<ExprState>>,
    pub hashfunctions: Vec<FmgrInfo>,
    pub collations: Vec<Oid>,
    pub mem_used: u64,
    pub mem_limit: u64,
    pub table_context: MemoryContext,
    /// least recently used entry list (C dlist).
    pub lru_list: DlistHead,
    /// last tuple returned during a cache hit (C `void *`).
    pub last_tuple: OpaqueState,
    /// entry that last_tuple belongs to (C `void *`).
    pub entry: OpaqueState,
    pub singlerow: bool,
    pub binary_mode: bool,
    pub stats: MemoizeInstrumentation,
    pub shared_info: Option<Box<SharedMemoizeInfo>>,
    pub keyparamids: Option<Bitmapset>,
}

/// Information about one presorted key (sort prefix already ordered).
#[allow(deprecated)]
pub struct PresortedKeyData {
    pub flinfo: FmgrInfo,
    pub fcinfo: FunctionCallInfo,
    pub attno: OffsetNumber,
}

/// Shared-memory container for per-worker sort info.
#[allow(deprecated)]
pub struct SharedSortInfo {
    pub num_workers: i32,
    pub sinstrument: Vec<TuplesortInstrumentation>,
}

/// SortState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct SortState {
    pub ss: ScanState,
    pub random_access: bool,
    pub bounded: bool,
    pub bound: i64,
    pub sort_done: bool,
    pub bounded_done: bool,
    pub bound_done: i64,
    /// private state of tuplesort.c (C `void *`).
    pub tuplesortstate: OpaqueState,
    pub am_worker: bool,
    pub datum_sort: bool,
    pub shared_info: Option<Box<SharedSortInfo>>,
}

/// Instrumentation for IncrementalSort.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct IncrementalSortGroupInfo {
    pub group_count: i64,
    pub max_disk_space_used: i64,
    pub total_disk_space_used: i64,
    pub max_memory_space_used: i64,
    pub total_memory_space_used: i64,
    /// bitmask of TuplesortMethod.
    pub sort_methods: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct IncrementalSortInfo {
    pub fullsort_group_info: IncrementalSortGroupInfo,
    pub prefixsort_group_info: IncrementalSortGroupInfo,
}

/// Shared-memory container for per-worker incremental sort info.
pub struct SharedIncrementalSortInfo {
    pub num_workers: i32,
    pub sinfo: Vec<IncrementalSortInfo>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncrementalSortExecutionStatus {
    LOADFULLSORT,
    LOADPREFIXSORT,
    READFULLSORT,
    READPREFIXSORT,
}

/// IncrementalSortState information.
#[allow(deprecated)]
pub struct IncrementalSortState {
    pub ss: ScanState,
    pub bounded: bool,
    pub bound: i64,
    pub outer_node_done: bool,
    pub bound_done: i64,
    pub execution_status: IncrementalSortExecutionStatus,
    pub n_fullsort_remaining: i64,
    /// private state of tuplesort.c (C `void *`).
    pub fullsort_state: OpaqueState,
    pub prefixsort_state: OpaqueState,
    pub presorted_keys: Vec<PresortedKeyData>,
    pub incsort_info: IncrementalSortInfo,
    pub group_pivot: Option<Box<TupleTableSlot>>,
    pub transfer_tuple: Option<Box<TupleTableSlot>>,
    pub am_worker: bool,
    pub shared_info: Option<Box<SharedIncrementalSortInfo>>,
}

/// GroupState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct GroupState {
    pub ss: ScanState,
    pub eqfunction: Option<Box<ExprState>>,
    pub grp_done: bool,
}

/// Per-worker aggregate instrumentation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AggregateInstrumentation {
    pub hash_mem_peak: usize,
    pub hash_disk_used: u64,
    pub hash_batches_used: i32,
}

/// Shared-memory container for per-worker aggregate info.
pub struct SharedAggInfo {
    pub num_workers: i32,
    pub sinstrument: Vec<AggregateInstrumentation>,
}

/// AggState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct AggState {
    pub ss: ScanState,
    /// all Aggref nodes in targetlist & quals.
    pub aggs: Vec<Node>,
    pub numaggs: i32,
    pub numtrans: i32,
    pub aggstrategy: Option<AggStrategy>,
    pub aggsplit: Option<AggSplit>,
    /// pointer to current phase data (C `void *`, private to nodeAgg.c).
    pub phase: OpaqueState,
    pub numphases: i32,
    pub current_phase: i32,
    /// per-Aggref information (C `void *`).
    pub peragg: OpaqueState,
    /// per-Trans state information (C `void *`).
    pub pertrans: OpaqueState,
    pub hashcontext: Option<Box<ExprContext>>,
    pub aggcontexts: Vec<Box<ExprContext>>,
    pub tmpcontext: Option<Box<ExprContext>>,
    pub curaggcontext: Option<Box<ExprContext>>,
    /// currently active aggregate (C `void *`).
    pub curperagg: OpaqueState,
    /// currently active trans state (C `void *`).
    pub curpertrans: OpaqueState,
    pub input_done: bool,
    pub agg_done: bool,
    pub projected_set: i32,
    pub current_set: i32,
    pub grouped_cols: Option<Bitmapset>,
    pub all_grouped_cols: Vec<Node>,
    pub colnos_needed: Option<Bitmapset>,
    pub max_colno_needed: i32,
    pub all_cols_needed: bool,
    pub maxsets: i32,
    /// array of all phases (C `void *`).
    pub phases: OpaqueState,
    /// sorted input to phases > 1 (C `void *`).
    pub sort_in: OpaqueState,
    pub sort_out: OpaqueState,
    pub sort_slot: Option<Box<TupleTableSlot>>,
    /// grouping-set indexed array of per-group pointers (C `void *`).
    pub pergroups: OpaqueState,
    pub grp_first_tuple: HeapTuple,
    pub table_filled: bool,
    pub num_hashes: i32,
    pub hash_metacxt: MemoryContext,
    pub hash_tablecxt: MemoryContext,
    /// tape set for hash spill tapes (C `void *`).
    pub hash_tapeset: OpaqueState,
    /// HashAggSpill per grouping set (C `void *`).
    pub hash_spills: OpaqueState,
    pub hash_spill_rslot: Option<Box<TupleTableSlot>>,
    pub hash_spill_wslot: Option<Box<TupleTableSlot>>,
    pub hash_batches: Vec<Node>,
    pub hash_ever_spilled: bool,
    pub hash_spill_mode: bool,
    pub hash_mem_limit: usize,
    pub hash_ngroups_limit: u64,
    pub hash_planned_partitions: i32,
    pub hashentrysize: f64,
    pub hash_mem_peak: usize,
    pub hash_ngroups_current: u64,
    pub hash_disk_used: u64,
    pub hash_batches_used: i32,
    /// array of per-hashtable data (C `void *`).
    pub perhash: OpaqueState,
    /// grouping-set indexed array of per-group pointers (C `void *`).
    pub hash_pergroup: OpaqueState,
    pub all_pergroups: OpaqueState,
    pub shared_info: Option<Box<SharedAggInfo>>,
}

/// Run status of a WindowAggState.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowAggStatus {
    DONE,
    RUN,
    PASSTHROUGH,
    PASSTHROUGH_STRICT,
}

/// WindowAggState information.
#[allow(deprecated)]
pub struct WindowAggState {
    pub ss: ScanState,
    /// all WindowFunc nodes in targetlist.
    pub funcs: Vec<Node>,
    pub numfuncs: i32,
    pub numaggs: i32,
    /// per-window-function info (C `void *`, private to nodeWindowAgg.c).
    pub perfunc: OpaqueState,
    /// per-plain-aggregate info (C `void *`).
    pub peragg: OpaqueState,
    pub part_eqfunction: Option<Box<ExprState>>,
    pub ord_eqfunction: Option<Box<ExprState>>,
    pub buffer: Option<Box<Tuplestorestate>>,
    pub current_ptr: i32,
    pub framehead_ptr: i32,
    pub frametail_ptr: i32,
    pub grouptail_ptr: i32,
    pub spooled_rows: i64,
    pub currentpos: i64,
    pub frameheadpos: i64,
    pub frametailpos: i64,
    /// winobj for aggregate fetches (C `void *`).
    pub agg_winobj: OpaqueState,
    pub aggregatedbase: i64,
    pub aggregatedupto: i64,
    pub status: Option<WindowAggStatus>,
    pub frame_options: i32,
    pub start_offset: Option<Box<ExprState>>,
    pub end_offset: Option<Box<ExprState>>,
    pub start_offset_value: Datum,
    pub end_offset_value: Datum,
    pub start_in_range_func: FmgrInfo,
    pub end_in_range_func: FmgrInfo,
    pub in_range_coll: Oid,
    pub in_range_asc: bool,
    pub in_range_nulls_first: bool,
    pub use_pass_through: bool,
    pub top_window: bool,
    pub runcondition: Option<Box<ExprState>>,
    pub currentgroup: i64,
    pub frameheadgroup: i64,
    pub frametailgroup: i64,
    pub groupheadpos: i64,
    pub grouptailpos: i64,
    pub partcontext: MemoryContext,
    pub aggcontext: MemoryContext,
    pub curaggcontext: MemoryContext,
    pub tmpcontext: Option<Box<ExprContext>>,
    pub all_first: bool,
    pub partition_spooled: bool,
    pub next_partition: bool,
    pub more_partitions: bool,
    pub framehead_valid: bool,
    pub frametail_valid: bool,
    pub grouptail_valid: bool,
    pub first_part_slot: Option<Box<TupleTableSlot>>,
    pub framehead_slot: Option<Box<TupleTableSlot>>,
    pub frametail_slot: Option<Box<TupleTableSlot>>,
    pub agg_row_slot: Option<Box<TupleTableSlot>>,
    pub temp_slot_1: Option<Box<TupleTableSlot>>,
    pub temp_slot_2: Option<Box<TupleTableSlot>>,
}

/// UniqueState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct UniqueState {
    pub ps: PlanState,
    pub eqfunction: Option<Box<ExprState>>,
}

/// GatherState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct GatherState {
    pub ps: PlanState,
    pub initialized: bool,
    pub need_to_scan_locally: bool,
    pub tuples_needed: i64,
    pub funnel_slot: Option<Box<TupleTableSlot>>,
    /// ParallelExecutorInfo (C `void *`).
    pub pei: OpaqueState,
    pub nworkers_launched: i32,
    pub nreaders: i32,
    pub nextreader: i32,
    /// array of TupleQueueReader (C `void *`).
    pub reader: OpaqueState,
}

/// GatherMergeState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct GatherMergeState {
    pub ps: PlanState,
    pub initialized: bool,
    pub gm_initialized: bool,
    pub need_to_scan_locally: bool,
    pub tuples_needed: i64,
    pub tup_desc: Option<TupleDesc>,
    pub gm_nkeys: i32,
    pub gm_sortkeys: Vec<SortSupport>,
    /// ParallelExecutorInfo (C `void *`).
    pub pei: OpaqueState,
    pub nworkers_launched: i32,
    pub nreaders: i32,
    pub gm_slots: Vec<Box<TupleTableSlot>>,
    /// array of TupleQueueReader (C `void *`).
    pub reader: OpaqueState,
    /// nreaders tuple buffers (C `void *`).
    pub gm_tuple_buffers: OpaqueState,
    /// binary heap of slot indices (C `void *`).
    pub gm_heap: OpaqueState,
}

/// Values displayed by EXPLAIN ANALYZE for a hash node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HashInstrumentation {
    pub nbuckets: i32,
    pub nbuckets_original: i32,
    pub nbatch: i32,
    pub nbatch_original: i32,
    pub space_peak: usize,
}

/// Shared-memory container for per-worker hash info.
pub struct SharedHashInfo {
    pub num_workers: i32,
    pub hinstrument: Vec<HashInstrumentation>,
}

/// HashState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct HashState {
    pub ps: PlanState,
    /// hash table for the hashjoin (C `void *`).
    pub hashtable: OpaqueState,
    pub hash_expr: Option<Box<ExprState>>,
    pub skew_hashfunction: Option<Box<FmgrInfo>>,
    pub skew_collation: Oid,
    pub shared_info: Option<Box<SharedHashInfo>>,
    pub hinstrument: Option<Box<HashInstrumentation>>,
    /// parallel hash state (C `void *`).
    pub parallel_state: OpaqueState,
}

/// Per-input state for SetOp.
#[allow(deprecated)]
#[derive(Default)]
pub struct SetOpStatePerInput {
    pub first_tuple_slot: Option<Box<TupleTableSlot>>,
    pub num_tuples: i64,
    pub next_tuple_slot: Option<Box<TupleTableSlot>>,
    pub need_group: bool,
}

/// SetOpState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct SetOpState {
    pub ps: PlanState,
    pub setop_done: bool,
    pub num_output: i64,
    pub num_cols: i32,
    pub sort_keys: Vec<SortSupport>,
    pub left_input: SetOpStatePerInput,
    pub right_input: SetOpStatePerInput,
    pub need_init: bool,
    pub eqfuncoids: Vec<Oid>,
    pub hashfunctions: Vec<FmgrInfo>,
    pub hashtable: TupleHashTable,
    pub table_context: MemoryContext,
    pub table_filled: bool,
    pub hashiter: TupleHashIterator,
}

/// LockRowsState information.
#[allow(deprecated)]
#[derive(Default)]
pub struct LockRowsState {
    pub ps: PlanState,
    /// List of ExecAuxRowMarks.
    pub lr_arow_marks: Vec<Box<ExecAuxRowMark>>,
    pub lr_epqstate: EPQState,
}

/// State machine status for a Limit node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitStateCond {
    INITIAL,
    RESCAN,
    EMPTY,
    INWINDOW,
    WINDOWEND_TIES,
    SUBPLANEOF,
    WINDOWEND,
    WINDOWSTART,
}

/// LimitState information.
#[allow(deprecated)]
pub struct LimitState {
    pub ps: PlanState,
    pub limit_offset: Option<Box<ExprState>>,
    pub limit_count: Option<Box<ExprState>>,
    pub limit_option: LimitOption,
    pub offset: i64,
    pub count: i64,
    pub no_count: bool,
    pub lstate: LimitStateCond,
    pub position: i64,
    pub sub_slot: Option<Box<TupleTableSlot>>,
    pub eqfunction: Option<Box<ExprState>>,
    pub last_slot: Option<Box<TupleTableSlot>>,
}


