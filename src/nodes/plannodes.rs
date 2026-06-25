//! Translated from PostgreSQL src/include/nodes/plannodes.h

use crate::access::attnum::AttrNumber;
use crate::access::sdir::ScanDirection;
use crate::access::stratnum::StrategyNumber;
use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::lockoptions::{LockClauseStrength, LockWaitPolicy};
use crate::nodes::nodes::{
    AggSplit, AggStrategy, Cardinality, CmdType, Cost, JoinType, LimitOption, Node, OnConflictAction,
    ParseLoc, SetOpCmd, SetOpStrategy,
};
use crate::nodes::parsenodes::TableSampleClause;
use crate::nodes::primnodes::{Index, TableFunc, Var};
use crate::postgres_ext::Oid;

/// Opaque CustomScan method table; modeled as a trait in nodes::extensible, which
/// cannot be a struct field here without a cycle, so kept opaque in this port.
#[derive(Debug, Clone, PartialEq)]
pub struct CustomScanMethods;

/// Fetch the Plan of a SubPlan: subplans[subplan.plan_id - 1].
/// (C macro `exec_subplan_get_plan`.)
pub fn exec_subplan_get_plan(stmt: &PlannedStmt, plan_id: i32) -> &Node {
    &stmt.subplans[(plan_id - 1) as usize]
}

/// The output of the planner: a Plan tree headed by PlannedStmt, holding the
/// "one time" info the executor needs. Utility statements are also wrapped here
/// (commandType == UTILITY, statement in `utility_stmt`).
#[derive(Debug, Clone, PartialEq)]
pub struct PlannedStmt {
    pub command_type: CmdType,
    pub query_id: i64,
    pub plan_id: i64,
    pub has_returning: bool,
    pub has_modifying_cte: bool,
    pub can_set_tag: bool,
    pub transient_plan: bool,
    pub depends_on_role: bool,
    pub parallel_mode_needed: bool,
    pub jit_flags: i32,
    pub plan_tree: Box<Node>,
    pub part_prune_infos: Vec<Box<Node>>,
    pub rtable: Vec<Box<Node>>,
    pub unprunable_relids: Option<Bitmapset>,
    pub perm_infos: Vec<Box<Node>>,
    pub result_relations: Vec<i32>,
    pub append_relations: Vec<Box<Node>>,
    pub subplans: Vec<Box<Node>>,
    pub rewind_plan_ids: Option<Bitmapset>,
    pub row_marks: Vec<Box<Node>>,
    pub relation_oids: Vec<Oid>,
    pub inval_items: Vec<Box<Node>>,
    pub param_exec_types: Vec<Oid>,
    pub utility_stmt: Option<Box<Node>>,
    pub stmt_location: ParseLoc,
    pub stmt_len: ParseLoc,
}

/// Common abstract superclass for all Plan-type nodes (C `Plan`). Never
/// instantiated on its own; embedded as the first field of every plan node so
/// they share its cost/structural data. The C NodeTag is dropped (it lives in
/// the `Node` enum discriminant).
#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    pub disabled_nodes: i32,
    pub startup_cost: Cost,
    pub total_cost: Cost,
    pub plan_rows: Cardinality,
    pub plan_width: i32,
    pub parallel_aware: bool,
    pub parallel_safe: bool,
    pub async_capable: bool,
    pub plan_node_id: i32,
    pub targetlist: Vec<Box<Node>>,
    pub qual: Vec<Box<Node>>,
    pub lefttree: Option<Box<Node>>,
    pub righttree: Option<Box<Node>>,
    pub init_plan: Vec<Box<Node>>,
    pub ext_param: Option<Bitmapset>,
    pub all_param: Option<Bitmapset>,
}

/// Result node: evaluate a variable-free targetlist, or project the outer plan.
/// `resconstantqual` is an optional one-time qualification test.
#[derive(Debug, Clone, PartialEq)]
pub struct Result {
    pub plan: Plan,
    pub resconstantqual: Option<Box<Node>>,
}

/// ProjectSet: apply a projection containing set-returning functions.
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectSet {
    pub plan: Plan,
}

/// ModifyTable: apply rows from the outer plan to result table(s).
#[derive(Debug, Clone, PartialEq)]
pub struct ModifyTable {
    pub plan: Plan,
    pub operation: CmdType,
    pub can_set_tag: bool,
    pub nominal_relation: Index,
    pub root_relation: Index,
    pub part_cols_updated: bool,
    pub result_relations: Vec<i32>,
    pub update_colnos_lists: Vec<Box<Node>>,
    pub with_check_option_lists: Vec<Box<Node>>,
    pub returning_old_alias: Option<String>,
    pub returning_new_alias: Option<String>,
    pub returning_lists: Vec<Box<Node>>,
    pub fdw_priv_lists: Vec<Box<Node>>,
    pub fdw_direct_modify_plans: Option<Bitmapset>,
    pub row_marks: Vec<Box<Node>>,
    pub epq_param: i32,
    pub on_conflict_action: OnConflictAction,
    pub arbiter_indexes: Vec<Oid>,
    pub on_conflict_set: Vec<Box<Node>>,
    pub on_conflict_cols: Vec<Box<Node>>,
    pub on_conflict_where: Option<Box<Node>>,
    pub excl_rel_rti: Index,
    pub excl_rel_tlist: Vec<Box<Node>>,
    pub merge_action_lists: Vec<Box<Node>>,
    pub merge_join_conditions: Vec<Box<Node>>,
}

/// Append: concatenation of the results of sub-plans.
#[derive(Debug, Clone, PartialEq)]
pub struct Append {
    pub plan: Plan,
    pub apprelids: Option<Bitmapset>,
    pub appendplans: Vec<Box<Node>>,
    pub nasyncplans: i32,
    pub first_partial_plan: i32,
    pub part_prune_index: i32,
}

/// MergeAppend: merge pre-sorted sub-plans, preserving ordering.
#[derive(Debug, Clone, PartialEq)]
pub struct MergeAppend {
    pub plan: Plan,
    pub apprelids: Option<Bitmapset>,
    pub mergeplans: Vec<Box<Node>>,
    pub num_cols: i32,
    pub sort_col_idx: Vec<AttrNumber>,
    pub sort_operators: Vec<Oid>,
    pub collations: Vec<Oid>,
    pub nulls_first: Vec<bool>,
    pub part_prune_index: i32,
}

/// RecursiveUnion: recursive union of two subplans (outer = non-recursive term,
/// inner = recursive term).
#[derive(Debug, Clone, PartialEq)]
pub struct RecursiveUnion {
    pub plan: Plan,
    pub wt_param: i32,
    pub num_cols: i32,
    pub dup_col_idx: Vec<AttrNumber>,
    pub dup_operators: Vec<Oid>,
    pub dup_collations: Vec<Oid>,
    pub num_groups: i64,
}

/// BitmapAnd: intersection of sub-plan bitmaps.
#[derive(Debug, Clone, PartialEq)]
pub struct BitmapAnd {
    pub plan: Plan,
    pub bitmapplans: Vec<Box<Node>>,
}

/// BitmapOr: union of sub-plan bitmaps.
#[derive(Debug, Clone, PartialEq)]
pub struct BitmapOr {
    pub plan: Plan,
    pub isshared: bool,
    pub bitmapplans: Vec<Box<Node>>,
}

/// Abstract base for all relation scan plan types (C `Scan`). Embedded as the
/// first field of each scan node.
#[derive(Debug, Clone, PartialEq)]
pub struct Scan {
    pub plan: Plan,
    pub scanrelid: Index,
}

/// Sequential scan node.
#[derive(Debug, Clone, PartialEq)]
pub struct SeqScan {
    pub scan: Scan,
}

/// Table sample scan node.
#[derive(Debug, Clone, PartialEq)]
pub struct SampleScan {
    pub scan: Scan,
    pub tablesample: Box<TableSampleClause>,
}

/// Index scan node.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexScan {
    pub scan: Scan,
    pub indexid: Oid,
    pub indexqual: Vec<Box<Node>>,
    pub indexqualorig: Vec<Box<Node>>,
    pub indexorderby: Vec<Box<Node>>,
    pub indexorderbyorig: Vec<Box<Node>>,
    pub indexorderbyops: Vec<Oid>,
    pub indexorderdir: ScanDirection,
}

/// Index-only scan node (data comes from the index, not the heap).
#[derive(Debug, Clone, PartialEq)]
pub struct IndexOnlyScan {
    pub scan: Scan,
    pub indexid: Oid,
    pub indexqual: Vec<Box<Node>>,
    pub recheckqual: Vec<Box<Node>>,
    pub indexorderby: Vec<Box<Node>>,
    pub indextlist: Vec<Box<Node>>,
    pub indexorderdir: ScanDirection,
}

/// Bitmap index scan node: delivers a bitmap of candidate tuple locations.
#[derive(Debug, Clone, PartialEq)]
pub struct BitmapIndexScan {
    pub scan: Scan,
    pub indexid: Oid,
    pub isshared: bool,
    pub indexqual: Vec<Box<Node>>,
    pub indexqualorig: Vec<Box<Node>>,
}

/// Bitmap heap scan node.
#[derive(Debug, Clone, PartialEq)]
pub struct BitmapHeapScan {
    pub scan: Scan,
    pub bitmapqualorig: Vec<Box<Node>>,
}

/// TID scan node (CTID = something).
#[derive(Debug, Clone, PartialEq)]
pub struct TidScan {
    pub scan: Scan,
    pub tidquals: Vec<Box<Node>>,
}

/// TID range scan node (CTID relop something).
#[derive(Debug, Clone, PartialEq)]
pub struct TidRangeScan {
    pub scan: Scan,
    pub tidrangequals: Vec<Box<Node>>,
}

/// Cached trivial_subqueryscan property; UNKNOWN = not yet determined.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubqueryScanStatus {
    UNKNOWN,
    TRIVIAL,
    NONTRIVIAL,
}

/// Subquery scan node: scans the output of a sub-query in the range table.
/// The sub-plan is stored here (not in `plan.lefttree`) to keep traversals from
/// crossing Query contexts unknowingly.
#[derive(Debug, Clone, PartialEq)]
pub struct SubqueryScan {
    pub scan: Scan,
    pub subplan: Box<Node>,
    pub scanstatus: SubqueryScanStatus,
}

/// FunctionScan node.
#[derive(Debug, Clone, PartialEq)]
pub struct FunctionScan {
    pub scan: Scan,
    pub functions: Vec<Box<Node>>,
    pub funcordinality: bool,
}

/// ValuesScan node.
#[derive(Debug, Clone, PartialEq)]
pub struct ValuesScan {
    pub scan: Scan,
    pub values_lists: Vec<Box<Node>>,
}

/// TableFunc scan node.
#[derive(Debug, Clone, PartialEq)]
pub struct TableFuncScan {
    pub scan: Scan,
    pub tablefunc: Box<TableFunc>,
}

/// CteScan node.
#[derive(Debug, Clone, PartialEq)]
pub struct CteScan {
    pub scan: Scan,
    pub cte_plan_id: i32,
    pub cte_param: i32,
}

/// NamedTuplestoreScan node.
#[derive(Debug, Clone, PartialEq)]
pub struct NamedTuplestoreScan {
    pub scan: Scan,
    pub enrname: Option<String>,
}

/// WorkTableScan node.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkTableScan {
    pub scan: Scan,
    pub wt_param: i32,
}

/// ForeignScan node (FDW-controlled exprs/private data).
#[derive(Debug, Clone, PartialEq)]
pub struct ForeignScan {
    pub scan: Scan,
    pub operation: CmdType,
    pub result_relation: Index,
    pub check_as_user: Oid,
    pub fs_server: Oid,
    pub fdw_exprs: Vec<Box<Node>>,
    pub fdw_private: Vec<Box<Node>>,
    pub fdw_scan_tlist: Vec<Box<Node>>,
    pub fdw_recheck_quals: Vec<Box<Node>>,
    pub fs_relids: Option<Bitmapset>,
    pub fs_base_relids: Option<Bitmapset>,
    pub fs_system_col: bool,
}

/// CustomScan node. `methods` references a static callback table (not copied).
#[derive(Debug, Clone, PartialEq)]
pub struct CustomScan {
    pub scan: Scan,
    pub flags: u32,
    pub custom_plans: Vec<Box<Node>>,
    pub custom_exprs: Vec<Box<Node>>,
    pub custom_private: Vec<Box<Node>>,
    pub custom_scan_tlist: Vec<Box<Node>>,
    pub custom_relids: Option<Bitmapset>,
    // TODO(ptr): borrows a process-static method table.
    pub methods: Box<CustomScanMethods>,
}

/// Abstract base for join nodes (C `Join`). Embedded as the first field of each
/// concrete join node.
#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    pub plan: Plan,
    pub jointype: JoinType,
    pub inner_unique: bool,
    pub joinqual: Vec<Box<Node>>,
}

/// Nested loop join node.
#[derive(Debug, Clone, PartialEq)]
pub struct NestLoop {
    pub join: Join,
    pub nest_params: Vec<Box<Node>>,
}

/// Param to pass from the current outer row into the inner subplan of a nestloop.
#[derive(Debug, Clone, PartialEq)]
pub struct NestLoopParam {
    pub paramno: i32,
    pub paramval: Box<Var>,
}

/// Merge join node.
#[derive(Debug, Clone, PartialEq)]
pub struct MergeJoin {
    pub join: Join,
    pub skip_mark_restore: bool,
    pub mergeclauses: Vec<Box<Node>>,
    pub merge_families: Vec<Oid>,
    pub merge_collations: Vec<Oid>,
    pub merge_reversals: Vec<bool>,
    pub merge_nulls_first: Vec<bool>,
}

/// Hash join node.
#[derive(Debug, Clone, PartialEq)]
pub struct HashJoin {
    pub join: Join,
    pub hashclauses: Vec<Box<Node>>,
    pub hashoperators: Vec<Oid>,
    pub hashcollations: Vec<Oid>,
    pub hashkeys: Vec<Box<Node>>,
}

/// Materialization node.
#[derive(Debug, Clone, PartialEq)]
pub struct Material {
    pub plan: Plan,
}

/// Memoize node.
#[derive(Debug, Clone, PartialEq)]
pub struct Memoize {
    pub plan: Plan,
    pub num_keys: i32,
    pub hash_operators: Vec<Oid>,
    pub collations: Vec<Oid>,
    pub param_exprs: Vec<Box<Node>>,
    pub singlerow: bool,
    pub binary_mode: bool,
    pub est_entries: u32,
    pub keyparamids: Option<Bitmapset>,
}

/// Sort node.
#[derive(Debug, Clone, PartialEq)]
pub struct Sort {
    pub plan: Plan,
    pub num_cols: i32,
    pub sort_col_idx: Vec<AttrNumber>,
    pub sort_operators: Vec<Oid>,
    pub collations: Vec<Oid>,
    pub nulls_first: Vec<bool>,
}

/// Incremental sort node.
#[derive(Debug, Clone, PartialEq)]
pub struct IncrementalSort {
    pub sort: Sort,
    pub n_presorted_cols: i32,
}

/// Group node (GROUP BY without aggregates; presorted input).
#[derive(Debug, Clone, PartialEq)]
pub struct Group {
    pub plan: Plan,
    pub num_cols: i32,
    pub grp_col_idx: Vec<AttrNumber>,
    pub grp_operators: Vec<Oid>,
    pub grp_collations: Vec<Oid>,
}

/// Aggregate node (plain or grouped aggregation).
#[derive(Debug, Clone, PartialEq)]
pub struct Agg {
    pub plan: Plan,
    pub aggstrategy: AggStrategy,
    pub aggsplit: AggSplit,
    pub num_cols: i32,
    pub grp_col_idx: Vec<AttrNumber>,
    pub grp_operators: Vec<Oid>,
    pub grp_collations: Vec<Oid>,
    pub num_groups: i64,
    pub transition_space: u64,
    pub agg_params: Option<Bitmapset>,
    pub grouping_sets: Vec<Box<Node>>,
    pub chain: Vec<Box<Node>>,
}

/// Window aggregate node.
#[derive(Debug, Clone, PartialEq)]
pub struct WindowAgg {
    pub plan: Plan,
    pub winname: Option<String>,
    pub winref: Index,
    pub part_num_cols: i32,
    pub part_col_idx: Vec<AttrNumber>,
    pub part_operators: Vec<Oid>,
    pub part_collations: Vec<Oid>,
    pub ord_num_cols: i32,
    pub ord_col_idx: Vec<AttrNumber>,
    pub ord_operators: Vec<Oid>,
    pub ord_collations: Vec<Oid>,
    pub frame_options: i32,
    pub start_offset: Option<Box<Node>>,
    pub end_offset: Option<Box<Node>>,
    pub run_condition: Vec<Box<Node>>,
    pub run_condition_orig: Vec<Box<Node>>,
    pub start_in_range_func: Oid,
    pub end_in_range_func: Oid,
    pub in_range_coll: Oid,
    pub in_range_asc: bool,
    pub in_range_nulls_first: bool,
    pub top_window: bool,
}

/// Unique node.
#[derive(Debug, Clone, PartialEq)]
pub struct Unique {
    pub plan: Plan,
    pub num_cols: i32,
    pub uniq_col_idx: Vec<AttrNumber>,
    pub uniq_operators: Vec<Oid>,
    pub uniq_collations: Vec<Oid>,
}

/// Gather node (parallel query).
#[derive(Debug, Clone, PartialEq)]
pub struct Gather {
    pub plan: Plan,
    pub num_workers: i32,
    pub rescan_param: i32,
    pub single_copy: bool,
    pub invisible: bool,
    pub init_param: Option<Bitmapset>,
}

/// Gather merge node.
#[derive(Debug, Clone, PartialEq)]
pub struct GatherMerge {
    pub plan: Plan,
    pub num_workers: i32,
    pub rescan_param: i32,
    pub num_cols: i32,
    pub sort_col_idx: Vec<AttrNumber>,
    pub sort_operators: Vec<Oid>,
    pub collations: Vec<Oid>,
    pub nulls_first: Vec<bool>,
    pub init_param: Option<Bitmapset>,
}

/// Hash build node.
#[derive(Debug, Clone, PartialEq)]
pub struct Hash {
    pub plan: Plan,
    pub hashkeys: Vec<Box<Node>>,
    pub skew_table: Oid,
    pub skew_column: AttrNumber,
    pub skew_inherit: bool,
    pub rows_total: Cardinality,
}

/// SetOp node.
#[derive(Debug, Clone, PartialEq)]
pub struct SetOp {
    pub plan: Plan,
    pub cmd: SetOpCmd,
    pub strategy: SetOpStrategy,
    pub num_cols: i32,
    pub cmp_col_idx: Vec<AttrNumber>,
    pub cmp_operators: Vec<Oid>,
    pub cmp_collations: Vec<Oid>,
    pub cmp_nulls_first: Vec<bool>,
    pub num_groups: i64,
}

/// LockRows node.
#[derive(Debug, Clone, PartialEq)]
pub struct LockRows {
    pub plan: Plan,
    pub row_marks: Vec<Box<Node>>,
    pub epq_param: i32,
}

/// Limit node. OFFSET/COUNT exprs yield int8.
#[derive(Debug, Clone, PartialEq)]
pub struct Limit {
    pub plan: Plan,
    pub limit_offset: Option<Box<Node>>,
    pub limit_count: Option<Box<Node>>,
    pub limit_option: LimitOption,
    pub uniq_num_cols: i32,
    pub uniq_col_idx: Vec<AttrNumber>,
    pub uniq_operators: Vec<Oid>,
    pub uniq_collations: Vec<Oid>,
}

/// Types of row-marking operations. The first four are lock strengths.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowMarkType {
    EXCLUSIVE,
    NOKEYEXCLUSIVE,
    SHARE,
    KEYSHARE,
    REFERENCE,
    COPY,
}

/// True if the mark type requires a RowShareLock (first four kinds).
pub fn row_mark_requires_row_share_lock(marktype: RowMarkType) -> bool {
    (marktype as u32) <= (RowMarkType::KEYSHARE as u32)
}

/// Plan-time representation of FOR [KEY] UPDATE/SHARE clauses.
#[derive(Debug, Clone, PartialEq)]
pub struct PlanRowMark {
    pub rti: Index,
    pub prti: Index,
    pub rowmark_id: Index,
    pub mark_type: RowMarkType,
    pub all_mark_types: i32,
    pub strength: LockClauseStrength,
    pub wait_policy: LockWaitPolicy,
    pub is_parent: bool,
}

/// PartitionPruneInfo: details to let the executor prune partitions.
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionPruneInfo {
    pub relids: Option<Bitmapset>,
    pub prune_infos: Vec<Box<Node>>,
    pub other_subplans: Option<Bitmapset>,
}

/// Pruning details for a single partitioned table (one level of partitioning).
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionedRelPruneInfo {
    pub rtindex: Index,
    pub present_parts: Option<Bitmapset>,
    pub nparts: i32,
    pub subplan_map: Vec<i32>,
    pub subpart_map: Vec<i32>,
    pub leafpart_rti_map: Vec<i32>,
    pub relid_map: Vec<Oid>,
    pub initial_pruning_steps: Vec<Box<Node>>,
    pub exec_pruning_steps: Vec<Box<Node>>,
    pub execparamids: Option<Bitmapset>,
}

/// Abstract base for partition pruning steps (no concrete nodes of this type).
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionPruneStep {
    pub step_id: i32,
}

/// Prune using a set of mutually-ANDed OpExpr clauses.
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionPruneStepOp {
    pub step: PartitionPruneStep,
    pub opstrategy: StrategyNumber,
    pub exprs: Vec<Box<Node>>,
    pub cmpfns: Vec<Box<Node>>,
    pub nullkeys: Option<Bitmapset>,
}

/// How to combine pruning results from sub-steps of a BoolExpr clause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionPruneCombineOp {
    UNION,
    INTERSECT,
}

/// Prune by combining sub-step results (for a BoolExpr clause).
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionPruneStepCombine {
    pub step: PartitionPruneStep,
    pub combine_op: PartitionPruneCombineOp,
    pub source_stepids: Vec<i32>,
}

/// Plan invalidation item: identifies a syscache entry by cache ID + hash value.
#[derive(Debug, Clone, PartialEq)]
pub struct PlanInvalItem {
    pub cache_id: i32,
    pub hash_value: u32,
}

/// Monotonic properties the planner tracks for functions. OR-able bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MonotonicFunction {
    NONE = 0,
    INCREASING = 1 << 0,
    DECREASING = 1 << 1,
    BOTH = (1 << 0) | (1 << 1),
}
