//! Translated from PostgreSQL src/include/executor/execPartition.h
//! POSTGRES partitioning executor interface.

#![allow(non_snake_case, non_camel_case_types, deprecated)]

use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::execnodes::{
    EState, ExprContext, ModifyTableState, PlanState, ResultRelInfo,
};
use crate::nodes::plannodes::PartitionPruneStep;
use crate::partitioning::partprune::PartitionPruneContext;
use crate::utils::memutils::MemoryContext;
use crate::utils::rel::Relation;

use crate::executor::tuptable::TupleTableSlot;

// See execPartition.c for the definitions; these are opaque private types.

/// Opaque: `PartitionDispatchData` is defined in execPartition.c.
#[deprecated(note = "TODO(struct-forward): private executor type defined in execPartition.c")]
// TODO(struct-forward)
#[derive(Debug, Default)]
pub struct PartitionDispatchData;

/// `typedef struct PartitionDispatchData *PartitionDispatch;`
pub type PartitionDispatch = Option<Box<PartitionDispatchData>>;

/// Opaque: `PartitionTupleRouting` is defined in execPartition.c.
#[deprecated(note = "TODO(struct-forward): private executor type defined in execPartition.c")]
// TODO(struct-forward)
#[derive(Debug, Default)]
pub struct PartitionTupleRouting;

pub fn ExecSetupPartitionTupleRouting(
    _estate: &mut EState,
    _rel: Relation,
) -> Box<PartitionTupleRouting> {
    unimplemented!()
}

pub fn ExecFindPartition(
    _mtstate: &mut ModifyTableState,
    _root_result_rel_info: &mut ResultRelInfo,
    _proute: &mut PartitionTupleRouting,
    _slot: &mut TupleTableSlot,
    _estate: &mut EState,
) -> *mut ResultRelInfo {
    unimplemented!()
}

pub fn ExecCleanupTupleRouting(
    _mtstate: &mut ModifyTableState,
    _proute: &mut PartitionTupleRouting,
) {
    unimplemented!()
}

/// Per-partitioned-table data for run-time pruning of partitions.
///
/// `subplan_map`/`subpart_map` carry indexes into the parent arrays (or -1);
/// `leafpart_rti_map` carries RT indexes (or 0). In-memory planner/executor
/// state - idiomatic Rust, no layout contract.
pub struct PartitionedRelPruningData {
    pub partrel: Relation,
    pub nparts: i32,
    pub subplan_map: Vec<i32>,
    pub subpart_map: Vec<i32>,
    pub leafpart_rti_map: Vec<i32>,
    pub present_parts: Option<Box<Bitmapset>>,
    /// List of PartitionPruneSteps for executor startup pruning.
    pub initial_pruning_steps: Vec<PartitionPruneStep>,
    /// List of PartitionPruneSteps for per-scan pruning.
    pub exec_pruning_steps: Vec<PartitionPruneStep>,
    pub initial_context: PartitionPruneContext,
    pub exec_context: PartitionPruneContext,
}

/// All run-time pruning info for a single partitioning hierarchy.
/// `partrelprunedata[]` is parent-before-child ordered; first entry is the
/// topmost (SQL-named) partition.
#[derive(Default)]
pub struct PartitionPruningData {
    pub partrelprunedata: Vec<PartitionedRelPruningData>,
}

/// State for plan nodes to perform run-time partition pruning.
pub struct PartitionPruneState {
    /// Standalone ExprContext to evaluate the pruning steps.
    pub econtext: Option<Box<ExprContext>>,
    /// paramids of PARAM_EXEC Params inside any partprunedata.
    pub execparamids: Option<Box<Bitmapset>>,
    /// subplan indexes that belong to no partprunedata (never pruned).
    pub other_subplans: Option<Box<Bitmapset>>,
    /// short-lived context for the pruning functions.
    pub prune_context: MemoryContext,
    pub do_initial_prune: bool,
    pub do_exec_prune: bool,
    pub num_partprunedata: i32,
    pub partprunedata: Vec<Box<PartitionPruningData>>,
}

pub fn ExecDoInitialPruning(_estate: &mut EState) {
    unimplemented!()
}

/// Returns the prune state plus the initially-valid subplans
/// (`initially_valid_subplans` out-param folded into the tuple).
pub fn ExecInitPartitionExecPruning(
    _planstate: &mut PlanState,
    _n_total_subplans: i32,
    _part_prune_index: i32,
    _relids: &Bitmapset,
) -> (Box<PartitionPruneState>, Option<Box<Bitmapset>>) {
    unimplemented!()
}

/// Returns the matching subplans; `validsubplan_rtis` out-param folded in.
pub fn ExecFindMatchingSubPlans(
    _prunestate: &mut PartitionPruneState,
    _initial_prune: bool,
) -> (Box<Bitmapset>, Option<Box<Bitmapset>>) {
    unimplemented!()
}
