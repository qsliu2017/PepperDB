//! Translated from PostgreSQL src/include/partitioning/partprune.h

use crate::fmgr::FmgrInfo;
use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::execnodes::{ExprContext, ExprState, PlanState};
use crate::nodes::memnodes::MemoryContext;
use crate::nodes::nodes::Node;
use crate::nodes::pathnodes::{PlannerInfo, RelOptInfo};
use crate::partitioning::partdefs::PartitionBoundInfo;
use crate::postgres_ext::Oid;

/// Stores information needed at runtime for pruning computations related to a
/// single partitioned table.
pub struct PartitionPruneContext {
    /// Partition strategy, e.g. LIST, RANGE, HASH.
    pub strategy: u8,
    /// Number of columns in the partition key.
    pub partnatts: i32,
    /// Number of partitions in this partitioned table.
    pub nparts: i32,
    /// Partition boundary info for the partitioned table.
    pub boundinfo: PartitionBoundInfo,
    /// Collations of the partition key columns (partnatts elements).
    pub partcollation: Vec<Oid>,
    /// Comparison/hashing FmgrInfos for the partition keys (partnatts elements).
    pub partsupfunc: Vec<FmgrInfo>,
    /// Comparison/hashing FmgrInfos per pruning step and partition key.
    pub stepcmpfuncs: Vec<FmgrInfo>,
    /// Memory context holding this context's subsidiary data.
    pub ppccontext: MemoryContext,
    /// Parent plan node's PlanState during execution; None from the planner.
    pub planstate: Option<Box<PlanState>>,
    /// ExprContext to use when evaluating pruning expressions.
    pub exprcontext: Option<Box<ExprContext>>,
    /// ExprStates indexed per PruneCxtStateIdx; None when planstate is None.
    pub exprstates: Vec<ExprState>,
}

/// Computes the index into the stepcmpfuncs[] and exprstates[] arrays for step
/// `step_id` and partition key column `keyno`.
pub const fn PruneCxtStateIdx(partnatts: i32, step_id: i32, keyno: i32) -> i32 {
    partnatts * step_id + keyno
}

pub fn make_partition_pruneinfo(
    _root: &mut PlannerInfo,
    _parentrel: &RelOptInfo,
    _subpaths: Vec<Box<Node>>,
    _prunequal: Vec<Box<Node>>,
) -> i32 {
    unimplemented!()
}

pub fn prune_append_rel_partitions(_rel: &RelOptInfo) -> Bitmapset {
    unimplemented!()
}

pub fn get_matching_partitions(
    _context: &mut PartitionPruneContext,
    _pruning_steps: Vec<Box<Node>>,
) -> Bitmapset {
    unimplemented!()
}
