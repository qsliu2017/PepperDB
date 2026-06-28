//! Translated from PostgreSQL src/include/nodes/supportnodes.h

use crate::nodes::nodes::{Cost, JoinType, Node, Selectivity};
use crate::nodes::pathnodes::{IndexOptInfo, PlannerInfo, SpecialJoinInfo};
use crate::nodes::plannodes::MonotonicFunction;
use crate::nodes::primnodes::{FuncExpr, WindowFunc};
use crate::postgres_ext::Oid;

/// Plan-time simplification request for a target function.
#[derive(Debug, Clone, PartialEq)]
pub struct SupportRequestSimplify {
    /// Planner's infrastructure (may be NULL in some usages).
    pub root: Option<Box<PlannerInfo>>,
    /// Function call to be simplified.
    pub fcall: Box<FuncExpr>,
}

/// Selectivity-estimate request for a boolean-returning function in WHERE.
#[derive(Debug, Clone, PartialEq)]
pub struct SupportRequestSelectivity {
    // Input fields:
    pub root: Option<Box<PlannerInfo>>,
    /// Function we are inquiring about.
    pub funcid: Oid,
    /// Pre-simplified arguments to function.
    pub args: Vec<Node>,
    /// Function's input collation.
    pub inputcollid: Oid,
    /// Is this a join or restriction case?
    pub is_join: bool,
    /// If restriction, RTI of target relation.
    pub var_relid: i32,
    /// If join, outer join type.
    pub jointype: JoinType,
    /// If outer join, info about join.
    pub sjinfo: Option<Box<SpecialJoinInfo>>,
    // Output fields:
    /// Returned selectivity estimate.
    pub selectivity: Selectivity,
}

/// Execution-cost-estimate request for a target function.
#[derive(Debug, Clone, PartialEq)]
pub struct SupportRequestCost {
    // Input fields:
    pub root: Option<Box<PlannerInfo>>,
    /// Function we are inquiring about.
    pub funcid: Oid,
    /// Parse node invoking function, or NULL.
    pub node: Option<Node>,
    // Output fields:
    /// One-time cost.
    pub startup: Cost,
    /// Per-evaluation cost.
    pub per_tuple: Cost,
}

/// Output-rowcount-estimate request for a set-returning function.
#[derive(Debug, Clone, PartialEq)]
pub struct SupportRequestRows {
    // Input fields:
    pub root: Option<Box<PlannerInfo>>,
    /// Function we are inquiring about.
    pub funcid: Oid,
    /// Parse node invoking function.
    pub node: Option<Node>,
    // Output fields:
    /// Number of rows expected to be returned.
    pub rows: f64,
}

/// Request to derive a directly-indexable condition from a function call.
#[derive(Debug, Clone, PartialEq)]
pub struct SupportRequestIndexCondition {
    // Input fields:
    pub root: Option<Box<PlannerInfo>>,
    /// Function we are inquiring about.
    pub funcid: Oid,
    /// Parse node invoking function.
    pub node: Option<Node>,
    /// Index of function arg matching indexcol.
    pub indexarg: i32,
    /// Planner's info about target index.
    pub index: Option<Box<IndexOptInfo>>,
    /// Index of target index column (0-based).
    pub indexcol: i32,
    /// Index column's operator family.
    pub opfamily: Oid,
    /// Index column's collation.
    pub indexcollation: Oid,
    // Output fields:
    /// False if index condition is an exact equivalent of the function call.
    pub lossy: bool,
}

/// Request to evaluate a window function's monotonicity.
#[derive(Debug, Clone, PartialEq)]
pub struct SupportRequestWFuncMonotonic {
    // Input fields:
    /// Pointer to the window function data.
    pub window_func: Box<WindowFunc>,
    /// Pointer to the window clause data.
    pub window_clause: Box<crate::nodes::parsenodes::WindowClause>,
    // Output fields:
    pub monotonic: MonotonicFunction,
}

/// Request to optimize a WindowClause's frameOptions for a window function.
#[derive(Debug, Clone, PartialEq)]
pub struct SupportRequestOptimizeWindowClause {
    // Input fields:
    pub window_func: Box<WindowFunc>,
    pub window_clause: Box<crate::nodes::parsenodes::WindowClause>,
    // Input/Output fields:
    /// New frameOptions, or left untouched if no optimizations are possible.
    pub frame_options: i32,
}

/// Request to detect whether a call can modify a read/write expanded object.
#[derive(Debug, Clone, PartialEq)]
pub struct SupportRequestModifyInPlace {
    /// PG_PROC OID of the target function.
    pub funcid: Oid,
    /// Arguments to the function.
    pub args: Vec<Node>,
    /// ID of Param(s) representing variable.
    pub paramid: i32,
}
