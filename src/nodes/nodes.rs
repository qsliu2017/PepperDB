//! Translated from PostgreSQL src/include/nodes/nodes.h

use bitflags::bitflags;

/// The universal node type. PostgreSQL recovers a node's concrete type from a
/// leading `NodeTag`; in Rust the tag IS the enum discriminant, so there is no
/// separate `NodeTag`. Variants are added here by each later pass that defines a
/// node type (parse/plan/exec/path nodes), each carrying its data
/// (e.g. `SeqScan(Box<SeqScan>)`).
// TODO(node): variants filled in by the node-defining passes.
pub enum Node {}

// nodes/{outfuncs.c,print.c}
pub fn nodeToString(_obj: &Node) -> String {
    unimplemented!()
}

// nodes/{readfuncs.c,read.c}
pub fn stringToNode(_str: &str) -> *mut core::ffi::c_void {
    unimplemented!()
}

// nodes/copyfuncs.c
pub fn copyObjectImpl(_from: &Node) -> *mut core::ffi::c_void {
    unimplemented!()
}

// nodes/equalfuncs.c
pub fn equal(_a: &Node, _b: &Node) -> bool {
    unimplemented!()
}

/// Parse location; -1 means unknown. (C: `typedef int ParseLoc`.)
pub type ParseLoc = i32;

/// Fraction of tuples a qualifier will pass.
pub type Selectivity = f64;
/// Execution cost in page-access units.
pub type Cost = f64;
/// Estimated number of rows or other integer count.
pub type Cardinality = f64;

/// Type of operation represented by a Query or PlannedStmt.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmdType {
    CMD_UNKNOWN = 0,
    CMD_SELECT,
    CMD_UPDATE,
    CMD_INSERT,
    CMD_DELETE,
    CMD_MERGE,
    /// Utility cmds like create, destroy, copy, vacuum.
    CMD_UTILITY,
    /// Dummy command for instead-nothing rules with qual.
    CMD_NOTHING,
}

/// Types of relation joins; determines handling of unmatched tuples.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    JOIN_INNER = 0,
    JOIN_LEFT,
    JOIN_FULL,
    JOIN_RIGHT,
    JOIN_SEMI,
    JOIN_ANTI,
    JOIN_RIGHT_SEMI,
    JOIN_RIGHT_ANTI,
    JOIN_UNIQUE_OUTER,
    JOIN_UNIQUE_INNER,
}

/// C: `IS_OUTER_JOIN(jointype)`.
pub fn IS_OUTER_JOIN(jointype: JoinType) -> bool {
    matches!(
        jointype,
        JoinType::JOIN_LEFT
            | JoinType::JOIN_FULL
            | JoinType::JOIN_RIGHT
            | JoinType::JOIN_ANTI
            | JoinType::JOIN_RIGHT_ANTI
    )
}

/// Overall execution strategies for Agg plan nodes.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggStrategy {
    /// Simple agg across all input rows.
    AGG_PLAIN = 0,
    /// Grouped agg, input must be sorted.
    AGG_SORTED,
    /// Grouped agg, use internal hashtable.
    AGG_HASHED,
    /// Grouped agg, hash and sort both used.
    AGG_MIXED,
}

bitflags! {
    /// Primitive partial-aggregation options (C: `AGGSPLITOP_*`).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct AggSplitOp: i32 {
        /// Substitute combinefn for transfn.
        const COMBINE = 0x01;
        /// Skip finalfn, return state as-is.
        const SKIPFINAL = 0x02;
        /// Apply serialfn to output.
        const SERIALIZE = 0x04;
        /// Apply deserialfn to input.
        const DESERIALIZE = 0x08;
    }
}

/// Supported partial-aggregation operating modes (combinations of `AggSplitOp`).
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggSplit {
    /// Basic, non-split aggregation.
    AGGSPLIT_SIMPLE = 0,
    /// Initial phase of partial aggregation, with serialization.
    AGGSPLIT_INITIAL_SERIAL = AggSplitOp::SKIPFINAL.bits() | AggSplitOp::SERIALIZE.bits(),
    /// Final phase of partial aggregation, with deserialization.
    AGGSPLIT_FINAL_DESERIAL = AggSplitOp::COMBINE.bits() | AggSplitOp::DESERIALIZE.bits(),
}

impl AggSplit {
    fn ops(self) -> AggSplitOp {
        AggSplitOp::from_bits_truncate(self as i32)
    }
    pub fn do_combine(self) -> bool {
        self.ops().contains(AggSplitOp::COMBINE)
    }
    pub fn do_skipfinal(self) -> bool {
        self.ops().contains(AggSplitOp::SKIPFINAL)
    }
    pub fn do_serialize(self) -> bool {
        self.ops().contains(AggSplitOp::SERIALIZE)
    }
    pub fn do_deserialize(self) -> bool {
        self.ops().contains(AggSplitOp::DESERIALIZE)
    }
}

/// Overall semantics for SetOp plan nodes.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOpCmd {
    SETOPCMD_INTERSECT = 0,
    SETOPCMD_INTERSECT_ALL,
    SETOPCMD_EXCEPT,
    SETOPCMD_EXCEPT_ALL,
}

/// Execution strategies for SetOp plan nodes.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOpStrategy {
    /// Input must be sorted.
    SETOP_SORTED = 0,
    /// Use internal hashtable.
    SETOP_HASHED,
}

/// "ON CONFLICT" clause type of query.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnConflictAction {
    /// No "ON CONFLICT" clause.
    ONCONFLICT_NONE = 0,
    /// ON CONFLICT ... DO NOTHING.
    ONCONFLICT_NOTHING,
    /// ON CONFLICT ... DO UPDATE.
    ONCONFLICT_UPDATE,
}

/// LIMIT option of query.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitOption {
    /// FETCH FIRST ... ONLY.
    LIMIT_OPTION_COUNT = 0,
    /// FETCH FIRST ... WITH TIES.
    LIMIT_OPTION_WITH_TIES,
}
