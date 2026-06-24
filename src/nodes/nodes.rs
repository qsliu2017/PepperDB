//! Translated from PostgreSQL src/include/nodes/nodes.h

use bitflags::bitflags;

/// The universal node type. PostgreSQL recovers a node's concrete type from a
/// leading `NodeTag`; in Rust the tag IS the enum discriminant, so there is no
/// separate `NodeTag`. Variants are added here by each later pass that defines a
/// node type (parse/plan/exec/path nodes), each carrying its data
/// (e.g. `SeqScan(Box<SeqScan>)`).
// TODO(node): variants filled in by the node-defining passes.
#[derive(Debug, Clone, PartialEq)]
pub enum Node {
    // from nodes/primnodes.rs
    Alias(Box<crate::nodes::primnodes::Alias>),
    RangeVar(Box<crate::nodes::primnodes::RangeVar>),
    TableFunc(Box<crate::nodes::primnodes::TableFunc>),
    IntoClause(Box<crate::nodes::primnodes::IntoClause>),
    Var(Box<crate::nodes::primnodes::Var>),
    Const(Box<crate::nodes::primnodes::Const>),
    Param(Box<crate::nodes::primnodes::Param>),
    Aggref(Box<crate::nodes::primnodes::Aggref>),
    GroupingFunc(Box<crate::nodes::primnodes::GroupingFunc>),
    WindowFunc(Box<crate::nodes::primnodes::WindowFunc>),
    WindowFuncRunCondition(Box<crate::nodes::primnodes::WindowFuncRunCondition>),
    MergeSupportFunc(Box<crate::nodes::primnodes::MergeSupportFunc>),
    SubscriptingRef(Box<crate::nodes::primnodes::SubscriptingRef>),
    FuncExpr(Box<crate::nodes::primnodes::FuncExpr>),
    NamedArgExpr(Box<crate::nodes::primnodes::NamedArgExpr>),
    OpExpr(Box<crate::nodes::primnodes::OpExpr>),
    // DistinctExpr/NullIfExpr are distinct node tags aliasing OpExpr's layout.
    DistinctExpr(Box<crate::nodes::primnodes::DistinctExpr>),
    NullIfExpr(Box<crate::nodes::primnodes::NullIfExpr>),
    ScalarArrayOpExpr(Box<crate::nodes::primnodes::ScalarArrayOpExpr>),
    BoolExpr(Box<crate::nodes::primnodes::BoolExpr>),
    SubLink(Box<crate::nodes::primnodes::SubLink>),
    SubPlan(Box<crate::nodes::primnodes::SubPlan>),
    AlternativeSubPlan(Box<crate::nodes::primnodes::AlternativeSubPlan>),
    FieldSelect(Box<crate::nodes::primnodes::FieldSelect>),
    FieldStore(Box<crate::nodes::primnodes::FieldStore>),
    RelabelType(Box<crate::nodes::primnodes::RelabelType>),
    CoerceViaIO(Box<crate::nodes::primnodes::CoerceViaIO>),
    ArrayCoerceExpr(Box<crate::nodes::primnodes::ArrayCoerceExpr>),
    ConvertRowtypeExpr(Box<crate::nodes::primnodes::ConvertRowtypeExpr>),
    CollateExpr(Box<crate::nodes::primnodes::CollateExpr>),
    CaseExpr(Box<crate::nodes::primnodes::CaseExpr>),
    CaseWhen(Box<crate::nodes::primnodes::CaseWhen>),
    CaseTestExpr(Box<crate::nodes::primnodes::CaseTestExpr>),
    ArrayExpr(Box<crate::nodes::primnodes::ArrayExpr>),
    RowExpr(Box<crate::nodes::primnodes::RowExpr>),
    RowCompareExpr(Box<crate::nodes::primnodes::RowCompareExpr>),
    CoalesceExpr(Box<crate::nodes::primnodes::CoalesceExpr>),
    MinMaxExpr(Box<crate::nodes::primnodes::MinMaxExpr>),
    SQLValueFunction(Box<crate::nodes::primnodes::SQLValueFunction>),
    XmlExpr(Box<crate::nodes::primnodes::XmlExpr>),
    JsonFormat(Box<crate::nodes::primnodes::JsonFormat>),
    JsonReturning(Box<crate::nodes::primnodes::JsonReturning>),
    JsonValueExpr(Box<crate::nodes::primnodes::JsonValueExpr>),
    JsonConstructorExpr(Box<crate::nodes::primnodes::JsonConstructorExpr>),
    JsonIsPredicate(Box<crate::nodes::primnodes::JsonIsPredicate>),
    JsonBehavior(Box<crate::nodes::primnodes::JsonBehavior>),
    JsonExpr(Box<crate::nodes::primnodes::JsonExpr>),
    JsonTablePath(Box<crate::nodes::primnodes::JsonTablePath>),
    JsonTablePathScan(Box<crate::nodes::primnodes::JsonTablePathScan>),
    JsonTableSiblingJoin(Box<crate::nodes::primnodes::JsonTableSiblingJoin>),
    NullTest(Box<crate::nodes::primnodes::NullTest>),
    BooleanTest(Box<crate::nodes::primnodes::BooleanTest>),
    MergeAction(Box<crate::nodes::primnodes::MergeAction>),
    CoerceToDomain(Box<crate::nodes::primnodes::CoerceToDomain>),
    CoerceToDomainValue(Box<crate::nodes::primnodes::CoerceToDomainValue>),
    SetToDefault(Box<crate::nodes::primnodes::SetToDefault>),
    CurrentOfExpr(Box<crate::nodes::primnodes::CurrentOfExpr>),
    NextValueExpr(Box<crate::nodes::primnodes::NextValueExpr>),
    InferenceElem(Box<crate::nodes::primnodes::InferenceElem>),
    ReturningExpr(Box<crate::nodes::primnodes::ReturningExpr>),
    TargetEntry(Box<crate::nodes::primnodes::TargetEntry>),
    RangeTblRef(Box<crate::nodes::primnodes::RangeTblRef>),
    JoinExpr(Box<crate::nodes::primnodes::JoinExpr>),
    FromExpr(Box<crate::nodes::primnodes::FromExpr>),
    OnConflictExpr(Box<crate::nodes::primnodes::OnConflictExpr>),

    // from nodes/replnodes.rs
    IdentifySystemCmd(Box<crate::nodes::replnodes::IdentifySystemCmd>),
    BaseBackupCmd(Box<crate::nodes::replnodes::BaseBackupCmd>),
    CreateReplicationSlotCmd(Box<crate::nodes::replnodes::CreateReplicationSlotCmd>),
    DropReplicationSlotCmd(Box<crate::nodes::replnodes::DropReplicationSlotCmd>),
    AlterReplicationSlotCmd(Box<crate::nodes::replnodes::AlterReplicationSlotCmd>),
    StartReplicationCmd(Box<crate::nodes::replnodes::StartReplicationCmd>),
    ReadReplicationSlotCmd(Box<crate::nodes::replnodes::ReadReplicationSlotCmd>),
    TimeLineHistoryCmd(Box<crate::nodes::replnodes::TimeLineHistoryCmd>),
    UploadManifestCmd(Box<crate::nodes::replnodes::UploadManifestCmd>),
}

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
