//! Translation of postgres/src/include/nodes/nodes.h
//!
//! Definitions for tagged nodes. Every PostgreSQL "node" begins with a NodeTag,
//! so any node can be inspected by casting to `Node`.
//!
//! NOTE on `NodeTag`: in C the enum body is `#include "nodes/nodetags.h"`, a file
//! generated at build time by gen_node_support.pl from every node-defining header.
//! We grow this enum as node types are translated; the numeric values are
//! never stored on disk, so the exact ordering is unimportant for the port.

use crate::prelude::*;
use core::ffi::{c_int, c_void};

/// The tag carried in the first field of every node.
///
/// This mirrors the generated `nodetags.h`. Add a variant here when translating a
/// new node type. `T_Invalid` is 0, matching C.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum NodeTag {
    T_Invalid = 0,

    // ---- nodes/pg_list.h ----
    T_List,
    T_IntList,
    T_OidList,
    T_XidList,

    // ---- nodes/bitmapset.h ----
    T_Bitmapset,

    // ---- nodes/value.h ----
    T_Integer,
    T_Float,
    T_Boolean,
    T_String,
    T_BitString,

    // ---- nodes/memnodes.h: memory-context implementations ----
    T_AllocSetContext,
    T_GenerationContext,
    T_SlabContext,
    T_BumpContext,

    // ---- nodes/primnodes.h: primitive (expression) nodes ----
    T_Alias,
    T_RangeVar,
    T_TableFunc,
    T_IntoClause,
    T_Var,
    T_Const,
    T_Param,
    T_Aggref,
    T_GroupingFunc,
    T_WindowFunc,
    T_WindowFuncRunCondition,
    T_MergeSupportFunc,
    T_SubscriptingRef,
    T_FuncExpr,
    T_NamedArgExpr,
    T_OpExpr,
    T_DistinctExpr,
    T_NullIfExpr,
    T_ScalarArrayOpExpr,
    T_BoolExpr,
    T_SubLink,
    T_SubPlan,
    T_AlternativeSubPlan,
    T_FieldSelect,
    T_FieldStore,
    T_RelabelType,
    T_CoerceViaIO,
    T_ArrayCoerceExpr,
    T_ConvertRowtypeExpr,
    T_CollateExpr,
    T_CaseExpr,
    T_CaseWhen,
    T_CaseTestExpr,
    T_ArrayExpr,
    T_RowExpr,
    T_RowCompareExpr,
    T_CoalesceExpr,
    T_MinMaxExpr,
    T_SQLValueFunction,
    T_XmlExpr,
    T_JsonFormat,
    T_JsonReturning,
    T_JsonValueExpr,
    T_JsonConstructorExpr,
    T_JsonIsPredicate,
    T_JsonBehavior,
    T_JsonExpr,
    T_JsonTablePath,
    T_JsonTablePathScan,
    T_JsonTableSiblingJoin,
    T_NullTest,
    T_BooleanTest,
    T_MergeAction,
    T_CoerceToDomain,
    T_CoerceToDomainValue,
    T_SetToDefault,
    T_CurrentOfExpr,
    T_NextValueExpr,
    T_InferenceElem,
    T_ReturningExpr,
    T_TargetEntry,
    T_RangeTblRef,
    T_JoinExpr,
    T_FromExpr,
    T_OnConflictExpr,

    // ---- nodes/parsenodes.h: parse-tree / statement nodes ----
    T_Query,
    T_TypeName,
    T_ColumnRef,
    T_ParamRef,
    T_A_Expr,
    T_A_Const,
    T_TypeCast,
    T_CollateClause,
    T_RoleSpec,
    T_FuncCall,
    T_A_Star,
    T_A_Indices,
    T_A_Indirection,
    T_A_ArrayExpr,
    T_ResTarget,
    T_MultiAssignRef,
    T_SortBy,
    T_WindowDef,
    T_RangeSubselect,
    T_RangeFunction,
    T_RangeTableFunc,
    T_RangeTableFuncCol,
    T_RangeTableSample,
    T_ColumnDef,
    T_TableLikeClause,
    T_IndexElem,
    T_DefElem,
    T_LockingClause,
    T_XmlSerialize,
    T_PartitionElem,
    T_PartitionSpec,
    T_PartitionBoundSpec,
    T_PartitionRangeDatum,
    T_PartitionCmd,
    T_RangeTblEntry,
    T_RTEPermissionInfo,
    T_RangeTblFunction,
    T_TableSampleClause,
    T_WithCheckOption,
    T_SortGroupClause,
    T_GroupingSet,
    T_WindowClause,
    T_RowMarkClause,
    T_WithClause,
    T_InferClause,
    T_OnConflictClause,
    T_CTESearchClause,
    T_CTECycleClause,
    T_CommonTableExpr,
    T_MergeWhenClause,
    T_ReturningOption,
    T_ReturningClause,
    T_TriggerTransition,
    T_JsonOutput,
    T_JsonArgument,
    T_JsonFuncExpr,
    T_JsonTablePathSpec,
    T_JsonTable,
    T_JsonTableColumn,
    T_JsonKeyValue,
    T_JsonParseExpr,
    T_JsonScalarExpr,
    T_JsonSerializeExpr,
    T_JsonObjectConstructor,
    T_JsonArrayConstructor,
    T_JsonArrayQueryConstructor,
    T_JsonAggConstructor,
    T_JsonObjectAgg,
    T_JsonArrayAgg,
    T_RawStmt,
    T_InsertStmt,
    T_DeleteStmt,
    T_UpdateStmt,
    T_MergeStmt,
    T_SelectStmt,
    T_SetOperationStmt,
    T_ReturnStmt,
    T_PLAssignStmt,
    T_CreateSchemaStmt,
    T_AlterTableStmt,
    T_AlterTableCmd,
    T_ATAlterConstraint,
    T_ReplicaIdentityStmt,
    T_AlterCollationStmt,
    T_AlterDomainStmt,
    T_GrantStmt,
    T_ObjectWithArgs,
    T_AccessPriv,
    T_GrantRoleStmt,
    T_AlterDefaultPrivilegesStmt,
    T_CopyStmt,
    T_VariableSetStmt,
    T_VariableShowStmt,
    T_CreateStmt,
    T_Constraint,
    T_CreateTableSpaceStmt,
    T_DropTableSpaceStmt,
    T_AlterTableSpaceOptionsStmt,
    T_AlterTableMoveAllStmt,
    T_CreateExtensionStmt,
    T_AlterExtensionStmt,
    T_AlterExtensionContentsStmt,
    T_CreateFdwStmt,
    T_AlterFdwStmt,
    T_CreateForeignServerStmt,
    T_AlterForeignServerStmt,
    T_CreateForeignTableStmt,
    T_CreateUserMappingStmt,
    T_AlterUserMappingStmt,
    T_DropUserMappingStmt,
    T_ImportForeignSchemaStmt,
    T_CreatePolicyStmt,
    T_AlterPolicyStmt,
    T_CreateAmStmt,
    T_CreateTrigStmt,
    T_CreateEventTrigStmt,
    T_AlterEventTrigStmt,
    T_CreatePLangStmt,
    T_CreateRoleStmt,
    T_AlterRoleStmt,
    T_AlterRoleSetStmt,
    T_DropRoleStmt,
    T_CreateSeqStmt,
    T_AlterSeqStmt,
    T_DefineStmt,
    T_CreateDomainStmt,
    T_CreateOpClassStmt,
    T_CreateOpClassItem,
    T_CreateOpFamilyStmt,
    T_AlterOpFamilyStmt,
    T_DropStmt,
    T_TruncateStmt,
    T_CommentStmt,
    T_SecLabelStmt,
    T_DeclareCursorStmt,
    T_ClosePortalStmt,
    T_FetchStmt,
    T_IndexStmt,
    T_CreateStatsStmt,
    T_StatsElem,
    T_AlterStatsStmt,
    T_CreateFunctionStmt,
    T_FunctionParameter,
    T_AlterFunctionStmt,
    T_DoStmt,
    T_InlineCodeBlock,
    T_CallStmt,
    T_CallContext,
    T_RenameStmt,
    T_AlterObjectDependsStmt,
    T_AlterObjectSchemaStmt,
    T_AlterOwnerStmt,
    T_AlterOperatorStmt,
    T_AlterTypeStmt,
    T_RuleStmt,
    T_NotifyStmt,
    T_ListenStmt,
    T_UnlistenStmt,
    T_TransactionStmt,
    T_CompositeTypeStmt,
    T_CreateEnumStmt,
    T_CreateRangeStmt,
    T_AlterEnumStmt,
    T_ViewStmt,
    T_LoadStmt,
    T_CreatedbStmt,
    T_AlterDatabaseStmt,
    T_AlterDatabaseRefreshCollStmt,
    T_AlterDatabaseSetStmt,
    T_DropdbStmt,
    T_AlterSystemStmt,
    T_ClusterStmt,
    T_VacuumStmt,
    T_VacuumRelation,
    T_ExplainStmt,
    T_CreateTableAsStmt,
    T_RefreshMatViewStmt,
    T_CheckPointStmt,
    T_DiscardStmt,
    T_LockStmt,
    T_ConstraintsSetStmt,
    T_ReindexStmt,
    T_CreateConversionStmt,
    T_CreateCastStmt,
    T_CreateTransformStmt,
    T_PrepareStmt,
    T_ExecuteStmt,
    T_DeallocateStmt,
    T_DropOwnedStmt,
    T_ReassignOwnedStmt,
    T_AlterTSDictionaryStmt,
    T_AlterTSConfigurationStmt,
    T_PublicationTable,
    T_PublicationObjSpec,
    T_CreatePublicationStmt,
    T_AlterPublicationStmt,
    T_CreateSubscriptionStmt,
    T_AlterSubscriptionStmt,
    T_DropSubscriptionStmt,

    // ---- nodes/plannodes.h: executor plan nodes ----
    T_PlannedStmt,
    T_Plan,
    T_Result,
    T_ProjectSet,
    T_ModifyTable,
    T_Append,
    T_MergeAppend,
    T_RecursiveUnion,
    T_BitmapAnd,
    T_BitmapOr,
    T_Scan,
    T_SeqScan,
    T_SampleScan,
    T_IndexScan,
    T_IndexOnlyScan,
    T_BitmapIndexScan,
    T_BitmapHeapScan,
    T_TidScan,
    T_TidRangeScan,
    T_SubqueryScan,
    T_FunctionScan,
    T_ValuesScan,
    T_TableFuncScan,
    T_CteScan,
    T_NamedTuplestoreScan,
    T_WorkTableScan,
    T_ForeignScan,
    T_CustomScan,
    T_Join,
    T_NestLoop,
    T_NestLoopParam,
    T_MergeJoin,
    T_HashJoin,
    T_Material,
    T_Memoize,
    T_Sort,
    T_IncrementalSort,
    T_Group,
    T_Agg,
    T_WindowAgg,
    T_Unique,
    T_Gather,
    T_GatherMerge,
    T_Hash,
    T_SetOp,
    T_LockRows,
    T_Limit,
    T_PlanRowMark,
    T_PartitionPruneInfo,
    T_PartitionedRelPruneInfo,
    T_PartitionPruneStep,
    T_PartitionPruneStepOp,
    T_PartitionPruneStepCombine,
    T_PlanInvalItem,

    // ---- nodes/pathnodes.h: planner internal nodes ----
    T_PlannerGlobal,
    T_PlannerInfo,
    T_RelOptInfo,
    T_IndexOptInfo,
    T_ForeignKeyOptInfo,
    T_StatisticExtInfo,
    T_JoinDomain,
    T_EquivalenceClass,
    T_EquivalenceMember,
    T_PathKey,
    T_GroupByOrdering,
    T_PathTarget,
    T_ParamPathInfo,
    T_Path,
    T_IndexClause,
    T_RestrictInfo,
    T_PlaceHolderVar,
    T_SpecialJoinInfo,
    T_OuterJoinClauseInfo,
    T_AppendRelInfo,
    T_RowIdentityVarInfo,
    T_PlaceHolderInfo,
    T_MinMaxAggInfo,
    T_PlannerParamItem,
    T_GroupingSetData,
    T_RollupData,
    T_AggInfo,
    T_AggTransInfo,
    T_UniqueRelInfo,
    T_IndexPath,
    T_BitmapHeapPath,
    T_BitmapAndPath,
    T_BitmapOrPath,
    T_TidPath,
    T_TidRangePath,
    T_SubqueryScanPath,
    T_ForeignPath,
    T_CustomPath,
    T_AppendPath,
    T_MergeAppendPath,
    T_GroupResultPath,
    T_MaterialPath,
    T_MemoizePath,
    T_UniquePath,
    T_GatherPath,
    T_GatherMergePath,
    T_JoinPath,
    T_NestPath,
    T_MergePath,
    T_HashPath,
    T_ProjectionPath,
    T_ProjectSetPath,
    T_SortPath,
    T_IncrementalSortPath,
    T_GroupPath,
    T_UpperUniquePath,
    T_AggPath,
    T_GroupingSetsPath,
    T_MinMaxAggPath,
    T_WindowAggPath,
    T_SetOpPath,
    T_RecursiveUnionPath,
    T_LockRowsPath,
    T_ModifyTablePath,
    T_LimitPath,

    // ---- nodes/execnodes.h: executor runtime state ----
    T_ExprState,
    T_TupleTableSlot,
    T_TriggerData,
    T_IndexInfo,
    T_ExprContext,
    T_ReturnSetInfo,
    T_ProjectionInfo,
    T_JunkFilter,
    T_OnConflictSetState,
    T_MergeActionState,
    T_ResultRelInfo,
    T_EState,
    T_TupleHashState,
    T_WindowFuncExprState,
    T_SetExprState,
    T_SubPlanState,
    T_DomainConstraintState,
    T_PlanState,
    T_ResultState,
    T_ProjectSetState,
    T_ModifyTableState,
    T_AppendState,
    T_MergeAppendState,
    T_RecursiveUnionState,
    T_BitmapAndState,
    T_BitmapOrState,
    T_ScanState,
    T_SeqScanState,
    T_SampleScanState,
    T_IndexScanState,
    T_IndexOnlyScanState,
    T_BitmapIndexScanState,
    T_BitmapHeapScanState,
    T_TidScanState,
    T_TidRangeScanState,
    T_SubqueryScanState,
    T_FunctionScanState,
    T_ValuesScanState,
    T_TableFuncScanState,
    T_CteScanState,
    T_NamedTuplestoreScanState,
    T_WorkTableScanState,
    T_ForeignScanState,
    T_CustomScanState,
    T_JoinState,
    T_NestLoopState,
    T_MergeJoinState,
    T_HashJoinState,
    T_MaterialState,
    T_MemoizeState,
    T_SortState,
    T_IncrementalSortState,
    T_GroupState,
    T_AggState,
    T_WindowAggState,
    T_UniqueState,
    T_GatherState,
    T_GatherMergeState,
    T_HashState,
    T_SetOpState,
    T_LockRowsState,
    T_LimitState,

    // ---- nodes/replnodes.h: replication command nodes ----
    T_IdentifySystemCmd,
    T_BaseBackupCmd,
    T_CreateReplicationSlotCmd,
    T_DropReplicationSlotCmd,
    T_AlterReplicationSlotCmd,
    T_StartReplicationCmd,
    T_ReadReplicationSlotCmd,
    T_TimeLineHistoryCmd,
    T_UploadManifestCmd,

    // ---- nodes/extensible.h ----
    T_ExtensibleNode,

    // ---- access/amapi.h, access/tableam.h (AM handler routine nodes) ----
    T_IndexAmRoutine,
    T_TableAmRoutine,
    // Appended (out of C order) to avoid shifting existing discriminants.
    T_ErrorSaveContext,
    // nodes/supportnodes.h: planner-support-function request nodes.
    T_SupportRequestSimplify,
    T_SupportRequestSelectivity,
    T_SupportRequestCost,
    T_SupportRequestRows,
    T_SupportRequestIndexCondition,
    T_SupportRequestWFuncMonotonic,
    T_SupportRequestOptimizeWindowClause,
    T_SupportRequestModifyInPlace,
}
pub use NodeTag::*;

/// `pg_node_attr(...)` is a no-op marker macro consumed by gen_node_support.pl.
/// It has no runtime meaning; node structs are translated directly, so it is
/// unused in the Rust port (kept here only for documentation).

/// The first field of a node of any type is guaranteed to be the NodeTag, so the
/// type of any node can be obtained by casting it to `Node`.
#[repr(C)]
pub struct Node {
    pub r#type: NodeTag,
}

/// `nodeTag(nodeptr)`: read the NodeTag of any node pointer.
///
/// # Safety
/// `nodeptr` must point to a value whose first field is a `NodeTag` (i.e. any node).
#[inline]
pub unsafe fn nodeTag<T>(nodeptr: *const T) -> NodeTag {
    (*(nodeptr as *const Node)).r#type
}

/// `newNode(size, tag)`: allocate a zeroed node of `size` bytes and tag it.
///
/// Prefer the [`makeNode!`] macro. Returns a `*mut Node`.
///
/// # Safety
/// `size` must be at least `size_of::<Node>()`; the returned pointer is palloc'd.
#[inline]
pub unsafe fn newNode(size: Size, tag: NodeTag) -> *mut Node {
    Assert!(size >= core::mem::size_of::<Node>()); // need the tag, at least
    let result = palloc0(size) as *mut Node;
    (*result).r#type = tag;
    result
}

/// `makeNode(type)` in C. Because Rust `macro_rules!` cannot synthesize the
/// `T_##type` identifier, pass the tag explicitly: `makeNode!(Query, T_Query)`.
#[macro_export]
macro_rules! makeNode {
    ($ty:ty, $tag:ident) => {
        $crate::nodes::nodes::newNode(
            core::mem::size_of::<$ty>(),
            $crate::nodes::nodes::NodeTag::$tag,
        ) as *mut $ty
    };
}

/// `NodeSetTag(nodeptr, t)`: set a node's tag.
#[macro_export]
macro_rules! NodeSetTag {
    ($nodeptr:expr, $tag:expr) => {
        (*($nodeptr as *mut $crate::nodes::nodes::Node)).r#type = $tag
    };
}

/// `IsA(nodeptr, type)` in C. Pass the `T_`-prefixed tag: `IsA!(p, T_List)`.
#[macro_export]
macro_rules! IsA {
    ($nodeptr:expr, $tag:ident) => {
        $crate::nodes::nodes::nodeTag($nodeptr) == $crate::nodes::nodes::NodeTag::$tag
    };
}

/// `castNodeImpl`: assertion-checked cast helper backing [`castNode!`].
///
/// # Safety
/// `ptr` must be NULL or a node whose tag equals `tag`.
#[inline]
pub unsafe fn castNodeImpl(tag: NodeTag, ptr: *mut c_void) -> *mut Node {
    Assert!(ptr.is_null() || nodeTag(ptr) == tag);
    ptr as *mut Node
}

/// `castNode(type, ptr)` in C. Pass the `T_`-prefixed tag too:
/// `castNode!(List, T_List, ptr)`. Assertion-checks the tag in debug builds.
#[macro_export]
macro_rules! castNode {
    ($ty:ty, $tag:ident, $nodeptr:expr) => {
        $crate::nodes::nodes::castNodeImpl(
            $crate::nodes::nodes::NodeTag::$tag,
            $nodeptr as *mut core::ffi::c_void,
        ) as *mut $ty
    };
}

// ----------------------------------------------------------------
//   Assorted typedefs/enums that live in nodes.h (needed widely)
// ----------------------------------------------------------------

/// Parse location (a plain int; -1 means unknown).
pub type ParseLoc = c_int;

/// fraction of tuples a qualifier will pass
pub type Selectivity = f64;
/// execution cost (in page-access units)
pub type Cost = f64;
/// (estimated) number of rows or other integer count
pub type Cardinality = f64;

/// CmdType: type of operation represented by a Query or PlannedStmt.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CmdType {
    CMD_UNKNOWN,
    CMD_SELECT,
    CMD_UPDATE,
    CMD_INSERT,
    CMD_DELETE,
    CMD_MERGE,
    CMD_UTILITY,
    CMD_NOTHING,
}
pub use CmdType::*;

/// JoinType: types of relation joins.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum JoinType {
    JOIN_INNER,
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
pub use JoinType::*;

/// `IS_OUTER_JOIN(jointype)`.
#[inline]
pub fn IS_OUTER_JOIN(jointype: JoinType) -> bool {
    ((1u32 << (jointype as u32))
        & ((1u32 << (JOIN_LEFT as u32))
            | (1u32 << (JOIN_FULL as u32))
            | (1u32 << (JOIN_RIGHT as u32))
            | (1u32 << (JOIN_ANTI as u32))
            | (1u32 << (JOIN_RIGHT_ANTI as u32))))
        != 0
}

/// AggStrategy: overall execution strategies for Agg plan nodes.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AggStrategy {
    AGG_PLAIN,
    AGG_SORTED,
    AGG_HASHED,
    AGG_MIXED,
}
pub use AggStrategy::*;

// Primitive options supported by nodeAgg.c (AggSplit bit flags):
pub const AGGSPLITOP_COMBINE: c_int = 0x01;
pub const AGGSPLITOP_SKIPFINAL: c_int = 0x02;
pub const AGGSPLITOP_SERIALIZE: c_int = 0x04;
pub const AGGSPLITOP_DESERIALIZE: c_int = 0x08;

/// AggSplit: splitting (partial aggregation) modes for Agg plan nodes.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AggSplit {
    AGGSPLIT_SIMPLE = 0,
    AGGSPLIT_INITIAL_SERIAL = (AGGSPLITOP_SKIPFINAL | AGGSPLITOP_SERIALIZE) as isize,
    AGGSPLIT_FINAL_DESERIAL = (AGGSPLITOP_COMBINE | AGGSPLITOP_DESERIALIZE) as isize,
}
pub use AggSplit::*;

#[inline]
pub fn DO_AGGSPLIT_COMBINE(aggsplit: AggSplit) -> bool {
    (aggsplit as c_int & AGGSPLITOP_COMBINE) != 0
}
#[inline]
pub fn DO_AGGSPLIT_SKIPFINAL(aggsplit: AggSplit) -> bool {
    (aggsplit as c_int & AGGSPLITOP_SKIPFINAL) != 0
}
#[inline]
pub fn DO_AGGSPLIT_SERIALIZE(aggsplit: AggSplit) -> bool {
    (aggsplit as c_int & AGGSPLITOP_SERIALIZE) != 0
}
#[inline]
pub fn DO_AGGSPLIT_DESERIALIZE(aggsplit: AggSplit) -> bool {
    (aggsplit as c_int & AGGSPLITOP_DESERIALIZE) != 0
}

/// SetOpCmd: overall semantics for SetOp plan nodes.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SetOpCmd {
    SETOPCMD_INTERSECT,
    SETOPCMD_INTERSECT_ALL,
    SETOPCMD_EXCEPT,
    SETOPCMD_EXCEPT_ALL,
}
pub use SetOpCmd::*;

/// SetOpStrategy: execution strategies for SetOp plan nodes.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SetOpStrategy {
    SETOP_SORTED,
    SETOP_HASHED,
}
pub use SetOpStrategy::*;

/// OnConflictAction: "ON CONFLICT" clause type of query.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OnConflictAction {
    ONCONFLICT_NONE,
    ONCONFLICT_NOTHING,
    ONCONFLICT_UPDATE,
}
pub use OnConflictAction::*;

/// LimitOption: LIMIT option of query.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LimitOption {
    LIMIT_OPTION_COUNT,
    LIMIT_OPTION_WITH_TIES,
}
pub use LimitOption::*;
