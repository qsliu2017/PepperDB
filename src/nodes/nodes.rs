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
    T_List = 1,
    T_IntList = 471,
    T_OidList = 472,
    T_XidList = 473,

    // ---- nodes/bitmapset.h ----
    T_Bitmapset = 445,

    // ---- nodes/value.h ----
    T_Integer = 465,
    T_Float = 466,
    T_Boolean = 467,
    T_String = 468,
    T_BitString = 469,

    // ---- nodes/memnodes.h: memory-context implementations ----
    T_AllocSetContext = 474,
    T_GenerationContext = 475,
    T_SlabContext = 476,
    T_BumpContext = 477,

    // ---- nodes/primnodes.h: primitive (expression) nodes ----
    T_Alias = 2,
    T_RangeVar = 3,
    T_TableFunc = 4,
    T_IntoClause = 5,
    T_Var = 6,
    T_Const = 7,
    T_Param = 8,
    T_Aggref = 9,
    T_GroupingFunc = 10,
    T_WindowFunc = 11,
    T_WindowFuncRunCondition = 12,
    T_MergeSupportFunc = 13,
    T_SubscriptingRef = 14,
    T_FuncExpr = 15,
    T_NamedArgExpr = 16,
    T_OpExpr = 17,
    T_DistinctExpr = 18,
    T_NullIfExpr = 19,
    T_ScalarArrayOpExpr = 20,
    T_BoolExpr = 21,
    T_SubLink = 22,
    T_SubPlan = 23,
    T_AlternativeSubPlan = 24,
    T_FieldSelect = 25,
    T_FieldStore = 26,
    T_RelabelType = 27,
    T_CoerceViaIO = 28,
    T_ArrayCoerceExpr = 29,
    T_ConvertRowtypeExpr = 30,
    T_CollateExpr = 31,
    T_CaseExpr = 32,
    T_CaseWhen = 33,
    T_CaseTestExpr = 34,
    T_ArrayExpr = 35,
    T_RowExpr = 36,
    T_RowCompareExpr = 37,
    T_CoalesceExpr = 38,
    T_MinMaxExpr = 39,
    T_SQLValueFunction = 40,
    T_XmlExpr = 41,
    T_JsonFormat = 42,
    T_JsonReturning = 43,
    T_JsonValueExpr = 44,
    T_JsonConstructorExpr = 45,
    T_JsonIsPredicate = 46,
    T_JsonBehavior = 47,
    T_JsonExpr = 48,
    T_JsonTablePath = 49,
    T_JsonTablePathScan = 50,
    T_JsonTableSiblingJoin = 51,
    T_NullTest = 52,
    T_BooleanTest = 53,
    T_MergeAction = 54,
    T_CoerceToDomain = 55,
    T_CoerceToDomainValue = 56,
    T_SetToDefault = 57,
    T_CurrentOfExpr = 58,
    T_NextValueExpr = 59,
    T_InferenceElem = 60,
    T_ReturningExpr = 61,
    T_TargetEntry = 62,
    T_RangeTblRef = 63,
    T_JoinExpr = 64,
    T_FromExpr = 65,
    T_OnConflictExpr = 66,

    // ---- nodes/parsenodes.h: parse-tree / statement nodes ----
    T_Query = 67,
    T_TypeName = 68,
    T_ColumnRef = 69,
    T_ParamRef = 70,
    T_A_Expr = 71,
    T_A_Const = 72,
    T_TypeCast = 73,
    T_CollateClause = 74,
    T_RoleSpec = 75,
    T_FuncCall = 76,
    T_A_Star = 77,
    T_A_Indices = 78,
    T_A_Indirection = 79,
    T_A_ArrayExpr = 80,
    T_ResTarget = 81,
    T_MultiAssignRef = 82,
    T_SortBy = 83,
    T_WindowDef = 84,
    T_RangeSubselect = 85,
    T_RangeFunction = 86,
    T_RangeTableFunc = 87,
    T_RangeTableFuncCol = 88,
    T_RangeTableSample = 89,
    T_ColumnDef = 90,
    T_TableLikeClause = 91,
    T_IndexElem = 92,
    T_DefElem = 93,
    T_LockingClause = 94,
    T_XmlSerialize = 95,
    T_PartitionElem = 96,
    T_PartitionSpec = 97,
    T_PartitionBoundSpec = 98,
    T_PartitionRangeDatum = 99,
    T_PartitionCmd = 100,
    T_RangeTblEntry = 101,
    T_RTEPermissionInfo = 102,
    T_RangeTblFunction = 103,
    T_TableSampleClause = 104,
    T_WithCheckOption = 105,
    T_SortGroupClause = 106,
    T_GroupingSet = 107,
    T_WindowClause = 108,
    T_RowMarkClause = 109,
    T_WithClause = 110,
    T_InferClause = 111,
    T_OnConflictClause = 112,
    T_CTESearchClause = 113,
    T_CTECycleClause = 114,
    T_CommonTableExpr = 115,
    T_MergeWhenClause = 116,
    T_ReturningOption = 117,
    T_ReturningClause = 118,
    T_TriggerTransition = 119,
    T_JsonOutput = 120,
    T_JsonArgument = 121,
    T_JsonFuncExpr = 122,
    T_JsonTablePathSpec = 123,
    T_JsonTable = 124,
    T_JsonTableColumn = 125,
    T_JsonKeyValue = 126,
    T_JsonParseExpr = 127,
    T_JsonScalarExpr = 128,
    T_JsonSerializeExpr = 129,
    T_JsonObjectConstructor = 130,
    T_JsonArrayConstructor = 131,
    T_JsonArrayQueryConstructor = 132,
    T_JsonAggConstructor = 133,
    T_JsonObjectAgg = 134,
    T_JsonArrayAgg = 135,
    T_RawStmt = 136,
    T_InsertStmt = 137,
    T_DeleteStmt = 138,
    T_UpdateStmt = 139,
    T_MergeStmt = 140,
    T_SelectStmt = 141,
    T_SetOperationStmt = 142,
    T_ReturnStmt = 143,
    T_PLAssignStmt = 144,
    T_CreateSchemaStmt = 145,
    T_AlterTableStmt = 146,
    T_AlterTableCmd = 147,
    T_ATAlterConstraint = 148,
    T_ReplicaIdentityStmt = 149,
    T_AlterCollationStmt = 150,
    T_AlterDomainStmt = 151,
    T_GrantStmt = 152,
    T_ObjectWithArgs = 153,
    T_AccessPriv = 154,
    T_GrantRoleStmt = 155,
    T_AlterDefaultPrivilegesStmt = 156,
    T_CopyStmt = 157,
    T_VariableSetStmt = 158,
    T_VariableShowStmt = 159,
    T_CreateStmt = 160,
    T_Constraint = 161,
    T_CreateTableSpaceStmt = 162,
    T_DropTableSpaceStmt = 163,
    T_AlterTableSpaceOptionsStmt = 164,
    T_AlterTableMoveAllStmt = 165,
    T_CreateExtensionStmt = 166,
    T_AlterExtensionStmt = 167,
    T_AlterExtensionContentsStmt = 168,
    T_CreateFdwStmt = 169,
    T_AlterFdwStmt = 170,
    T_CreateForeignServerStmt = 171,
    T_AlterForeignServerStmt = 172,
    T_CreateForeignTableStmt = 173,
    T_CreateUserMappingStmt = 174,
    T_AlterUserMappingStmt = 175,
    T_DropUserMappingStmt = 176,
    T_ImportForeignSchemaStmt = 177,
    T_CreatePolicyStmt = 178,
    T_AlterPolicyStmt = 179,
    T_CreateAmStmt = 180,
    T_CreateTrigStmt = 181,
    T_CreateEventTrigStmt = 182,
    T_AlterEventTrigStmt = 183,
    T_CreatePLangStmt = 184,
    T_CreateRoleStmt = 185,
    T_AlterRoleStmt = 186,
    T_AlterRoleSetStmt = 187,
    T_DropRoleStmt = 188,
    T_CreateSeqStmt = 189,
    T_AlterSeqStmt = 190,
    T_DefineStmt = 191,
    T_CreateDomainStmt = 192,
    T_CreateOpClassStmt = 193,
    T_CreateOpClassItem = 194,
    T_CreateOpFamilyStmt = 195,
    T_AlterOpFamilyStmt = 196,
    T_DropStmt = 197,
    T_TruncateStmt = 198,
    T_CommentStmt = 199,
    T_SecLabelStmt = 200,
    T_DeclareCursorStmt = 201,
    T_ClosePortalStmt = 202,
    T_FetchStmt = 203,
    T_IndexStmt = 204,
    T_CreateStatsStmt = 205,
    T_StatsElem = 206,
    T_AlterStatsStmt = 207,
    T_CreateFunctionStmt = 208,
    T_FunctionParameter = 209,
    T_AlterFunctionStmt = 210,
    T_DoStmt = 211,
    T_InlineCodeBlock = 212,
    T_CallStmt = 213,
    T_CallContext = 214,
    T_RenameStmt = 215,
    T_AlterObjectDependsStmt = 216,
    T_AlterObjectSchemaStmt = 217,
    T_AlterOwnerStmt = 218,
    T_AlterOperatorStmt = 219,
    T_AlterTypeStmt = 220,
    T_RuleStmt = 221,
    T_NotifyStmt = 222,
    T_ListenStmt = 223,
    T_UnlistenStmt = 224,
    T_TransactionStmt = 225,
    T_CompositeTypeStmt = 226,
    T_CreateEnumStmt = 227,
    T_CreateRangeStmt = 228,
    T_AlterEnumStmt = 229,
    T_ViewStmt = 230,
    T_LoadStmt = 231,
    T_CreatedbStmt = 232,
    T_AlterDatabaseStmt = 233,
    T_AlterDatabaseRefreshCollStmt = 234,
    T_AlterDatabaseSetStmt = 235,
    T_DropdbStmt = 236,
    T_AlterSystemStmt = 237,
    T_ClusterStmt = 238,
    T_VacuumStmt = 239,
    T_VacuumRelation = 240,
    T_ExplainStmt = 241,
    T_CreateTableAsStmt = 242,
    T_RefreshMatViewStmt = 243,
    T_CheckPointStmt = 244,
    T_DiscardStmt = 245,
    T_LockStmt = 246,
    T_ConstraintsSetStmt = 247,
    T_ReindexStmt = 248,
    T_CreateConversionStmt = 249,
    T_CreateCastStmt = 250,
    T_CreateTransformStmt = 251,
    T_PrepareStmt = 252,
    T_ExecuteStmt = 253,
    T_DeallocateStmt = 254,
    T_DropOwnedStmt = 255,
    T_ReassignOwnedStmt = 256,
    T_AlterTSDictionaryStmt = 257,
    T_AlterTSConfigurationStmt = 258,
    T_PublicationTable = 259,
    T_PublicationObjSpec = 260,
    T_CreatePublicationStmt = 261,
    T_AlterPublicationStmt = 262,
    T_CreateSubscriptionStmt = 263,
    T_AlterSubscriptionStmt = 264,
    T_DropSubscriptionStmt = 265,

    // ---- nodes/plannodes.h: executor plan nodes ----
    T_PlannedStmt = 330,
    T_Plan = 480,
    T_Result = 331,
    T_ProjectSet = 332,
    T_ModifyTable = 333,
    T_Append = 334,
    T_MergeAppend = 335,
    T_RecursiveUnion = 336,
    T_BitmapAnd = 337,
    T_BitmapOr = 338,
    T_Scan = 481,
    T_SeqScan = 339,
    T_SampleScan = 340,
    T_IndexScan = 341,
    T_IndexOnlyScan = 342,
    T_BitmapIndexScan = 343,
    T_BitmapHeapScan = 344,
    T_TidScan = 345,
    T_TidRangeScan = 346,
    T_SubqueryScan = 347,
    T_FunctionScan = 348,
    T_ValuesScan = 349,
    T_TableFuncScan = 350,
    T_CteScan = 351,
    T_NamedTuplestoreScan = 352,
    T_WorkTableScan = 353,
    T_ForeignScan = 354,
    T_CustomScan = 355,
    T_Join = 482,
    T_NestLoop = 356,
    T_NestLoopParam = 357,
    T_MergeJoin = 358,
    T_HashJoin = 359,
    T_Material = 360,
    T_Memoize = 361,
    T_Sort = 362,
    T_IncrementalSort = 363,
    T_Group = 364,
    T_Agg = 365,
    T_WindowAgg = 366,
    T_Unique = 367,
    T_Gather = 368,
    T_GatherMerge = 369,
    T_Hash = 370,
    T_SetOp = 371,
    T_LockRows = 372,
    T_Limit = 373,
    T_PlanRowMark = 374,
    T_PartitionPruneInfo = 375,
    T_PartitionedRelPruneInfo = 376,
    T_PartitionPruneStep = 483,
    T_PartitionPruneStepOp = 377,
    T_PartitionPruneStepCombine = 378,
    T_PlanInvalItem = 379,

    // ---- nodes/pathnodes.h: planner internal nodes ----
    T_PlannerGlobal = 266,
    T_PlannerInfo = 267,
    T_RelOptInfo = 268,
    T_IndexOptInfo = 269,
    T_ForeignKeyOptInfo = 270,
    T_StatisticExtInfo = 271,
    T_JoinDomain = 272,
    T_EquivalenceClass = 273,
    T_EquivalenceMember = 274,
    T_PathKey = 275,
    T_GroupByOrdering = 276,
    T_PathTarget = 277,
    T_ParamPathInfo = 278,
    T_Path = 279,
    T_IndexClause = 281,
    T_RestrictInfo = 318,
    T_PlaceHolderVar = 319,
    T_SpecialJoinInfo = 320,
    T_OuterJoinClauseInfo = 321,
    T_AppendRelInfo = 322,
    T_RowIdentityVarInfo = 323,
    T_PlaceHolderInfo = 324,
    T_MinMaxAggInfo = 325,
    T_PlannerParamItem = 326,
    T_GroupingSetData = 308,
    T_RollupData = 309,
    T_AggInfo = 327,
    T_AggTransInfo = 328,
    T_UniqueRelInfo = 329,
    T_IndexPath = 280,
    T_BitmapHeapPath = 282,
    T_BitmapAndPath = 283,
    T_BitmapOrPath = 284,
    T_TidPath = 285,
    T_TidRangePath = 286,
    T_SubqueryScanPath = 287,
    T_ForeignPath = 288,
    T_CustomPath = 289,
    T_AppendPath = 290,
    T_MergeAppendPath = 291,
    T_GroupResultPath = 292,
    T_MaterialPath = 293,
    T_MemoizePath = 294,
    T_UniquePath = 295,
    T_GatherPath = 296,
    T_GatherMergePath = 297,
    T_JoinPath = 484,
    T_NestPath = 298,
    T_MergePath = 299,
    T_HashPath = 300,
    T_ProjectionPath = 301,
    T_ProjectSetPath = 302,
    T_SortPath = 303,
    T_IncrementalSortPath = 304,
    T_GroupPath = 305,
    T_UpperUniquePath = 306,
    T_AggPath = 307,
    T_GroupingSetsPath = 310,
    T_MinMaxAggPath = 311,
    T_WindowAggPath = 312,
    T_SetOpPath = 313,
    T_RecursiveUnionPath = 314,
    T_LockRowsPath = 315,
    T_ModifyTablePath = 316,
    T_LimitPath = 317,

    // ---- nodes/execnodes.h: executor runtime state ----
    T_ExprState = 380,
    T_TupleTableSlot = 443,
    T_TriggerData = 442,
    T_IndexInfo = 381,
    T_ExprContext = 382,
    T_ReturnSetInfo = 383,
    T_ProjectionInfo = 384,
    T_JunkFilter = 385,
    T_OnConflictSetState = 386,
    T_MergeActionState = 387,
    T_ResultRelInfo = 388,
    T_EState = 389,
    T_TupleHashState = 485,
    T_WindowFuncExprState = 390,
    T_SetExprState = 391,
    T_SubPlanState = 392,
    T_DomainConstraintState = 393,
    T_PlanState = 486,
    T_ResultState = 394,
    T_ProjectSetState = 395,
    T_ModifyTableState = 396,
    T_AppendState = 397,
    T_MergeAppendState = 398,
    T_RecursiveUnionState = 399,
    T_BitmapAndState = 400,
    T_BitmapOrState = 401,
    T_ScanState = 402,
    T_SeqScanState = 403,
    T_SampleScanState = 404,
    T_IndexScanState = 405,
    T_IndexOnlyScanState = 406,
    T_BitmapIndexScanState = 407,
    T_BitmapHeapScanState = 408,
    T_TidScanState = 409,
    T_TidRangeScanState = 410,
    T_SubqueryScanState = 411,
    T_FunctionScanState = 412,
    T_ValuesScanState = 413,
    T_TableFuncScanState = 414,
    T_CteScanState = 415,
    T_NamedTuplestoreScanState = 416,
    T_WorkTableScanState = 417,
    T_ForeignScanState = 418,
    T_CustomScanState = 419,
    T_JoinState = 420,
    T_NestLoopState = 421,
    T_MergeJoinState = 422,
    T_HashJoinState = 423,
    T_MaterialState = 424,
    T_MemoizeState = 425,
    T_SortState = 426,
    T_IncrementalSortState = 427,
    T_GroupState = 428,
    T_AggState = 429,
    T_WindowAggState = 430,
    T_UniqueState = 431,
    T_GatherState = 432,
    T_GatherMergeState = 433,
    T_HashState = 434,
    T_SetOpState = 435,
    T_LockRowsState = 436,
    T_LimitState = 437,

    // ---- nodes/replnodes.h: replication command nodes ----
    T_IdentifySystemCmd = 448,
    T_BaseBackupCmd = 449,
    T_CreateReplicationSlotCmd = 450,
    T_DropReplicationSlotCmd = 451,
    T_AlterReplicationSlotCmd = 452,
    T_StartReplicationCmd = 453,
    T_ReadReplicationSlotCmd = 454,
    T_TimeLineHistoryCmd = 455,
    T_UploadManifestCmd = 456,

    // ---- nodes/extensible.h ----
    T_ExtensibleNode = 446,

    // ---- access/amapi.h, access/tableam.h (AM handler routine nodes) ----
    T_IndexAmRoutine = 438,
    T_TableAmRoutine = 439,
    // Appended (out of C order) to avoid shifting existing discriminants.
    T_ErrorSaveContext = 447,
    // nodes/supportnodes.h: planner-support-function request nodes.
    T_SupportRequestSimplify = 457,
    T_SupportRequestSelectivity = 458,
    T_SupportRequestCost = 459,
    T_SupportRequestRows = 460,
    T_SupportRequestIndexCondition = 461,
    T_SupportRequestWFuncMonotonic = 462,
    T_SupportRequestOptimizeWindowClause = 463,
    T_SupportRequestModifyInPlace = 464,
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
