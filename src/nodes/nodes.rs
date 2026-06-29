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

    // from nodes/parsenodes.rs
    Query(Box<crate::nodes::parsenodes::Query>),
    TypeName(Box<crate::nodes::parsenodes::TypeName>),
    ColumnRef(Box<crate::nodes::parsenodes::ColumnRef>),
    ParamRef(Box<crate::nodes::parsenodes::ParamRef>),
    A_Expr(Box<crate::nodes::parsenodes::A_Expr>),
    A_Const(Box<crate::nodes::parsenodes::A_Const>),
    TypeCast(Box<crate::nodes::parsenodes::TypeCast>),
    CollateClause(Box<crate::nodes::parsenodes::CollateClause>),
    RoleSpec(Box<crate::nodes::parsenodes::RoleSpec>),
    FuncCall(Box<crate::nodes::parsenodes::FuncCall>),
    A_Star(Box<crate::nodes::parsenodes::A_Star>),
    A_Indices(Box<crate::nodes::parsenodes::A_Indices>),
    A_Indirection(Box<crate::nodes::parsenodes::A_Indirection>),
    A_ArrayExpr(Box<crate::nodes::parsenodes::A_ArrayExpr>),
    ResTarget(Box<crate::nodes::parsenodes::ResTarget>),
    MultiAssignRef(Box<crate::nodes::parsenodes::MultiAssignRef>),
    SortBy(Box<crate::nodes::parsenodes::SortBy>),
    WindowDef(Box<crate::nodes::parsenodes::WindowDef>),
    RangeSubselect(Box<crate::nodes::parsenodes::RangeSubselect>),
    RangeFunction(Box<crate::nodes::parsenodes::RangeFunction>),
    RangeTableFunc(Box<crate::nodes::parsenodes::RangeTableFunc>),
    RangeTableFuncCol(Box<crate::nodes::parsenodes::RangeTableFuncCol>),
    RangeTableSample(Box<crate::nodes::parsenodes::RangeTableSample>),
    ColumnDef(Box<crate::nodes::parsenodes::ColumnDef>),
    TableLikeClause(Box<crate::nodes::parsenodes::TableLikeClause>),
    IndexElem(Box<crate::nodes::parsenodes::IndexElem>),
    DefElem(Box<crate::nodes::parsenodes::DefElem>),
    LockingClause(Box<crate::nodes::parsenodes::LockingClause>),
    XmlSerialize(Box<crate::nodes::parsenodes::XmlSerialize>),
    PartitionElem(Box<crate::nodes::parsenodes::PartitionElem>),
    PartitionSpec(Box<crate::nodes::parsenodes::PartitionSpec>),
    PartitionBoundSpec(Box<crate::nodes::parsenodes::PartitionBoundSpec>),
    PartitionRangeDatum(Box<crate::nodes::parsenodes::PartitionRangeDatum>),
    PartitionCmd(Box<crate::nodes::parsenodes::PartitionCmd>),
    RangeTblEntry(Box<crate::nodes::parsenodes::RangeTblEntry>),
    RTEPermissionInfo(Box<crate::nodes::parsenodes::RTEPermissionInfo>),
    RangeTblFunction(Box<crate::nodes::parsenodes::RangeTblFunction>),
    TableSampleClause(Box<crate::nodes::parsenodes::TableSampleClause>),
    WithCheckOption(Box<crate::nodes::parsenodes::WithCheckOption>),
    SortGroupClause(Box<crate::nodes::parsenodes::SortGroupClause>),
    GroupingSet(Box<crate::nodes::parsenodes::GroupingSet>),
    WindowClause(Box<crate::nodes::parsenodes::WindowClause>),
    RowMarkClause(Box<crate::nodes::parsenodes::RowMarkClause>),
    WithClause(Box<crate::nodes::parsenodes::WithClause>),
    InferClause(Box<crate::nodes::parsenodes::InferClause>),
    OnConflictClause(Box<crate::nodes::parsenodes::OnConflictClause>),
    CTESearchClause(Box<crate::nodes::parsenodes::CTESearchClause>),
    CTECycleClause(Box<crate::nodes::parsenodes::CTECycleClause>),
    CommonTableExpr(Box<crate::nodes::parsenodes::CommonTableExpr>),
    MergeWhenClause(Box<crate::nodes::parsenodes::MergeWhenClause>),
    ReturningOption(Box<crate::nodes::parsenodes::ReturningOption>),
    ReturningClause(Box<crate::nodes::parsenodes::ReturningClause>),
    TriggerTransition(Box<crate::nodes::parsenodes::TriggerTransition>),
    JsonOutput(Box<crate::nodes::parsenodes::JsonOutput>),
    JsonArgument(Box<crate::nodes::parsenodes::JsonArgument>),
    JsonFuncExpr(Box<crate::nodes::parsenodes::JsonFuncExpr>),
    JsonTablePathSpec(Box<crate::nodes::parsenodes::JsonTablePathSpec>),
    JsonTable(Box<crate::nodes::parsenodes::JsonTable>),
    JsonTableColumn(Box<crate::nodes::parsenodes::JsonTableColumn>),
    JsonKeyValue(Box<crate::nodes::parsenodes::JsonKeyValue>),
    JsonParseExpr(Box<crate::nodes::parsenodes::JsonParseExpr>),
    JsonScalarExpr(Box<crate::nodes::parsenodes::JsonScalarExpr>),
    JsonSerializeExpr(Box<crate::nodes::parsenodes::JsonSerializeExpr>),
    JsonObjectConstructor(Box<crate::nodes::parsenodes::JsonObjectConstructor>),
    JsonArrayConstructor(Box<crate::nodes::parsenodes::JsonArrayConstructor>),
    JsonArrayQueryConstructor(Box<crate::nodes::parsenodes::JsonArrayQueryConstructor>),
    JsonAggConstructor(Box<crate::nodes::parsenodes::JsonAggConstructor>),
    JsonObjectAgg(Box<crate::nodes::parsenodes::JsonObjectAgg>),
    JsonArrayAgg(Box<crate::nodes::parsenodes::JsonArrayAgg>),
    RawStmt(Box<crate::nodes::parsenodes::RawStmt>),
    InsertStmt(Box<crate::nodes::parsenodes::InsertStmt>),
    DeleteStmt(Box<crate::nodes::parsenodes::DeleteStmt>),
    UpdateStmt(Box<crate::nodes::parsenodes::UpdateStmt>),
    MergeStmt(Box<crate::nodes::parsenodes::MergeStmt>),
    SelectStmt(Box<crate::nodes::parsenodes::SelectStmt>),
    SetOperationStmt(Box<crate::nodes::parsenodes::SetOperationStmt>),
    ReturnStmt(Box<crate::nodes::parsenodes::ReturnStmt>),
    PLAssignStmt(Box<crate::nodes::parsenodes::PLAssignStmt>),
    CreateSchemaStmt(Box<crate::nodes::parsenodes::CreateSchemaStmt>),
    AlterTableStmt(Box<crate::nodes::parsenodes::AlterTableStmt>),
    AlterTableCmd(Box<crate::nodes::parsenodes::AlterTableCmd>),
    ATAlterConstraint(Box<crate::nodes::parsenodes::ATAlterConstraint>),
    ReplicaIdentityStmt(Box<crate::nodes::parsenodes::ReplicaIdentityStmt>),
    AlterCollationStmt(Box<crate::nodes::parsenodes::AlterCollationStmt>),
    AlterDomainStmt(Box<crate::nodes::parsenodes::AlterDomainStmt>),
    GrantStmt(Box<crate::nodes::parsenodes::GrantStmt>),
    ObjectWithArgs(Box<crate::nodes::parsenodes::ObjectWithArgs>),
    AccessPriv(Box<crate::nodes::parsenodes::AccessPriv>),
    GrantRoleStmt(Box<crate::nodes::parsenodes::GrantRoleStmt>),
    AlterDefaultPrivilegesStmt(Box<crate::nodes::parsenodes::AlterDefaultPrivilegesStmt>),
    CopyStmt(Box<crate::nodes::parsenodes::CopyStmt>),
    VariableSetStmt(Box<crate::nodes::parsenodes::VariableSetStmt>),
    VariableShowStmt(Box<crate::nodes::parsenodes::VariableShowStmt>),
    CreateStmt(Box<crate::nodes::parsenodes::CreateStmt>),
    Constraint(Box<crate::nodes::parsenodes::Constraint>),
    CreateTableSpaceStmt(Box<crate::nodes::parsenodes::CreateTableSpaceStmt>),
    DropTableSpaceStmt(Box<crate::nodes::parsenodes::DropTableSpaceStmt>),
    AlterTableSpaceOptionsStmt(Box<crate::nodes::parsenodes::AlterTableSpaceOptionsStmt>),
    AlterTableMoveAllStmt(Box<crate::nodes::parsenodes::AlterTableMoveAllStmt>),
    CreateExtensionStmt(Box<crate::nodes::parsenodes::CreateExtensionStmt>),
    AlterExtensionStmt(Box<crate::nodes::parsenodes::AlterExtensionStmt>),
    AlterExtensionContentsStmt(Box<crate::nodes::parsenodes::AlterExtensionContentsStmt>),
    CreateFdwStmt(Box<crate::nodes::parsenodes::CreateFdwStmt>),
    AlterFdwStmt(Box<crate::nodes::parsenodes::AlterFdwStmt>),
    CreateForeignServerStmt(Box<crate::nodes::parsenodes::CreateForeignServerStmt>),
    AlterForeignServerStmt(Box<crate::nodes::parsenodes::AlterForeignServerStmt>),
    CreateForeignTableStmt(Box<crate::nodes::parsenodes::CreateForeignTableStmt>),
    CreateUserMappingStmt(Box<crate::nodes::parsenodes::CreateUserMappingStmt>),
    AlterUserMappingStmt(Box<crate::nodes::parsenodes::AlterUserMappingStmt>),
    DropUserMappingStmt(Box<crate::nodes::parsenodes::DropUserMappingStmt>),
    ImportForeignSchemaStmt(Box<crate::nodes::parsenodes::ImportForeignSchemaStmt>),
    CreatePolicyStmt(Box<crate::nodes::parsenodes::CreatePolicyStmt>),
    AlterPolicyStmt(Box<crate::nodes::parsenodes::AlterPolicyStmt>),
    CreateAmStmt(Box<crate::nodes::parsenodes::CreateAmStmt>),
    CreateTrigStmt(Box<crate::nodes::parsenodes::CreateTrigStmt>),
    CreateEventTrigStmt(Box<crate::nodes::parsenodes::CreateEventTrigStmt>),
    AlterEventTrigStmt(Box<crate::nodes::parsenodes::AlterEventTrigStmt>),
    CreatePLangStmt(Box<crate::nodes::parsenodes::CreatePLangStmt>),
    CreateRoleStmt(Box<crate::nodes::parsenodes::CreateRoleStmt>),
    AlterRoleStmt(Box<crate::nodes::parsenodes::AlterRoleStmt>),
    AlterRoleSetStmt(Box<crate::nodes::parsenodes::AlterRoleSetStmt>),
    DropRoleStmt(Box<crate::nodes::parsenodes::DropRoleStmt>),
    CreateSeqStmt(Box<crate::nodes::parsenodes::CreateSeqStmt>),
    AlterSeqStmt(Box<crate::nodes::parsenodes::AlterSeqStmt>),
    DefineStmt(Box<crate::nodes::parsenodes::DefineStmt>),
    CreateDomainStmt(Box<crate::nodes::parsenodes::CreateDomainStmt>),
    CreateOpClassStmt(Box<crate::nodes::parsenodes::CreateOpClassStmt>),
    CreateOpClassItem(Box<crate::nodes::parsenodes::CreateOpClassItem>),
    CreateOpFamilyStmt(Box<crate::nodes::parsenodes::CreateOpFamilyStmt>),
    AlterOpFamilyStmt(Box<crate::nodes::parsenodes::AlterOpFamilyStmt>),
    DropStmt(Box<crate::nodes::parsenodes::DropStmt>),
    TruncateStmt(Box<crate::nodes::parsenodes::TruncateStmt>),
    CommentStmt(Box<crate::nodes::parsenodes::CommentStmt>),
    SecLabelStmt(Box<crate::nodes::parsenodes::SecLabelStmt>),
    DeclareCursorStmt(Box<crate::nodes::parsenodes::DeclareCursorStmt>),
    ClosePortalStmt(Box<crate::nodes::parsenodes::ClosePortalStmt>),
    FetchStmt(Box<crate::nodes::parsenodes::FetchStmt>),
    IndexStmt(Box<crate::nodes::parsenodes::IndexStmt>),
    CreateStatsStmt(Box<crate::nodes::parsenodes::CreateStatsStmt>),
    StatsElem(Box<crate::nodes::parsenodes::StatsElem>),
    AlterStatsStmt(Box<crate::nodes::parsenodes::AlterStatsStmt>),
    CreateFunctionStmt(Box<crate::nodes::parsenodes::CreateFunctionStmt>),
    FunctionParameter(Box<crate::nodes::parsenodes::FunctionParameter>),
    AlterFunctionStmt(Box<crate::nodes::parsenodes::AlterFunctionStmt>),
    DoStmt(Box<crate::nodes::parsenodes::DoStmt>),
    InlineCodeBlock(Box<crate::nodes::parsenodes::InlineCodeBlock>),
    CallStmt(Box<crate::nodes::parsenodes::CallStmt>),
    CallContext(Box<crate::nodes::parsenodes::CallContext>),
    RenameStmt(Box<crate::nodes::parsenodes::RenameStmt>),
    AlterObjectDependsStmt(Box<crate::nodes::parsenodes::AlterObjectDependsStmt>),
    AlterObjectSchemaStmt(Box<crate::nodes::parsenodes::AlterObjectSchemaStmt>),
    AlterOwnerStmt(Box<crate::nodes::parsenodes::AlterOwnerStmt>),
    AlterOperatorStmt(Box<crate::nodes::parsenodes::AlterOperatorStmt>),
    AlterTypeStmt(Box<crate::nodes::parsenodes::AlterTypeStmt>),
    RuleStmt(Box<crate::nodes::parsenodes::RuleStmt>),
    NotifyStmt(Box<crate::nodes::parsenodes::NotifyStmt>),
    ListenStmt(Box<crate::nodes::parsenodes::ListenStmt>),
    UnlistenStmt(Box<crate::nodes::parsenodes::UnlistenStmt>),
    TransactionStmt(Box<crate::nodes::parsenodes::TransactionStmt>),
    CompositeTypeStmt(Box<crate::nodes::parsenodes::CompositeTypeStmt>),
    CreateEnumStmt(Box<crate::nodes::parsenodes::CreateEnumStmt>),
    CreateRangeStmt(Box<crate::nodes::parsenodes::CreateRangeStmt>),
    AlterEnumStmt(Box<crate::nodes::parsenodes::AlterEnumStmt>),
    ViewStmt(Box<crate::nodes::parsenodes::ViewStmt>),
    LoadStmt(Box<crate::nodes::parsenodes::LoadStmt>),
    CreatedbStmt(Box<crate::nodes::parsenodes::CreatedbStmt>),
    AlterDatabaseStmt(Box<crate::nodes::parsenodes::AlterDatabaseStmt>),
    AlterDatabaseRefreshCollStmt(Box<crate::nodes::parsenodes::AlterDatabaseRefreshCollStmt>),
    AlterDatabaseSetStmt(Box<crate::nodes::parsenodes::AlterDatabaseSetStmt>),
    DropdbStmt(Box<crate::nodes::parsenodes::DropdbStmt>),
    AlterSystemStmt(Box<crate::nodes::parsenodes::AlterSystemStmt>),
    ClusterStmt(Box<crate::nodes::parsenodes::ClusterStmt>),
    VacuumStmt(Box<crate::nodes::parsenodes::VacuumStmt>),
    VacuumRelation(Box<crate::nodes::parsenodes::VacuumRelation>),
    ExplainStmt(Box<crate::nodes::parsenodes::ExplainStmt>),
    CreateTableAsStmt(Box<crate::nodes::parsenodes::CreateTableAsStmt>),
    RefreshMatViewStmt(Box<crate::nodes::parsenodes::RefreshMatViewStmt>),
    CheckPointStmt(Box<crate::nodes::parsenodes::CheckPointStmt>),
    DiscardStmt(Box<crate::nodes::parsenodes::DiscardStmt>),
    LockStmt(Box<crate::nodes::parsenodes::LockStmt>),
    ConstraintsSetStmt(Box<crate::nodes::parsenodes::ConstraintsSetStmt>),
    ReindexStmt(Box<crate::nodes::parsenodes::ReindexStmt>),
    CreateConversionStmt(Box<crate::nodes::parsenodes::CreateConversionStmt>),
    CreateCastStmt(Box<crate::nodes::parsenodes::CreateCastStmt>),
    CreateTransformStmt(Box<crate::nodes::parsenodes::CreateTransformStmt>),
    PrepareStmt(Box<crate::nodes::parsenodes::PrepareStmt>),
    ExecuteStmt(Box<crate::nodes::parsenodes::ExecuteStmt>),
    DeallocateStmt(Box<crate::nodes::parsenodes::DeallocateStmt>),
    DropOwnedStmt(Box<crate::nodes::parsenodes::DropOwnedStmt>),
    ReassignOwnedStmt(Box<crate::nodes::parsenodes::ReassignOwnedStmt>),
    AlterTSDictionaryStmt(Box<crate::nodes::parsenodes::AlterTSDictionaryStmt>),
    AlterTSConfigurationStmt(Box<crate::nodes::parsenodes::AlterTSConfigurationStmt>),
    PublicationTable(Box<crate::nodes::parsenodes::PublicationTable>),
    PublicationObjSpec(Box<crate::nodes::parsenodes::PublicationObjSpec>),
    CreatePublicationStmt(Box<crate::nodes::parsenodes::CreatePublicationStmt>),
    AlterPublicationStmt(Box<crate::nodes::parsenodes::AlterPublicationStmt>),
    CreateSubscriptionStmt(Box<crate::nodes::parsenodes::CreateSubscriptionStmt>),
    AlterSubscriptionStmt(Box<crate::nodes::parsenodes::AlterSubscriptionStmt>),
    DropSubscriptionStmt(Box<crate::nodes::parsenodes::DropSubscriptionStmt>),

    // from nodes/plannodes.rs
    PlannedStmt(Box<crate::nodes::plannodes::PlannedStmt>),
    Result(Box<crate::nodes::plannodes::Result>),
    ProjectSet(Box<crate::nodes::plannodes::ProjectSet>),
    ModifyTable(Box<crate::nodes::plannodes::ModifyTable>),
    Append(Box<crate::nodes::plannodes::Append>),
    MergeAppend(Box<crate::nodes::plannodes::MergeAppend>),
    RecursiveUnion(Box<crate::nodes::plannodes::RecursiveUnion>),
    BitmapAnd(Box<crate::nodes::plannodes::BitmapAnd>),
    BitmapOr(Box<crate::nodes::plannodes::BitmapOr>),
    SeqScan(Box<crate::nodes::plannodes::SeqScan>),
    SampleScan(Box<crate::nodes::plannodes::SampleScan>),
    IndexScan(Box<crate::nodes::plannodes::IndexScan>),
    IndexOnlyScan(Box<crate::nodes::plannodes::IndexOnlyScan>),
    BitmapIndexScan(Box<crate::nodes::plannodes::BitmapIndexScan>),
    BitmapHeapScan(Box<crate::nodes::plannodes::BitmapHeapScan>),
    TidScan(Box<crate::nodes::plannodes::TidScan>),
    TidRangeScan(Box<crate::nodes::plannodes::TidRangeScan>),
    SubqueryScan(Box<crate::nodes::plannodes::SubqueryScan>),
    FunctionScan(Box<crate::nodes::plannodes::FunctionScan>),
    ValuesScan(Box<crate::nodes::plannodes::ValuesScan>),
    TableFuncScan(Box<crate::nodes::plannodes::TableFuncScan>),
    CteScan(Box<crate::nodes::plannodes::CteScan>),
    NamedTuplestoreScan(Box<crate::nodes::plannodes::NamedTuplestoreScan>),
    WorkTableScan(Box<crate::nodes::plannodes::WorkTableScan>),
    ForeignScan(Box<crate::nodes::plannodes::ForeignScan>),
    CustomScan(Box<crate::nodes::plannodes::CustomScan>),
    NestLoop(Box<crate::nodes::plannodes::NestLoop>),
    NestLoopParam(Box<crate::nodes::plannodes::NestLoopParam>),
    MergeJoin(Box<crate::nodes::plannodes::MergeJoin>),
    HashJoin(Box<crate::nodes::plannodes::HashJoin>),
    Material(Box<crate::nodes::plannodes::Material>),
    Memoize(Box<crate::nodes::plannodes::Memoize>),
    Sort(Box<crate::nodes::plannodes::Sort>),
    IncrementalSort(Box<crate::nodes::plannodes::IncrementalSort>),
    Group(Box<crate::nodes::plannodes::Group>),
    Agg(Box<crate::nodes::plannodes::Agg>),
    WindowAgg(Box<crate::nodes::plannodes::WindowAgg>),
    Unique(Box<crate::nodes::plannodes::Unique>),
    Gather(Box<crate::nodes::plannodes::Gather>),
    GatherMerge(Box<crate::nodes::plannodes::GatherMerge>),
    Hash(Box<crate::nodes::plannodes::Hash>),
    SetOp(Box<crate::nodes::plannodes::SetOp>),
    LockRows(Box<crate::nodes::plannodes::LockRows>),
    Limit(Box<crate::nodes::plannodes::Limit>),
    PlanRowMark(Box<crate::nodes::plannodes::PlanRowMark>),
    PartitionPruneInfo(Box<crate::nodes::plannodes::PartitionPruneInfo>),
    PartitionedRelPruneInfo(Box<crate::nodes::plannodes::PartitionedRelPruneInfo>),
    PartitionPruneStepOp(Box<crate::nodes::plannodes::PartitionPruneStepOp>),
    PartitionPruneStepCombine(Box<crate::nodes::plannodes::PartitionPruneStepCombine>),
    PlanInvalItem(Box<crate::nodes::plannodes::PlanInvalItem>),

    // from nodes/pathnodes.rs
    PlannerGlobal(Box<crate::nodes::pathnodes::PlannerGlobal>),
    PlannerInfo(Box<crate::nodes::pathnodes::PlannerInfo>),
    RelOptInfo(Box<crate::nodes::pathnodes::RelOptInfo>),
    IndexOptInfo(Box<crate::nodes::pathnodes::IndexOptInfo>),
    ForeignKeyOptInfo(Box<crate::nodes::pathnodes::ForeignKeyOptInfo>),
    StatisticExtInfo(Box<crate::nodes::pathnodes::StatisticExtInfo>),
    JoinDomain(Box<crate::nodes::pathnodes::JoinDomain>),
    EquivalenceClass(Box<crate::nodes::pathnodes::EquivalenceClass>),
    EquivalenceMember(Box<crate::nodes::pathnodes::EquivalenceMember>),
    PathKey(Box<crate::nodes::pathnodes::PathKey>),
    GroupByOrdering(Box<crate::nodes::pathnodes::GroupByOrdering>),
    PathTarget(Box<crate::nodes::pathnodes::PathTarget>),
    ParamPathInfo(Box<crate::nodes::pathnodes::ParamPathInfo>),
    Path(Box<crate::nodes::pathnodes::Path>),
    IndexPath(Box<crate::nodes::pathnodes::IndexPath>),
    IndexClause(Box<crate::nodes::pathnodes::IndexClause>),
    BitmapHeapPath(Box<crate::nodes::pathnodes::BitmapHeapPath>),
    BitmapAndPath(Box<crate::nodes::pathnodes::BitmapAndPath>),
    BitmapOrPath(Box<crate::nodes::pathnodes::BitmapOrPath>),
    TidPath(Box<crate::nodes::pathnodes::TidPath>),
    TidRangePath(Box<crate::nodes::pathnodes::TidRangePath>),
    SubqueryScanPath(Box<crate::nodes::pathnodes::SubqueryScanPath>),
    ForeignPath(Box<crate::nodes::pathnodes::ForeignPath>),
    CustomPath(Box<crate::nodes::pathnodes::CustomPath>),
    AppendPath(Box<crate::nodes::pathnodes::AppendPath>),
    MergeAppendPath(Box<crate::nodes::pathnodes::MergeAppendPath>),
    GroupResultPath(Box<crate::nodes::pathnodes::GroupResultPath>),
    MaterialPath(Box<crate::nodes::pathnodes::MaterialPath>),
    MemoizePath(Box<crate::nodes::pathnodes::MemoizePath>),
    UniquePath(Box<crate::nodes::pathnodes::UniquePath>),
    GatherPath(Box<crate::nodes::pathnodes::GatherPath>),
    GatherMergePath(Box<crate::nodes::pathnodes::GatherMergePath>),
    NestPath(Box<crate::nodes::pathnodes::NestPath>),
    MergePath(Box<crate::nodes::pathnodes::MergePath>),
    HashPath(Box<crate::nodes::pathnodes::HashPath>),
    ProjectionPath(Box<crate::nodes::pathnodes::ProjectionPath>),
    ProjectSetPath(Box<crate::nodes::pathnodes::ProjectSetPath>),
    SortPath(Box<crate::nodes::pathnodes::SortPath>),
    IncrementalSortPath(Box<crate::nodes::pathnodes::IncrementalSortPath>),
    GroupPath(Box<crate::nodes::pathnodes::GroupPath>),
    UpperUniquePath(Box<crate::nodes::pathnodes::UpperUniquePath>),
    AggPath(Box<crate::nodes::pathnodes::AggPath>),
    GroupingSetData(Box<crate::nodes::pathnodes::GroupingSetData>),
    RollupData(Box<crate::nodes::pathnodes::RollupData>),
    GroupingSetsPath(Box<crate::nodes::pathnodes::GroupingSetsPath>),
    MinMaxAggPath(Box<crate::nodes::pathnodes::MinMaxAggPath>),
    WindowAggPath(Box<crate::nodes::pathnodes::WindowAggPath>),
    SetOpPath(Box<crate::nodes::pathnodes::SetOpPath>),
    RecursiveUnionPath(Box<crate::nodes::pathnodes::RecursiveUnionPath>),
    LockRowsPath(Box<crate::nodes::pathnodes::LockRowsPath>),
    ModifyTablePath(Box<crate::nodes::pathnodes::ModifyTablePath>),
    LimitPath(Box<crate::nodes::pathnodes::LimitPath>),
    RestrictInfo(Box<crate::nodes::pathnodes::RestrictInfo>),
    PlaceHolderVar(Box<crate::nodes::pathnodes::PlaceHolderVar>),
    SpecialJoinInfo(Box<crate::nodes::pathnodes::SpecialJoinInfo>),
    OuterJoinClauseInfo(Box<crate::nodes::pathnodes::OuterJoinClauseInfo>),
    AppendRelInfo(Box<crate::nodes::pathnodes::AppendRelInfo>),
    RowIdentityVarInfo(Box<crate::nodes::pathnodes::RowIdentityVarInfo>),
    PlaceHolderInfo(Box<crate::nodes::pathnodes::PlaceHolderInfo>),
    MinMaxAggInfo(Box<crate::nodes::pathnodes::MinMaxAggInfo>),
    PlannerParamItem(Box<crate::nodes::pathnodes::PlannerParamItem>),
    AggInfo(Box<crate::nodes::pathnodes::AggInfo>),
    AggTransInfo(Box<crate::nodes::pathnodes::AggTransInfo>),
    UniqueRelInfo(Box<crate::nodes::pathnodes::UniqueRelInfo>),

    // from nodes/supportnodes.rs
    SupportRequestSimplify(Box<crate::nodes::supportnodes::SupportRequestSimplify>),
    SupportRequestSelectivity(Box<crate::nodes::supportnodes::SupportRequestSelectivity>),
    SupportRequestCost(Box<crate::nodes::supportnodes::SupportRequestCost>),
    SupportRequestRows(Box<crate::nodes::supportnodes::SupportRequestRows>),
    SupportRequestIndexCondition(Box<crate::nodes::supportnodes::SupportRequestIndexCondition>),
    SupportRequestWFuncMonotonic(Box<crate::nodes::supportnodes::SupportRequestWFuncMonotonic>),
    SupportRequestOptimizeWindowClause(
        Box<crate::nodes::supportnodes::SupportRequestOptimizeWindowClause>,
    ),
    SupportRequestModifyInPlace(Box<crate::nodes::supportnodes::SupportRequestModifyInPlace>),

    // from nodes/value.rs -- the primitive value nodes (nodes.h T_Integer / T_Float
    // / T_Boolean / T_String / T_BitString). These appear inside node lists (e.g.
    // an operator name `List *` is a list of T_String, a type name's name parts,
    // DefElem args). They carry their value inline (no Box: each is one machine
    // word or a String).
    Integer(crate::nodes::value::Integer),
    Float(crate::nodes::value::Float),
    Boolean(crate::nodes::value::Boolean),
    String_(crate::nodes::value::String_),
    BitString(crate::nodes::value::BitString),
    // NOTE: executor-state nodes (nodes/execnodes.rs) are runtime state that PG's
    // copyObject/equal/outNode never touch; they cannot derive Clone/PartialEq
    // (closures/opaque handles), so they are NOT Node variants. They live as plain
    // structs in execnodes.rs and are referenced by concrete type.
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
    UNKNOWN = 0,
    SELECT,
    UPDATE,
    INSERT,
    DELETE,
    MERGE,
    /// Utility cmds like create, destroy, copy, vacuum.
    UTILITY,
    /// Dummy command for instead-nothing rules with qual.
    NOTHING,
}

/// Types of relation joins; determines handling of unmatched tuples.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    INNER = 0,
    LEFT,
    FULL,
    RIGHT,
    SEMI,
    ANTI,
    RIGHT_SEMI,
    RIGHT_ANTI,
    UNIQUE_OUTER,
    UNIQUE_INNER,
}

/// C: `IS_OUTER_JOIN(jointype)`.
pub fn IS_OUTER_JOIN(jointype: JoinType) -> bool {
    matches!(
        jointype,
        JoinType::LEFT
            | JoinType::FULL
            | JoinType::RIGHT
            | JoinType::ANTI
            | JoinType::RIGHT_ANTI
    )
}

/// Overall execution strategies for Agg plan nodes.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggStrategy {
    /// Simple agg across all input rows.
    PLAIN = 0,
    /// Grouped agg, input must be sorted.
    SORTED,
    /// Grouped agg, use internal hashtable.
    HASHED,
    /// Grouped agg, hash and sort both used.
    MIXED,
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
    SIMPLE = 0,
    /// Initial phase of partial aggregation, with serialization.
    INITIAL_SERIAL = AggSplitOp::SKIPFINAL.bits() | AggSplitOp::SERIALIZE.bits(),
    /// Final phase of partial aggregation, with deserialization.
    FINAL_DESERIAL = AggSplitOp::COMBINE.bits() | AggSplitOp::DESERIALIZE.bits(),
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
    INTERSECT = 0,
    INTERSECT_ALL,
    EXCEPT,
    EXCEPT_ALL,
}

/// Execution strategies for SetOp plan nodes.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOpStrategy {
    /// Input must be sorted.
    SORTED = 0,
    /// Use internal hashtable.
    HASHED,
}

/// "ON CONFLICT" clause type of query.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnConflictAction {
    /// No "ON CONFLICT" clause.
    NONE = 0,
    /// ON CONFLICT ... DO NOTHING.
    NOTHING,
    /// ON CONFLICT ... DO UPDATE.
    UPDATE,
}

/// LIMIT option of query.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitOption {
    /// FETCH FIRST ... ONLY.
    COUNT = 0,
    /// FETCH FIRST ... WITH TIES.
    WITH_TIES,
}
