/*-------------------------------------------------------------------------
 *
 * utility.c
 *    Contains functions which control the execution of the POSTGRES utility
 *    commands.  At one time acted as an interface between the Lisp and C
 *    systems.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/tcop/utility.c
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;
use crate::{makeNode, IsA, foreach, current_cell, castNode, Assert};

use crate::nodes::nodes::{CmdType, Node, NodeTag, nodeTag};
use crate::nodes::nodes::CmdType::*;
use crate::nodes::nodes::NodeTag::*;
use crate::nodes::plannodes::PlannedStmt;
use crate::nodes::pg_list::{List, NIL};
use crate::nodes::parsenodes::{
    ObjectType, TransactionStmtKind,
    AlterDomainStmt, AlterFunctionStmt, AlterObjectDependsStmt,
    AlterObjectSchemaStmt, AlterOwnerStmt, AlterTableCmd, AlterTableType, AlterTableMoveAllStmt,
    AlterTableSpaceOptionsStmt, AlterTableStmt, CallStmt, CheckPointStmt,
    ClosePortalStmt, ClusterStmt, CommentStmt, ConstraintsSetStmt,
    CopyStmt, CreateAmStmt, CreateCastStmt, CreateConversionStmt,
    CreateDomainStmt, CreateEnumStmt, CreateEventTrigStmt, CreateExtensionStmt,
    CreateFdwStmt, CreateForeignServerStmt, CreateForeignTableStmt, CreateFunctionStmt,
    CreateOpClassStmt, CreateOpFamilyStmt, CreatePLangStmt, CreatePolicyStmt,
    CreatePublicationStmt, CreateRangeStmt, CreateRoleStmt, CreateSchemaStmt,
    CreateSeqStmt, CreateStatsStmt, CreateStmt, CreateSubscriptionStmt,
    CreateTableAsStmt, CreateTableSpaceStmt, CreateTransformStmt, CreateTrigStmt,
    CreateUserMappingStmt, CreatedbStmt, DeallocateStmt, DeclareCursorStmt,
    DefElem, DefineStmt, DiscardStmt, DoStmt, DropOwnedStmt, DropRoleStmt,
    DropStmt, DropSubscriptionStmt, DropTableSpaceStmt, DropUserMappingStmt,
    DropdbStmt, ExecuteStmt, ExplainStmt, FetchStmt, GrantRoleStmt, GrantStmt,
    ImportForeignSchemaStmt, IndexStmt, ListenStmt, LoadStmt, LockStmt,
    NotifyStmt, PrepareStmt, ReassignOwnedStmt, RefreshMatViewStmt, ReindexStmt,
    RenameStmt, RuleStmt, SecLabelStmt, SelectStmt, TransactionStmt,
    TruncateStmt, UnlistenStmt, VacuumStmt, VariableSetStmt, VariableShowStmt,
    ViewStmt, AlterCollationStmt, AlterDatabaseRefreshCollStmt,
    AlterDatabaseSetStmt, AlterDatabaseStmt, AlterDefaultPrivilegesStmt,
    AlterEnumStmt, AlterEventTrigStmt, AlterExtensionContentsStmt,
    AlterExtensionStmt, AlterFdwStmt, AlterForeignServerStmt, AlterOpFamilyStmt,
    AlterOperatorStmt, AlterPolicyStmt, AlterPublicationStmt,
    AlterRoleSetStmt, AlterRoleStmt, AlterSeqStmt, AlterStatsStmt,
    AlterSubscriptionStmt, AlterTSConfigurationStmt, AlterTSDictionaryStmt,
    AlterTypeStmt, AlterUserMappingStmt, AlterSystemStmt, CompositeTypeStmt,
    GrantStmt as _GrantStmt,
    PLAssignStmt, RawStmt,
};
use crate::nodes::parsenodes::ObjectType::*;
use crate::nodes::parsenodes::{VariableSetKind, DiscardMode, DropBehavior};
use crate::nodes::parsenodes::VariableSetKind::*;
use crate::nodes::parsenodes::DiscardMode::*;
use crate::nodes::parsenodes::TransactionStmtKind::*;
use crate::nodes::primnodes::{FuncExpr, RangeVar};
use crate::nodes::parsenodes::Query;
use crate::tcop::cmdtag::{
    CommandTag, CommandTag::*, QueryCompletion, SetQueryCompletion, GetCommandTagName,
};
use crate::tcop::dest::DestReceiver;
use crate::tcop::tcopprot::{LogStmtLevel, LOGSTMT_ALL, LOGSTMT_DDL, LOGSTMT_MOD};
use crate::storage::lockdefs::{LOCKMODE, RowExclusiveLock, ShareLock, ShareUpdateExclusiveLock};
use crate::catalog::objectaccess::ObjectAddress;
use crate::utils::misc::stack_depth::check_stack_depth;

// ---------------------------------------------------------------------------
// Constants (utility.h)
// ---------------------------------------------------------------------------

/// The command is never read-only.
pub const COMMAND_IS_NOT_READ_ONLY: c_int = 0x0000;
/// OK to execute in a read-only transaction.
pub const COMMAND_OK_IN_READ_ONLY_TXN: c_int = 0x0001;
/// OK to execute in parallel worker.
pub const COMMAND_OK_IN_PARALLEL_MODE: c_int = 0x0002;
/// OK to execute during recovery (implies the above two).
pub const COMMAND_OK_IN_RECOVERY: c_int = 0x0004;
/// The command is strictly read-only (all three bits).
pub const COMMAND_IS_STRICTLY_READ_ONLY: c_int =
    COMMAND_OK_IN_READ_ONLY_TXN | COMMAND_OK_IN_PARALLEL_MODE | COMMAND_OK_IN_RECOVERY;

/// Identifies the source context for ProcessUtility.
pub type ProcessUtilityContext = c_int;
pub const PROCESS_UTILITY_TOPLEVEL: ProcessUtilityContext = 0;
pub const PROCESS_UTILITY_QUERY: ProcessUtilityContext = 1;
pub const PROCESS_UTILITY_QUERY_NONATOMIC: ProcessUtilityContext = 2;
pub const PROCESS_UTILITY_SUBCOMMAND: ProcessUtilityContext = 3;

/*
 * Info needed to pass down to error reporting and to the underlying
 * ALTER TABLE implementation (defined in tcop/utility.h).
 */
#[repr(C)]
pub struct AlterTableUtilityContext {
    pub pstmt: *mut PlannedStmt,        /* PlannedStmt for outer ALTER TABLE command */
    pub queryString: *const c_char,     /* its query string */
    pub relid: Oid,                     /* OID of ALTER's target table */
    pub params: *mut ParamListInfo,     /* any parameters available to ALTER TABLE */
    pub queryEnv: *mut QueryEnvironment, /* execution environment for ALTER TABLE */
}

// ---------------------------------------------------------------------------
// Hook type (utility.h)
// ---------------------------------------------------------------------------

pub type ProcessUtility_hook_type = Option<
    unsafe fn(
        pstmt: *mut PlannedStmt,
        queryString: *const c_char,
        readOnlyTree: bool,
        context: ProcessUtilityContext,
        params: *mut ParamListInfo,
        queryEnv: *mut QueryEnvironment,
        dest: *mut DestReceiver,
        qc: *mut QueryCompletion,
    ),
>;

/* Hook for plugins to get control in ProcessUtility() */
pub static mut ProcessUtility_hook: ProcessUtility_hook_type = None;

// ---------------------------------------------------------------------------
// Stubs for as-yet-unported dependencies
// ---------------------------------------------------------------------------

// parser/parse_node.h
pub use crate::parser::parse_node::ParseState;
unsafe fn make_parsestate(_parent: *mut ParseState) -> *mut ParseState { crate::parser::parse_node::make_parsestate(_parent as _) }
unsafe fn free_parsestate(pstate: *mut ParseState) { crate::parser::parse_node::free_parsestate(pstate as _) }

// nodes/params.h
// C: typedef struct ParamListInfoData *ParamListInfo;
// Code in this file spells the parameter type as `*mut ParamListInfo`, matching
// the C "ParamListInfo params" pointer; alias ParamListInfo to the real
// ParamListInfoData struct so `*mut ParamListInfo` == the real ParamListInfo.
use crate::nodes::params::ParamListInfoData as ParamListInfo;

// utils/queryenvironment.h
pub use crate::utils::misc::queryenvironment::QueryEnvironment;

// access/xact.h
static mut XactReadOnly: bool = false;
unsafe fn IsTransactionBlock() -> bool { crate::access::transam::xact::IsTransactionBlock() }
unsafe fn IsInParallelMode() -> bool { crate::access::transam::xact::IsInParallelMode_real() }
unsafe fn RecoveryInProgress() -> bool { crate::access::transam::xlog::RecoveryInProgress() }
unsafe fn InSecurityRestrictedOperation() -> bool { crate::miscadmin::InSecurityRestrictedOperation() }
unsafe fn CommandCounterIncrement() { crate::access::transam::xact::CommandCounterIncrement() }
unsafe fn BeginTransactionBlock() { crate::access::transam::xact::BeginTransactionBlock() }
unsafe fn EndTransactionBlock(chain: bool) -> bool { crate::access::transam::xact::EndTransactionBlock(chain) }
unsafe fn PrepareTransactionBlock(gid: *mut c_char) -> bool { crate::access::transam::xact::PrepareTransactionBlock(gid as _) }
unsafe fn FinishPreparedTransaction(gid: *mut c_char, isCommit: bool) { crate::access::transam::twophase::FinishPreparedTransaction(gid as _, isCommit) }
unsafe fn UserAbortTransactionBlock(chain: bool) { crate::access::transam::xact::UserAbortTransactionBlock(chain) }
unsafe fn DefineSavepoint(name: *mut c_char) { crate::access::transam::xact::DefineSavepoint(name as _) }
unsafe fn ReleaseSavepoint(name: *mut c_char) { crate::access::transam::xact::ReleaseSavepoint(name as _) }
unsafe fn RollbackToSavepoint(name: *mut c_char) { crate::access::transam::xact::RollbackToSavepoint(name as _) }
unsafe fn PreventInTransactionBlock(_isTopLevel: bool, _stmttype: *const c_char) { crate::access::transam::xact::PreventInTransactionBlock(_isTopLevel, _stmttype as _) }
unsafe fn RequireTransactionBlock(_isTopLevel: bool, _stmttype: *const c_char) { crate::access::transam::xact::RequireTransactionBlock(_isTopLevel, _stmttype as _) }
unsafe fn WarnNoTransactionBlock(_isTopLevel: bool, _stmttype: *const c_char) { crate::access::transam::xact::WarnNoTransactionBlock(_isTopLevel, _stmttype as _) }

// access/xlog.h
unsafe fn RequestCheckpoint(_flags: c_int) { crate::postmaster::checkpointer::RequestCheckpoint(_flags as _) }
const CHECKPOINT_IMMEDIATE: c_int = 0x0001;
const CHECKPOINT_WAIT: c_int = 0x0002;
const CHECKPOINT_FORCE: c_int = 0x0008;

// utils/acl.h
unsafe fn has_privs_of_role(_member: Oid, _role: Oid) -> bool { crate::utils::adt::acl::has_privs_of_role(_member as _, _role as _) }

// catalog/pg_authid.h
const ROLE_PG_CHECKPOINT: Oid = 0; // TODO(pg-port): real OID

// miscadmin.h
unsafe fn GetUserId() -> Oid { crate::utils::init::miscinit::GetUserId() }
// BackendType
type BackendType = c_int;
static mut MyBackendType: BackendType = 0;
const B_BACKEND: BackendType = 0;

// utils/misc.h
unsafe fn superuser() -> bool { crate::miscadmin::superuser() }

// nodes/copyfuncs.h
unsafe fn copyObject(from: *mut PlannedStmt) -> *mut PlannedStmt { from }

// catalog/objectaddress.h
fn InvalidObjectAddress() -> ObjectAddress { unsafe { std::mem::zeroed() } }

// commands/copy.h
unsafe fn DoCopy(_pstate: *mut ParseState, _stmt: *mut CopyStmt, _stmt_location: c_int, _stmt_len: c_int, _processed: *mut u64) { crate::commands::copy::DoCopy(_pstate as _, _stmt as _, _stmt_location as _, _stmt_len as _, _processed as _) }

// commands/portalcmds.h
unsafe fn PerformCursorOpen(_pstate: *mut ParseState, _stmt: *mut DeclareCursorStmt, _params: *mut ParamListInfo, _isTopLevel: bool) { crate::commands::portalcmds::PerformCursorOpen(_pstate as _, _stmt as _, _params as _, _isTopLevel) }
unsafe fn PerformPortalClose(portalname: *mut c_char) { crate::commands::portalcmds::PerformPortalClose(portalname as _) }
unsafe fn PerformPortalFetch(_stmt: *mut FetchStmt, _dest: *mut DestReceiver, _qc: *mut QueryCompletion) { crate::commands::portalcmds::PerformPortalFetch(_stmt as _, _dest as _, _qc as _) }

// commands/prepare.h
unsafe fn PrepareQuery(_pstate: *mut ParseState, _stmt: *mut PrepareStmt, _stmt_location: c_int, _stmt_len: c_int) { crate::commands::prepare::PrepareQuery(_pstate as _, _stmt as _, _stmt_location as _, _stmt_len as _) }
unsafe fn ExecuteQuery(_pstate: *mut ParseState, _stmt: *mut ExecuteStmt, _plannedstmt: *mut PlannedStmt, _params: *mut ParamListInfo, _dest: *mut DestReceiver, _qc: *mut QueryCompletion) { crate::commands::prepare::ExecuteQuery(_pstate as _, _stmt as _, _plannedstmt as _, _params as _, _dest as _, _qc as _) }
unsafe fn DeallocateQuery(stmt: *mut DeallocateStmt) { crate::commands::prepare::DeallocateQuery(stmt as _) }
#[repr(C)] pub struct PreparedStatement { pub plansource: *mut CachedPlanSource }
#[repr(C)] pub struct CachedPlanSource { pub resultDesc: *mut TupleDescData, pub raw_parse_tree: *mut RawStmt }
unsafe fn FetchPreparedStatement(_name: *mut c_char, _throwError: bool) -> *mut PreparedStatement { crate::commands::prepare::FetchPreparedStatement(_name as _, _throwError) as _ }
unsafe fn FetchPreparedStatementResultDesc(_entry: *mut PreparedStatement) -> TupleDesc { crate::commands::prepare::FetchPreparedStatementResultDesc(_entry as _) as _ }

// commands/explain.h
unsafe fn ExplainQuery(_pstate: *mut ParseState, _stmt: *mut ExplainStmt, _params: *mut ParamListInfo, _dest: *mut DestReceiver) { crate::commands::explain::ExplainQuery(_pstate as _, _stmt as _, _params as _, _dest as _) }
unsafe fn ExplainResultDesc(stmt: *mut ExplainStmt) -> TupleDesc { crate::commands::explain::ExplainResultDesc(stmt as _) as _ }

// commands/defrem.h
unsafe fn defGetBoolean(_def: *mut DefElem) -> bool { crate::commands::define::defGetBoolean(_def as _) }

// tcop/dest.h
use crate::tcop::dest::None_Receiver;
#[repr(C)] pub struct TupleDescData { _opaque: [u8; 0] }
pub type TupleDesc = *mut TupleDescData;

// access/common/tupdesc.h
unsafe fn CreateTupleDescCopy(tupdesc: TupleDesc) -> TupleDesc { crate::access::common::tupdesc::CreateTupleDescCopy(tupdesc as _) as _ }

// commands/tablespace.h
unsafe fn CreateTableSpace(stmt: *mut CreateTableSpaceStmt) { crate::commands::tablespace::CreateTableSpace(stmt as _); }
unsafe fn DropTableSpace(stmt: *mut DropTableSpaceStmt) { crate::commands::tablespace::DropTableSpace(stmt as _) }
unsafe fn AlterTableSpaceOptions(stmt: *mut AlterTableSpaceOptionsStmt) { crate::commands::tablespace::AlterTableSpaceOptions(stmt as _); }

// commands/tablecmds.h
unsafe fn ExecuteTruncate(stmt: *mut TruncateStmt) { crate::commands::tablecmds::ExecuteTruncate(stmt as _) }
unsafe fn AlterTableGetLockLevel(cmds: *mut List) -> LOCKMODE { crate::commands::tablecmds::AlterTableGetLockLevel(cmds as _) }
unsafe fn AlterTableLookupRelation(stmt: *mut AlterTableStmt, lockmode: LOCKMODE) -> Oid { crate::commands::tablecmds::AlterTableLookupRelation(stmt as _, lockmode as _) }
unsafe fn AlterTable(stmt: *mut AlterTableStmt, lockmode: LOCKMODE, context: *mut AlterTableUtilityContext) { crate::commands::tablecmds::AlterTable(stmt as _, lockmode as _, context as _) }
unsafe fn AlterTableMoveAll(stmt: *mut AlterTableMoveAllStmt) { crate::commands::tablecmds::AlterTableMoveAll(stmt as _); }

// commands/async.h
unsafe fn Async_Notify(conditionname: *mut c_char, payload: *mut c_char) { crate::commands::r#async::Async_Notify(conditionname as _, payload as _) }
unsafe fn Async_Listen(conditionname: *mut c_char) { crate::commands::r#async::Async_Listen(conditionname as _) }
unsafe fn Async_Unlisten(conditionname: *mut c_char) { crate::commands::r#async::Async_Unlisten(conditionname as _) }
unsafe fn Async_UnlistenAll() { crate::commands::r#async::Async_UnlistenAll() }

// storage/fd.h
unsafe fn closeAllVfds() { crate::storage::file::fd::closeAllVfds() }
unsafe fn load_file(filename: *mut c_char, restricted: bool) { crate::utils::fmgr::dfmgr::load_file(filename as _, restricted) }

// commands/proclang.h
unsafe fn ExecuteCallStmt(stmt: *mut CallStmt, params: *mut ParamListInfo, atomic: bool, dest: *mut DestReceiver) { crate::commands::functioncmds::ExecuteCallStmt(stmt as _, params as _, atomic, dest as _) }
unsafe fn CallStmtResultDesc(stmt: *mut CallStmt) -> TupleDesc { crate::commands::functioncmds::CallStmtResultDesc(stmt as _) as _ }

// commands/cluster.h
unsafe fn cluster(pstate: *mut ParseState, stmt: *mut ClusterStmt, isTopLevel: bool) { unimplemented!() }

// commands/vacuum.h
unsafe fn ExecVacuum(_pstate: *mut ParseState, _stmt: *mut VacuumStmt, _isTopLevel: bool) { crate::commands::vacuum::ExecVacuum(_pstate as _, _stmt as _, _isTopLevel) }

// commands/discard.h
unsafe fn DiscardCommand(stmt: *mut DiscardStmt, isTopLevel: bool) { crate::commands::discard::DiscardCommand(stmt as _, isTopLevel) }

// commands/event_trigger.h
unsafe fn CreateEventTrigger(stmt: *mut CreateEventTrigStmt) { unimplemented!() }
unsafe fn AlterEventTrigger(_stmt: *mut AlterEventTrigStmt) { todo!("TODO(pg-port): AlterEventTrigger") }
unsafe fn EventTriggerSupportsObjectType(_objtype: ObjectType) -> bool { false }
unsafe fn EventTriggerBeginCompleteQuery() -> bool { false }
unsafe fn EventTriggerEndCompleteQuery() {}
unsafe fn EventTriggerDDLCommandStart(_parsetree: *mut Node) {}
unsafe fn EventTriggerDDLCommandEnd(_parsetree: *mut Node) {}
unsafe fn EventTriggerSQLDrop(_parsetree: *mut Node) {}
unsafe fn EventTriggerCollectSimpleCommand(_address: ObjectAddress, _secondaryObject: ObjectAddress, _parsetree: *mut Node) {}
unsafe fn EventTriggerCollectAlterDefPrivs(_stmt: *mut AlterDefaultPrivilegesStmt) {}
unsafe fn EventTriggerAlterTableStart(_parsetree: *mut Node) {}
unsafe fn EventTriggerAlterTableEnd() {}
unsafe fn EventTriggerAlterTableRelid(_relid: Oid) {}
unsafe fn EventTriggerInhibitCommandCollection() {}
unsafe fn EventTriggerUndoInhibitCommandCollection() {}

// commands/user.h
unsafe fn CreateRole(pstate: *mut ParseState, stmt: *mut CreateRoleStmt) { unimplemented!() }
unsafe fn AlterRole(pstate: *mut ParseState, stmt: *mut AlterRoleStmt) { unimplemented!() }
unsafe fn AlterRoleSet(stmt: *mut AlterRoleSetStmt) { unimplemented!() }
unsafe fn DropRole(stmt: *mut DropRoleStmt) { unimplemented!() }
unsafe fn ReassignOwnedObjects(stmt: *mut ReassignOwnedStmt) { unimplemented!() }
unsafe fn DropOwnedObjects(stmt: *mut DropOwnedStmt) { unimplemented!() }
unsafe fn GrantRole(pstate: *mut ParseState, stmt: *mut GrantRoleStmt) { unimplemented!() }

// commands/dbcommands.h
unsafe fn createdb(_pstate: *mut ParseState, _stmt: *mut CreatedbStmt) { let _ = crate::commands::dbcommands::createdb(_pstate as _, _stmt as _); }
unsafe fn AlterDatabase(pstate: *mut ParseState, stmt: *mut AlterDatabaseStmt, isTopLevel: bool) { crate::commands::dbcommands::AlterDatabase(pstate as _, stmt as _, isTopLevel); }
unsafe fn AlterDatabaseRefreshColl(stmt: *mut AlterDatabaseRefreshCollStmt) { crate::commands::dbcommands::AlterDatabaseRefreshColl(stmt as _); }
unsafe fn AlterDatabaseSet(_stmt: *mut AlterDatabaseSetStmt) { let _ = crate::commands::dbcommands::AlterDatabaseSet(_stmt as _); }
unsafe fn DropDatabase(pstate: *mut ParseState, stmt: *mut DropdbStmt) { crate::commands::dbcommands::DropDatabase(pstate as _, stmt as _) }

// commands/lockcmds.h
unsafe fn LockTableCommand(stmt: *mut LockStmt) { crate::commands::lockcmds::LockTableCommand(stmt as _) }

// commands/trigger.h
unsafe fn AfterTriggerSetState(stmt: *mut ConstraintsSetStmt) { crate::commands::trigger::AfterTriggerSetState(stmt as _) }

// utils/guc.h
unsafe fn SetPGVariable(name: *const c_char, args: *mut List, doit: bool) {
    crate::utils::misc::guc_funcs::SetPGVariable(name, args as _, doit)
}
unsafe fn GetPGVariable(name: *mut c_char, dest: *mut DestReceiver) {
    crate::utils::misc::guc_funcs::GetPGVariable(name, dest as _)
}
unsafe fn ExecSetVariableStmt(stmt: *mut VariableSetStmt, isTopLevel: bool) {
    crate::utils::misc::guc_funcs::ExecSetVariableStmt(stmt as _, isTopLevel)
}
unsafe fn GetPGVariableResultDesc(name: *mut c_char) -> TupleDesc {
    crate::utils::misc::guc_funcs::GetPGVariableResultDesc(name) as _
}
// guc AlterSystem
unsafe fn AlterSystemSetConfigFile(stmt: *mut AlterSystemStmt) { crate::utils::misc::guc::AlterSystemSetConfigFile(stmt as _) }

// commands/do.h
unsafe fn ExecuteDoStmt(_pstate: *mut ParseState, _stmt: *mut DoStmt, _atomic: bool) { crate::commands::functioncmds::ExecuteDoStmt(_pstate as _, _stmt as _, _atomic) }

// commands/schemacmds.h
unsafe fn CreateSchemaCommand(_stmt: *mut CreateSchemaStmt, _queryString: *const c_char, _stmt_location: c_int, _stmt_len: c_int) { let _ = crate::commands::schemacmds::CreateSchemaCommand(_stmt as _, _queryString as _, _stmt_location as _, _stmt_len as _); }

// parser/parse_utilcmd.h
unsafe fn transformCreateStmt(_stmt: *mut CreateStmt, _queryString: *const c_char) -> *mut List { crate::parser::parse_utilcmd::transformCreateStmt(_stmt as _, _queryString as _) }
unsafe fn transformIndexStmt(_relid: Oid, _stmt: *mut IndexStmt, _queryString: *const c_char) -> *mut IndexStmt { crate::parser::parse_utilcmd::transformIndexStmt(_relid as _, _stmt as _, _queryString as _) as _ }
unsafe fn transformStatsStmt(_relid: Oid, _stmt: *mut CreateStatsStmt, _queryString: *const c_char) -> *mut CreateStatsStmt { crate::parser::parse_utilcmd::transformStatsStmt(_relid as _, _stmt as _, _queryString as _) as _ }
unsafe fn expandTableLikeClause(_table_rv: *mut RangeVar, _like: *mut TableLikeClause) -> *mut List { crate::parser::parse_utilcmd::expandTableLikeClause(_table_rv as _, _like as _) as _ }

// nodes/parsenodes.h -- misc stubs for node types
#[repr(C)] pub struct TableLikeClause { _opaque: [u8; 0] }
#[repr(C)] pub struct PartitionCmd { pub concurrent: bool, _opaque: [u8; 0] }

// commands/tablecmds.h
unsafe fn DefineRelation(_stmt: *mut CreateStmt, _relkind: c_char, _ownerId: Oid, _typaddress: *mut ObjectAddress, _queryString: *const c_char) -> ObjectAddress { crate::commands::tablecmds::DefineRelation(_stmt as _, _relkind as _, _ownerId as _, _typaddress as _, _queryString as _) }
unsafe fn RemoveRelations(_stmt: *mut DropStmt) { crate::commands::tablecmds::RemoveRelations(_stmt as _) }
unsafe fn RemoveObjects(stmt: *mut DropStmt) { unimplemented!() }

// catalog/toasting.h
unsafe fn NewRelationCreateToastTable(_relOid: Oid, _reloptions: Datum) { crate::catalog::toasting::NewRelationCreateToastTable(_relOid as _, _reloptions as _) }
// relkind values (catalog/pg_class.h)
const RELKIND_RELATION: c_char = b'r' as c_char;
const RELKIND_FOREIGN_TABLE: c_char = b'f' as c_char;
const RELKIND_PARTITIONED_TABLE: c_char = b'p' as c_char;
const RELKIND_MATVIEW: c_char = b'm' as c_char;
const RELKIND_TOASTVALUE: c_char = b't' as c_char;

// access/reloptions.h
unsafe fn transformRelOptions(_oldOptions: Datum, _defList: *mut List, _namspace: *const c_char, _validnsps: *const *const c_char, _acceptOidsOff: bool, _isReset: bool) -> Datum {
    crate::access::common::reloptions::transformRelOptions(_oldOptions, _defList as _, _namspace, _validnsps, _acceptOidsOff, _isReset)
}
unsafe fn heap_reloptions(_relkind: c_char, _reloptions: Datum, _validate: bool) -> Datum {
    PointerGetDatum(crate::access::common::reloptions::heap_reloptions(_relkind, _reloptions, _validate) as *const c_void)
}
macro_rules! HEAP_RELOPT_NAMESPACES {
    () => { [null::<c_char>() as *const c_char, null::<c_char>() as *const c_char] }
}

// commands/createas.h
unsafe fn ExecCreateTableAs(_pstate: *mut ParseState, _stmt: *mut CreateTableAsStmt, _params: *mut ParamListInfo, _queryEnv: *mut QueryEnvironment, _qc: *mut QueryCompletion) -> ObjectAddress { crate::commands::createas::ExecCreateTableAs(_pstate as _, _stmt as _, _params as _, _queryEnv as _, _qc as _) as _ }
unsafe fn ExecRefreshMatView(_stmt: *mut RefreshMatViewStmt, _queryString: *const c_char, _qc: *mut QueryCompletion) -> ObjectAddress { crate::commands::matview::ExecRefreshMatView(_stmt as _, _queryString as _, _qc as _) as _ }

// commands/view.h
unsafe fn DefineView(_stmt: *mut ViewStmt, _queryString: *const c_char, _stmt_location: c_int, _stmt_len: c_int) -> ObjectAddress { core::mem::transmute(crate::commands::view::DefineView(_stmt as _, _queryString as _, _stmt_location, _stmt_len)) }

// commands/defrem.h - functions
unsafe fn DefineAggregate(_pstate: *mut ParseState, _name: *mut List, _args: *mut List, _oldstyle: bool, _definition: *mut List, _replace: bool) -> ObjectAddress { crate::commands::aggregatecmds::DefineAggregate(_pstate as _, _name as _, _args as _, _oldstyle, _definition as _, _replace) as _ }
unsafe fn DefineOperator(_name: *mut List, _definition: *mut List) -> ObjectAddress { crate::commands::operatorcmds::DefineOperator(_name as _, _definition as _) as _ }
unsafe fn DefineType(_pstate: *mut ParseState, _name: *mut List, _definition: *mut List) -> ObjectAddress { crate::commands::typecmds::DefineType(_pstate as _, _name as _, _definition as _) as _ }
unsafe fn DefineTSParser(name: *mut List, definition: *mut List) -> ObjectAddress { unimplemented!() }
unsafe fn DefineTSDictionary(name: *mut List, definition: *mut List) -> ObjectAddress { unimplemented!() }
unsafe fn DefineTSTemplate(name: *mut List, definition: *mut List) -> ObjectAddress { unimplemented!() }
unsafe fn DefineTSConfiguration(name: *mut List, definition: *mut List, secondary: *mut ObjectAddress) -> ObjectAddress { unimplemented!() }
unsafe fn DefineCollation(_pstate: *mut ParseState, _name: *mut List, _definition: *mut List, _if_not_exists: bool) -> ObjectAddress { todo!("TODO(pg-port): DefineCollation") }

// commands/indexcmds.h
unsafe fn DefineIndex(_relationId: Oid, _stmt: *mut IndexStmt, _indexRelationId: Oid, _parentIndexId: Oid, _parentConstraintId: Oid, _nparts: c_int, _is_alter_table: bool, _check_rights: bool, _check_not_in_use: bool, _skip_build: bool, _quiet: bool) -> ObjectAddress { crate::commands::indexcmds::DefineIndex(_relationId as _, _stmt as _, _indexRelationId as _, _parentIndexId as _, _parentConstraintId as _, _nparts as _, _is_alter_table, _check_rights, _check_not_in_use, _skip_build, _quiet) }
unsafe fn ExecReindex(_pstate: *mut ParseState, _stmt: *mut ReindexStmt, _isTopLevel: bool) { crate::commands::indexcmds::ExecReindex(_pstate as _, _stmt as _, _isTopLevel) }

// catalog/pg_inherits.h
unsafe fn find_all_inheritors(_parentRelId: Oid, _lockmode: LOCKMODE, _numparents: *mut c_int) -> *mut List { crate::catalog::pg_inherits::find_all_inheritors(_parentRelId as _, _lockmode as _, _numparents as _) as _ }

// utils/lsyscache.h
unsafe fn get_rel_relkind(_relid: Oid) -> c_char { crate::utils::cache::lsyscache::get_rel_relkind(_relid as _) }
unsafe fn RangeVarGetRelid(relation: *mut RangeVar, lockmode: LOCKMODE, missing_ok: bool) -> Oid { crate::catalog::namespace::RangeVarGetRelid(relation as _, lockmode as _, missing_ok) }
unsafe fn RangeVarGetRelidExtended(relation: *mut RangeVar, lockmode: LOCKMODE, flags: u32, callback: crate::catalog::namespace::RangeVarGetRelidCallback, callback_arg: *mut c_void) -> Oid { crate::catalog::namespace::RangeVarGetRelidExtended(relation as _, lockmode as _, flags as _, callback, callback_arg) }
unsafe extern "C" fn RangeVarCallbackOwnsRelation(relation: *const RangeVar, relId: Oid, oldRelId: Oid, arg: *mut c_void) { crate::commands::tablecmds::RangeVarCallbackOwnsRelation(relation as _, relId as _, oldRelId as _, arg as _); }
// OidIsValid
macro_rules! OidIsValid { ($oid:expr) => { $oid != InvalidOid } }
const InvalidOid: Oid = 0;

// commands/extension.h
unsafe fn CreateExtension(pstate: *mut ParseState, stmt: *mut CreateExtensionStmt) -> ObjectAddress { crate::commands::extension::CreateExtension(pstate as _, stmt as _) }
unsafe fn ExecAlterExtensionStmt(_pstate: *mut ParseState, _stmt: *mut AlterExtensionStmt) -> ObjectAddress { crate::commands::extension::ExecAlterExtensionStmt(_pstate as _, _stmt as _) }
unsafe fn ExecAlterExtensionContentsStmt(_stmt: *mut AlterExtensionContentsStmt, _secondary: *mut ObjectAddress) -> ObjectAddress { crate::commands::extension::ExecAlterExtensionContentsStmt(_stmt as _, _secondary as _) }

// commands/foreigncmds.h
unsafe fn CreateForeignDataWrapper(pstate: *mut ParseState, stmt: *mut CreateFdwStmt) -> ObjectAddress { unimplemented!() }
unsafe fn AlterForeignDataWrapper(pstate: *mut ParseState, stmt: *mut AlterFdwStmt) -> ObjectAddress { unimplemented!() }
unsafe fn CreateForeignServer(stmt: *mut CreateForeignServerStmt) -> ObjectAddress { unimplemented!() }
unsafe fn AlterForeignServer(stmt: *mut AlterForeignServerStmt) -> ObjectAddress { unimplemented!() }
unsafe fn CreateUserMapping(stmt: *mut CreateUserMappingStmt) -> ObjectAddress { unimplemented!() }
unsafe fn AlterUserMapping(stmt: *mut AlterUserMappingStmt) -> ObjectAddress { unimplemented!() }
unsafe fn RemoveUserMapping(stmt: *mut DropUserMappingStmt) { unimplemented!() }
unsafe fn ImportForeignSchema(stmt: *mut ImportForeignSchemaStmt) { unimplemented!() }
unsafe fn CreateForeignTable(stmt: *mut CreateForeignTableStmt, relid: Oid) { unimplemented!() }

// commands/typecmds.h
unsafe fn DefineCompositeType(typevar: *mut RangeVar, coldeflist: *mut List) -> ObjectAddress { crate::commands::typecmds::DefineCompositeType(typevar as _, coldeflist as _) }
unsafe fn DefineEnum(_stmt: *mut CreateEnumStmt) -> ObjectAddress { crate::commands::typecmds::DefineEnum(_stmt as _) as _ }
unsafe fn DefineRange(_pstate: *mut ParseState, _stmt: *mut CreateRangeStmt) -> ObjectAddress { crate::commands::typecmds::DefineRange(_pstate as _, _stmt as _) as _ }
unsafe fn AlterEnum(stmt: *mut AlterEnumStmt) -> ObjectAddress { crate::commands::typecmds::AlterEnum(stmt as _) }
unsafe fn DefineDomain(pstate: *mut ParseState, stmt: *mut CreateDomainStmt) -> ObjectAddress { crate::commands::typecmds::DefineDomain(pstate as _, stmt as _) }
unsafe fn AlterDomainDefault(typeName: *mut List, defaultRaw: *mut Node) -> ObjectAddress { crate::commands::typecmds::AlterDomainDefault(typeName as _, defaultRaw as _) }
unsafe fn AlterDomainNotNull(typeName: *mut List, notNull: bool) -> ObjectAddress { crate::commands::typecmds::AlterDomainNotNull(typeName as _, notNull) }
unsafe fn AlterDomainAddConstraint(_typeName: *mut List, _constr: *mut Node, _secondary: *mut ObjectAddress) -> ObjectAddress { crate::commands::typecmds::AlterDomainAddConstraint(_typeName as _, _constr as _, _secondary as _) }
unsafe fn AlterDomainDropConstraint(_typeName: *mut List, _constrName: *const c_char, _behavior: DropBehavior, _missing_ok: bool) -> ObjectAddress { crate::commands::typecmds::AlterDomainDropConstraint(_typeName as _, _constrName as _, _behavior as _, _missing_ok) }
unsafe fn AlterDomainValidateConstraint(typeName: *mut List, constrName: *const c_char) -> ObjectAddress { crate::commands::typecmds::AlterDomainValidateConstraint(typeName as _, constrName as _) }
unsafe fn AlterType(stmt: *mut AlterTypeStmt) -> ObjectAddress { crate::commands::typecmds::AlterType(stmt as _) }

// commands/functioncmds.h
unsafe fn CreateFunction(_pstate: *mut ParseState, _stmt: *mut CreateFunctionStmt) -> ObjectAddress { crate::commands::functioncmds::CreateFunction(_pstate as _, _stmt as _) as _ }
unsafe fn AlterFunction(_pstate: *mut ParseState, _stmt: *mut AlterFunctionStmt) -> ObjectAddress { crate::commands::functioncmds::AlterFunction(_pstate as _, _stmt as _) as _ }

// rewrite/rewriteDefine.h
unsafe fn DefineRule(stmt: *mut RuleStmt, queryString: *const c_char) -> ObjectAddress { crate::rewrite::rewriteDefine::DefineRule(stmt as _, queryString as _) }

// commands/sequence.h
unsafe fn DefineSequence(pstate: *mut ParseState, stmt: *mut CreateSeqStmt) -> ObjectAddress { crate::commands::sequence::DefineSequence(pstate as _, stmt as _) }
unsafe fn AlterSequence(pstate: *mut ParseState, stmt: *mut AlterSeqStmt) -> ObjectAddress { crate::commands::sequence::AlterSequence(pstate as _, stmt as _) }

// commands/trigger.h
unsafe fn CreateTrigger(_stmt: *mut CreateTrigStmt, _queryString: *const c_char, _relOid: Oid, _refRelOid: Oid, _constraintOid: Oid, _indexOid: Oid, _funcoid: Oid, _parentTriggerOid: Oid, _whenClause: *mut Node, _isInternal: bool, _in_partition: bool) -> ObjectAddress { crate::commands::trigger::CreateTrigger(_stmt as _, _queryString as _, _relOid as _, _refRelOid as _, _constraintOid as _, _indexOid as _, _funcoid as _, _parentTriggerOid as _, _whenClause as _, _isInternal, _in_partition) }

// commands/proclang.h
unsafe fn CreateProceduralLanguage(stmt: *mut CreatePLangStmt) -> ObjectAddress { crate::commands::proclang::CreateProceduralLanguage(stmt as _) }

// commands/conversioncmds.h
unsafe fn CreateConversionCommand(stmt: *mut CreateConversionStmt) -> ObjectAddress { crate::commands::conversioncmds::CreateConversionCommand(stmt as _) }

// commands/createcas.h
unsafe fn CreateCast(_stmt: *mut CreateCastStmt) -> ObjectAddress { crate::commands::functioncmds::CreateCast(_stmt as _) as _ }

// commands/opclasscmds.h
unsafe fn DefineOpClass(_stmt: *mut CreateOpClassStmt) { let _ = crate::commands::opclasscmds::DefineOpClass(_stmt as _); }
unsafe fn DefineOpFamily(_stmt: *mut CreateOpFamilyStmt) -> ObjectAddress { crate::commands::opclasscmds::DefineOpFamily(_stmt as _) as _ }
unsafe fn AlterOpFamily(_stmt: *mut AlterOpFamilyStmt) { let _ = crate::commands::opclasscmds::AlterOpFamily(_stmt as _); }

// commands/alter.h
unsafe fn CreateTransform(stmt: *mut CreateTransformStmt) -> ObjectAddress { crate::commands::functioncmds::CreateTransform(stmt as _) }
unsafe fn ExecRenameStmt(stmt: *mut RenameStmt) -> ObjectAddress { crate::commands::alter::ExecRenameStmt(stmt as _) }
unsafe fn ExecAlterObjectDependsStmt(_stmt: *mut AlterObjectDependsStmt, _secondary: *mut ObjectAddress) -> ObjectAddress { crate::commands::alter::ExecAlterObjectDependsStmt(_stmt as _, _secondary as _) }
unsafe fn ExecAlterObjectSchemaStmt(_stmt: *mut AlterObjectSchemaStmt, _secondary: *mut ObjectAddress) -> ObjectAddress { crate::commands::alter::ExecAlterObjectSchemaStmt(_stmt as _, _secondary as _) }
unsafe fn ExecAlterOwnerStmt(stmt: *mut AlterOwnerStmt) -> ObjectAddress { crate::commands::alter::ExecAlterOwnerStmt(stmt as _) }
unsafe fn AlterOperator(stmt: *mut AlterOperatorStmt) -> ObjectAddress { crate::commands::operatorcmds::AlterOperator(stmt as _) }
// TODO(pg-port): wire crate::catalog::aclchk::ExecuteGrantStmt (pulls in unwired ACL
// deps). Superuser (regression owner) bypasses ACL checks, so no-op for now.
unsafe fn ExecGrantStmt(_stmt: *mut GrantStmt) {}
unsafe fn ExecAlterDefaultPrivilegesStmt(_pstate: *mut ParseState, _stmt: *mut AlterDefaultPrivilegesStmt) { todo!("TODO(pg-port): ExecAlterDefaultPrivilegesStmt") }

// commands/tsearchcmds.h
unsafe fn AlterTSDictionary(stmt: *mut AlterTSDictionaryStmt) -> ObjectAddress { unimplemented!() }
unsafe fn AlterTSConfiguration(stmt: *mut AlterTSConfigurationStmt) { unimplemented!() }

// commands/policy.h
unsafe fn CreatePolicy(stmt: *mut CreatePolicyStmt) -> ObjectAddress { crate::commands::policy::CreatePolicy(stmt as _) }
unsafe fn AlterPolicy(stmt: *mut AlterPolicyStmt) -> ObjectAddress { crate::commands::policy::AlterPolicy(stmt as _) }

// commands/seclabel.h
unsafe fn ExecSecLabelStmt(stmt: *mut SecLabelStmt) -> ObjectAddress { crate::commands::seclabel::ExecSecLabelStmt(stmt as _) }

// commands/amcmds.h
unsafe fn CreateAccessMethod(stmt: *mut CreateAmStmt) -> ObjectAddress { crate::commands::amcmds::CreateAccessMethod(stmt as _) }

// commands/publicationcmds.h
unsafe fn CreatePublication(_pstate: *mut ParseState, _stmt: *mut CreatePublicationStmt) -> ObjectAddress { todo!("TODO(pg-port): CreatePublication") }
unsafe fn AlterPublication(pstate: *mut ParseState, stmt: *mut AlterPublicationStmt) { unimplemented!() }

// commands/subscriptioncmds.h
unsafe fn CreateSubscription(_pstate: *mut ParseState, _stmt: *mut CreateSubscriptionStmt, _isTopLevel: bool) -> ObjectAddress { todo!("TODO(pg-port): CreateSubscription") }
unsafe fn AlterSubscription(_pstate: *mut ParseState, _stmt: *mut AlterSubscriptionStmt, _isTopLevel: bool) -> ObjectAddress { todo!("TODO(pg-port): AlterSubscription") }
unsafe fn DropSubscription(stmt: *mut DropSubscriptionStmt, isTopLevel: bool) { unimplemented!() }

// commands/statscmds.h
unsafe fn CreateStatistics(stmt: *mut CreateStatsStmt, replace: bool) -> ObjectAddress { crate::commands::statscmds::CreateStatistics(stmt as _, replace) }
unsafe fn AlterStatistics(stmt: *mut AlterStatsStmt) -> ObjectAddress { crate::commands::statscmds::AlterStatistics(stmt as _) }

// commands/collationcmds.h
unsafe fn AlterCollation(stmt: *mut AlterCollationStmt) -> ObjectAddress { unimplemented!() }

// commands/comment.h
unsafe fn CommentObject(stmt: *mut CommentStmt) -> ObjectAddress { crate::commands::comment::CommentObject(stmt as _) }

// commands/prepare.h Portal API
#[repr(C)] pub struct Portal { pub tupDesc: TupleDesc, _opaque: [u8; 0] }
unsafe fn GetPortalByName(name: *mut c_char) -> *mut Portal { crate::utils::mmgr::portalmem::GetPortalByName(name as _) as _ }
unsafe fn PortalIsValid(portal: *mut Portal) -> bool { crate::utils::portal::PortalIsValid(portal as _) }

// catalog/pg_type_d.h
const RECORDOID: Oid = 2249;

// nodes/pg_list.h helpers
unsafe fn linitial(list: *mut List) -> *mut c_void { crate::nodes::pg_list::linitial(list as _) }
unsafe fn list_delete_first(list: *mut List) -> *mut List { crate::nodes::pg_list::list_delete_first(list as _) }
unsafe fn list_concat(list1: *mut List, list2: *mut List) -> *mut List { crate::nodes::pg_list::list_concat(list1 as _, list2 as _) }
unsafe fn list_free(list: *mut List) { crate::nodes::pg_list::list_free(list as _) }
unsafe fn list_length(list: *mut List) -> c_int { crate::nodes::pg_list::list_length(list as _) }
unsafe fn list_make1(item: *mut Node) -> *mut List { crate::list_make1!(item) }
unsafe fn lfirst_oid(lc: *mut crate::nodes::pg_list::ListCell) -> Oid { crate::nodes::pg_list::lfirst_oid(lc as _) }
unsafe fn lfirst(lc: *mut crate::nodes::pg_list::ListCell) -> *mut c_void { crate::nodes::pg_list::lfirst(lc as _) }

// libc
extern "C" {
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
}

// DropBehavior (parsenodes.h) -- use the real enum from parsenodes

// ---------------------------------------------------------------------------
// CommandIsReadOnly
// ---------------------------------------------------------------------------

/*
 * CommandIsReadOnly: is an executable query read-only?
 *
 * This is a much stricter test than we apply for XactReadOnly mode;
 * the query must be *in truth* read-only, because the caller wishes
 * not to do CommandCounterIncrement for it.
 *
 * Note: currently no need to support raw or analyzed queries here
 */
pub unsafe fn CommandIsReadOnly(pstmt: *mut PlannedStmt) -> bool {
    Assert!(IsA!(pstmt, T_PlannedStmt));
    match (*pstmt).commandType {
        CMD_SELECT => {
            if (*pstmt).rowMarks != NIL {
                return false; /* SELECT FOR [KEY] UPDATE/SHARE */
            } else if (*pstmt).hasModifyingCTE {
                return false; /* data-modifying CTE */
            } else {
                return true;
            }
        }
        CMD_UPDATE | CMD_INSERT | CMD_DELETE | CMD_MERGE => {
            return false;
        }
        CMD_UTILITY => {
            /* For now, treat all utility commands as read/write */
            return false;
        }
        _ => {
            elog!(WARNING, "unrecognized commandType: {}", (*pstmt).commandType as c_int);
        }
    }
    false
}

/*
 * Determine the degree to which a utility command is read only.
 *
 * Note the definitions of the relevant flags in src/include/tcop/utility.h.
 */
unsafe fn ClassifyUtilityCommandAsReadOnly(parsetree: *mut Node) -> c_int {
    match nodeTag(parsetree) {
        T_AlterCollationStmt
        | T_AlterDatabaseRefreshCollStmt
        | T_AlterDatabaseSetStmt
        | T_AlterDatabaseStmt
        | T_AlterDefaultPrivilegesStmt
        | T_AlterDomainStmt
        | T_AlterEnumStmt
        | T_AlterEventTrigStmt
        | T_AlterExtensionContentsStmt
        | T_AlterExtensionStmt
        | T_AlterFdwStmt
        | T_AlterForeignServerStmt
        | T_AlterFunctionStmt
        | T_AlterObjectDependsStmt
        | T_AlterObjectSchemaStmt
        | T_AlterOpFamilyStmt
        | T_AlterOperatorStmt
        | T_AlterOwnerStmt
        | T_AlterPolicyStmt
        | T_AlterPublicationStmt
        | T_AlterRoleSetStmt
        | T_AlterRoleStmt
        | T_AlterSeqStmt
        | T_AlterStatsStmt
        | T_AlterSubscriptionStmt
        | T_AlterTSConfigurationStmt
        | T_AlterTSDictionaryStmt
        | T_AlterTableMoveAllStmt
        | T_AlterTableSpaceOptionsStmt
        | T_AlterTableStmt
        | T_AlterTypeStmt
        | T_AlterUserMappingStmt
        | T_CommentStmt
        | T_CompositeTypeStmt
        | T_CreateAmStmt
        | T_CreateCastStmt
        | T_CreateConversionStmt
        | T_CreateDomainStmt
        | T_CreateEnumStmt
        | T_CreateEventTrigStmt
        | T_CreateExtensionStmt
        | T_CreateFdwStmt
        | T_CreateForeignServerStmt
        | T_CreateForeignTableStmt
        | T_CreateFunctionStmt
        | T_CreateOpClassStmt
        | T_CreateOpFamilyStmt
        | T_CreatePLangStmt
        | T_CreatePolicyStmt
        | T_CreatePublicationStmt
        | T_CreateRangeStmt
        | T_CreateRoleStmt
        | T_CreateSchemaStmt
        | T_CreateSeqStmt
        | T_CreateStatsStmt
        | T_CreateStmt
        | T_CreateSubscriptionStmt
        | T_CreateTableAsStmt
        | T_CreateTableSpaceStmt
        | T_CreateTransformStmt
        | T_CreateTrigStmt
        | T_CreateUserMappingStmt
        | T_CreatedbStmt
        | T_DefineStmt
        | T_DropOwnedStmt
        | T_DropRoleStmt
        | T_DropStmt
        | T_DropSubscriptionStmt
        | T_DropTableSpaceStmt
        | T_DropUserMappingStmt
        | T_DropdbStmt
        | T_GrantRoleStmt
        | T_GrantStmt
        | T_ImportForeignSchemaStmt
        | T_IndexStmt
        | T_ReassignOwnedStmt
        | T_RefreshMatViewStmt
        | T_RenameStmt
        | T_RuleStmt
        | T_SecLabelStmt
        | T_TruncateStmt
        | T_ViewStmt => {
            /* DDL is not read-only, and neither is TRUNCATE. */
            COMMAND_IS_NOT_READ_ONLY
        }

        T_AlterSystemStmt => {
            /*
             * Surprisingly, ALTER SYSTEM meets all our definitions of
             * read-only: it changes nothing that affects the output of
             * pg_dump, it doesn't write WAL or imperil the application of
             * future WAL, and it doesn't depend on any state that needs
             * to be synchronized with parallel workers.
             *
             * So, despite the fact that it writes to a file, it's read
             * only!
             */
            COMMAND_IS_STRICTLY_READ_ONLY
        }

        T_CallStmt | T_DoStmt => {
            /*
             * Commands inside the DO block or the called procedure might
             * not be read only, but they'll be checked separately when we
             * try to execute them.  Here we only need to worry about the
             * DO or CALL command itself.
             */
            COMMAND_IS_STRICTLY_READ_ONLY
        }

        T_CheckPointStmt => {
            /*
             * You might think that this should not be permitted in
             * recovery, but we interpret a CHECKPOINT command during
             * recovery as a request for a restartpoint instead. We allow
             * this since it can be a useful way of reducing switchover
             * time when using various forms of replication.
             */
            COMMAND_IS_STRICTLY_READ_ONLY
        }

        T_ClosePortalStmt
        | T_ConstraintsSetStmt
        | T_DeallocateStmt
        | T_DeclareCursorStmt
        | T_DiscardStmt
        | T_ExecuteStmt
        | T_FetchStmt
        | T_LoadStmt
        | T_PrepareStmt
        | T_UnlistenStmt
        | T_VariableSetStmt => {
            /*
             * These modify only backend-local state, so they're OK to run
             * in a read-only transaction or on a standby. However, they
             * are disallowed in parallel mode, because they either rely
             * upon or modify backend-local state that might not be
             * synchronized among cooperating backends.
             */
            COMMAND_OK_IN_RECOVERY | COMMAND_OK_IN_READ_ONLY_TXN
        }

        T_ClusterStmt | T_ReindexStmt | T_VacuumStmt => {
            /*
             * These commands write WAL, so they're not strictly
             * read-only, and running them in parallel workers isn't
             * supported.
             *
             * However, they don't change the database state in a way that
             * would affect pg_dump output, so it's fine to run them in a
             * read-only transaction. (CLUSTER might change the order of
             * rows on disk, which could affect the ordering of pg_dump
             * output, but that's not semantically significant.)
             */
            COMMAND_OK_IN_READ_ONLY_TXN
        }

        T_CopyStmt => {
            let stmt = parsetree as *mut CopyStmt;

            /*
             * You might think that COPY FROM is not at all read only, but
             * it's OK to copy into a temporary table, because that
             * wouldn't change the output of pg_dump.  If the target table
             * turns out to be non-temporary, DoCopy itself will call
             * PreventCommandIfReadOnly.
             */
            if (*stmt).is_from {
                COMMAND_OK_IN_READ_ONLY_TXN
            } else {
                COMMAND_IS_STRICTLY_READ_ONLY
            }
        }

        T_ExplainStmt | T_VariableShowStmt => {
            /*
             * These commands don't modify any data and are safe to run in
             * a parallel worker.
             */
            COMMAND_IS_STRICTLY_READ_ONLY
        }

        T_ListenStmt | T_NotifyStmt => {
            /*
             * NOTIFY requires an XID assignment, so it can't be permitted
             * on a standby. Perhaps LISTEN could, since without NOTIFY it
             * would be OK to just do nothing, at least until promotion,
             * but we currently prohibit it lest the user get the wrong
             * idea.
             *
             * (We do allow T_UnlistenStmt on a standby, though, because
             * it's a no-op.)
             */
            COMMAND_OK_IN_READ_ONLY_TXN
        }

        T_LockStmt => {
            let stmt = parsetree as *mut LockStmt;

            /*
             * Only weaker locker modes are allowed during recovery. The
             * restrictions here must match those in
             * LockAcquireExtended().
             */
            if (*stmt).mode > RowExclusiveLock {
                COMMAND_OK_IN_READ_ONLY_TXN
            } else {
                COMMAND_IS_STRICTLY_READ_ONLY
            }
        }

        T_TransactionStmt => {
            let stmt = parsetree as *mut TransactionStmt;

            /*
             * PREPARE, COMMIT PREPARED, and ROLLBACK PREPARED all write
             * WAL, so they're not read-only in the strict sense; but the
             * first and third do not change pg_dump output, so they're OK
             * in a read-only transactions.
             *
             * We also consider COMMIT PREPARED to be OK in a read-only
             * transaction environment, by way of exception.
             */
            match (*stmt).kind {
                TransactionStmtKind::TRANS_STMT_BEGIN
                | TransactionStmtKind::TRANS_STMT_START
                | TransactionStmtKind::TRANS_STMT_COMMIT
                | TransactionStmtKind::TRANS_STMT_ROLLBACK
                | TransactionStmtKind::TRANS_STMT_SAVEPOINT
                | TransactionStmtKind::TRANS_STMT_RELEASE
                | TransactionStmtKind::TRANS_STMT_ROLLBACK_TO => {
                    COMMAND_IS_STRICTLY_READ_ONLY
                }
                TransactionStmtKind::TRANS_STMT_PREPARE
                | TransactionStmtKind::TRANS_STMT_COMMIT_PREPARED
                | TransactionStmtKind::TRANS_STMT_ROLLBACK_PREPARED => {
                    COMMAND_OK_IN_READ_ONLY_TXN
                }
            }
        }

        _ => {
            elog!(ERROR, "unrecognized node type: {}", nodeTag(parsetree) as c_int);
            0 /* silence stupider compilers */
        }
    }
}

/*
 * PreventCommandIfReadOnly: throw error if XactReadOnly
 *
 * This is useful partly to ensure consistency of the error message wording;
 * some callers have checked XactReadOnly for themselves.
 */
pub unsafe fn PreventCommandIfReadOnly(cmdname: *const c_char) {
    if XactReadOnly {
        ereport!(ERROR, errmsg!("cannot execute {} in a read-only transaction",
                    std::ffi::CStr::from_ptr(cmdname).to_string_lossy()) /* C also: errcode!(ERRCODE_READ_ONLY_SQL_TRANSACTION) */);
    }
}

/*
 * PreventCommandIfParallelMode: throw error if current (sub)transaction is
 * in parallel mode.
 *
 * This is useful partly to ensure consistency of the error message wording;
 * some callers have checked IsInParallelMode() for themselves.
 */
pub unsafe fn PreventCommandIfParallelMode(cmdname: *const c_char) {
    if IsInParallelMode() {
        ereport!(ERROR, errmsg!("cannot execute {} during a parallel operation",
                    std::ffi::CStr::from_ptr(cmdname).to_string_lossy()) /* C also: errcode!(ERRCODE_INVALID_TRANSACTION_STATE) */);
    }
}

/*
 * PreventCommandDuringRecovery: throw error if RecoveryInProgress
 *
 * The majority of operations that are unsafe in a Hot Standby
 * will be rejected by XactReadOnly tests.  However there are a few
 * commands that are allowed in "read-only" xacts but cannot be allowed
 * in Hot Standby mode.  Those commands should call this function.
 */
pub unsafe fn PreventCommandDuringRecovery(cmdname: *const c_char) {
    if RecoveryInProgress() {
        ereport!(ERROR, errmsg!("cannot execute {} during recovery",
                    std::ffi::CStr::from_ptr(cmdname).to_string_lossy()) /* C also: errcode!(ERRCODE_READ_ONLY_SQL_TRANSACTION) */);
    }
}

/*
 * CheckRestrictedOperation: throw error for hazardous command if we're
 * inside a security restriction context.
 *
 * This is needed to protect session-local state for which there is not any
 * better-defined protection mechanism, such as ownership.
 */
unsafe fn CheckRestrictedOperation(cmdname: *const c_char) {
    if InSecurityRestrictedOperation() {
        ereport!(ERROR, errmsg!("cannot execute {} within security-restricted operation",
                    std::ffi::CStr::from_ptr(cmdname).to_string_lossy()) /* C also: errcode!(ERRCODE_INSUFFICIENT_PRIVILEGE) */);
    }
}

// ---------------------------------------------------------------------------
// ProcessUtility and standard_ProcessUtility
// ---------------------------------------------------------------------------

/*
 * ProcessUtility
 *        general utility function invoker
 *
 *    pstmt: PlannedStmt wrapper for the utility statement
 *    queryString: original source text of command
 *    readOnlyTree: if true, pstmt's node tree must not be modified
 *    context: identifies source of statement (toplevel client command,
 *        non-toplevel client command, subcommand of a larger utility command)
 *    params: parameters to use during execution
 *    queryEnv: environment for parse through execution (e.g., ephemeral named
 *        tables like trigger transition tables).  May be NULL.
 *    dest: where to send results
 *    qc: where to store command completion status data.  May be NULL,
 *        but if not, then caller must have initialized it.
 *
 * Caller MUST supply a queryString; it is not allowed (anymore) to pass NULL.
 * If you really don't have source text, you can pass a constant string,
 * perhaps "(query not available)".
 *
 * Note for users of ProcessUtility_hook: the same queryString may be passed
 * to multiple invocations of ProcessUtility when processing a query string
 * containing multiple semicolon-separated statements.  One should use
 * pstmt->stmt_location and pstmt->stmt_len to identify the substring
 * containing the current statement.  Keep in mind also that some utility
 * statements (e.g., CREATE SCHEMA) will recurse to ProcessUtility to process
 * sub-statements, often passing down the same queryString, stmt_location,
 * and stmt_len that were given for the whole statement.
 */
pub unsafe fn ProcessUtility(
    pstmt: *mut PlannedStmt,
    queryString: *const c_char,
    readOnlyTree: bool,
    context: ProcessUtilityContext,
    params: *mut ParamListInfo,
    queryEnv: *mut QueryEnvironment,
    dest: *mut DestReceiver,
    qc: *mut QueryCompletion,
) {
    Assert!(IsA!(pstmt, T_PlannedStmt));
    Assert!((*pstmt).commandType == CMD_UTILITY);
    Assert!(!queryString.is_null()); /* required as of 8.4 */
    Assert!(qc.is_null() || (*qc).commandTag == CMDTAG_UNKNOWN);

    /*
     * We provide a function hook variable that lets loadable plugins get
     * control when ProcessUtility is called.  Such a plugin would normally
     * call standard_ProcessUtility().
     */
    if let Some(hook) = ProcessUtility_hook {
        hook(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, qc);
    } else {
        standard_ProcessUtility(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, qc);
    }
}

/*
 * standard_ProcessUtility itself deals only with utility commands for
 * which we do not provide event trigger support.  Commands that do have
 * such support are passed down to ProcessUtilitySlow, which contains the
 * necessary infrastructure for such triggers.
 *
 * This division is not just for performance: it's critical that the
 * event trigger code not be invoked when doing START TRANSACTION for
 * example, because we might need to refresh the event trigger cache,
 * which requires being in a valid transaction.
 *
 * When adding or moving utility commands, check that the documentation in
 * event-trigger.sgml is kept up to date.
 */
pub unsafe fn standard_ProcessUtility(
    mut pstmt: *mut PlannedStmt,
    queryString: *const c_char,
    readOnlyTree: bool,
    context: ProcessUtilityContext,
    params: *mut ParamListInfo,
    queryEnv: *mut QueryEnvironment,
    dest: *mut DestReceiver,
    qc: *mut QueryCompletion,
) {
    let parsetree: *mut Node;
    let isTopLevel = context == PROCESS_UTILITY_TOPLEVEL;
    let isAtomicContext = (!(context == PROCESS_UTILITY_TOPLEVEL
        || context == PROCESS_UTILITY_QUERY_NONATOMIC))
        || IsTransactionBlock();
    let pstate: *mut ParseState;
    let readonly_flags: c_int;

    /* This can recurse, so check for excessive recursion */
    check_stack_depth();

    /*
     * If the given node tree is read-only, make a copy to ensure that parse
     * transformations don't damage the original tree.  This could be
     * refactored to avoid making unnecessary copies in more cases, but it's
     * not clear that it's worth a great deal of trouble over.  Statements
     * that are complex enough to be expensive to copy are exactly the ones
     * we'd need to copy, so that only marginal savings seem possible.
     */
    if readOnlyTree {
        pstmt = copyObject(pstmt);
    }
    parsetree = (*pstmt).utilityStmt;

    /* Prohibit read/write commands in read-only states. */
    readonly_flags = ClassifyUtilityCommandAsReadOnly(parsetree);
    if readonly_flags != COMMAND_IS_STRICTLY_READ_ONLY
        && (XactReadOnly || IsInParallelMode())
    {
        let commandtag = CreateCommandTag(parsetree);

        if (readonly_flags & COMMAND_OK_IN_READ_ONLY_TXN) == 0 {
            PreventCommandIfReadOnly(GetCommandTagName(commandtag));
        }
        if (readonly_flags & COMMAND_OK_IN_PARALLEL_MODE) == 0 {
            PreventCommandIfParallelMode(GetCommandTagName(commandtag));
        }
        if (readonly_flags & COMMAND_OK_IN_RECOVERY) == 0 {
            PreventCommandDuringRecovery(GetCommandTagName(commandtag));
        }
    }

    pstate = make_parsestate(null_mut());
    (*pstate).p_sourcetext = queryString;
    (*pstate).p_queryEnv = queryEnv as *mut c_void;

    match nodeTag(parsetree) {
        /*
         * ******************** transactions ********************
         */
        T_TransactionStmt => {
            let stmt = parsetree as *mut TransactionStmt;

            match (*stmt).kind {
                /*
                 * START TRANSACTION, as defined by SQL99: Identical
                 * to BEGIN.  Same code for both.
                 */
                TransactionStmtKind::TRANS_STMT_BEGIN
                | TransactionStmtKind::TRANS_STMT_START => {
                    let lc: *mut crate::nodes::pg_list::ListCell = null_mut();

                    BeginTransactionBlock();
                    foreach!(lc, (*stmt).options, {
                        let item = lfirst(crate::current_cell!(lc)) as *mut DefElem;

                        if strcmp((*item).defname, b"transaction_isolation\0".as_ptr() as *const c_char) == 0 {
                            SetPGVariable(
                                b"transaction_isolation\0".as_ptr() as *const c_char,
                                list_make1((*item).arg as *mut Node),
                                true,
                            );
                        } else if strcmp((*item).defname, b"transaction_read_only\0".as_ptr() as *const c_char) == 0 {
                            SetPGVariable(
                                b"transaction_read_only\0".as_ptr() as *const c_char,
                                list_make1((*item).arg as *mut Node),
                                true,
                            );
                        } else if strcmp((*item).defname, b"transaction_deferrable\0".as_ptr() as *const c_char) == 0 {
                            SetPGVariable(
                                b"transaction_deferrable\0".as_ptr() as *const c_char,
                                list_make1((*item).arg as *mut Node),
                                true,
                            );
                        }
                    });
                }

                TransactionStmtKind::TRANS_STMT_COMMIT => {
                    if !EndTransactionBlock((*stmt).chain) {
                        /* report unsuccessful commit in qc */
                        if !qc.is_null() {
                            SetQueryCompletion(&mut *qc, CMDTAG_ROLLBACK, 0);
                        }
                    }
                }

                TransactionStmtKind::TRANS_STMT_PREPARE => {
                    if !PrepareTransactionBlock((*stmt).gid) {
                        /* report unsuccessful commit in qc */
                        if !qc.is_null() {
                            SetQueryCompletion(&mut *qc, CMDTAG_ROLLBACK, 0);
                        }
                    }
                }

                TransactionStmtKind::TRANS_STMT_COMMIT_PREPARED => {
                    PreventInTransactionBlock(isTopLevel, b"COMMIT PREPARED\0".as_ptr() as *const c_char);
                    FinishPreparedTransaction((*stmt).gid, true);
                }

                TransactionStmtKind::TRANS_STMT_ROLLBACK_PREPARED => {
                    PreventInTransactionBlock(isTopLevel, b"ROLLBACK PREPARED\0".as_ptr() as *const c_char);
                    FinishPreparedTransaction((*stmt).gid, false);
                }

                TransactionStmtKind::TRANS_STMT_ROLLBACK => {
                    UserAbortTransactionBlock((*stmt).chain);
                }

                TransactionStmtKind::TRANS_STMT_SAVEPOINT => {
                    RequireTransactionBlock(isTopLevel, b"SAVEPOINT\0".as_ptr() as *const c_char);
                    DefineSavepoint((*stmt).savepoint_name);
                }

                TransactionStmtKind::TRANS_STMT_RELEASE => {
                    RequireTransactionBlock(isTopLevel, b"RELEASE SAVEPOINT\0".as_ptr() as *const c_char);
                    ReleaseSavepoint((*stmt).savepoint_name);
                }

                TransactionStmtKind::TRANS_STMT_ROLLBACK_TO => {
                    RequireTransactionBlock(isTopLevel, b"ROLLBACK TO SAVEPOINT\0".as_ptr() as *const c_char);
                    RollbackToSavepoint((*stmt).savepoint_name);

                    /*
                     * CommitTransactionCommand is in charge of
                     * re-defining the savepoint again
                     */
                }
            }
        }

        /*
         * Portal (cursor) manipulation
         */
        T_DeclareCursorStmt => {
            PerformCursorOpen(pstate, parsetree as *mut DeclareCursorStmt, params, isTopLevel);
        }

        T_ClosePortalStmt => {
            let stmt = parsetree as *mut ClosePortalStmt;

            CheckRestrictedOperation(b"CLOSE\0".as_ptr() as *const c_char);
            PerformPortalClose((*stmt).portalname);
        }

        T_FetchStmt => {
            PerformPortalFetch(parsetree as *mut FetchStmt, dest, qc);
        }

        T_DoStmt => {
            ExecuteDoStmt(pstate, parsetree as *mut DoStmt, isAtomicContext);
        }

        T_CreateTableSpaceStmt => {
            /* no event triggers for global objects */
            PreventInTransactionBlock(isTopLevel, b"CREATE TABLESPACE\0".as_ptr() as *const c_char);
            CreateTableSpace(parsetree as *mut CreateTableSpaceStmt);
        }

        T_DropTableSpaceStmt => {
            /* no event triggers for global objects */
            PreventInTransactionBlock(isTopLevel, b"DROP TABLESPACE\0".as_ptr() as *const c_char);
            DropTableSpace(parsetree as *mut DropTableSpaceStmt);
        }

        T_AlterTableSpaceOptionsStmt => {
            /* no event triggers for global objects */
            AlterTableSpaceOptions(parsetree as *mut AlterTableSpaceOptionsStmt);
        }

        T_TruncateStmt => {
            ExecuteTruncate(parsetree as *mut TruncateStmt);
        }

        T_CopyStmt => {
            let mut processed: u64 = 0;

            DoCopy(
                pstate,
                parsetree as *mut CopyStmt,
                (*pstmt).stmt_location,
                (*pstmt).stmt_len,
                &mut processed,
            );
            if !qc.is_null() {
                SetQueryCompletion(&mut *qc, CMDTAG_COPY, processed);
            }
        }

        T_PrepareStmt => {
            CheckRestrictedOperation(b"PREPARE\0".as_ptr() as *const c_char);
            PrepareQuery(
                pstate,
                parsetree as *mut PrepareStmt,
                (*pstmt).stmt_location,
                (*pstmt).stmt_len,
            );
        }

        T_ExecuteStmt => {
            ExecuteQuery(
                pstate,
                parsetree as *mut ExecuteStmt,
                null_mut(),
                params,
                dest,
                qc,
            );
        }

        T_DeallocateStmt => {
            CheckRestrictedOperation(b"DEALLOCATE\0".as_ptr() as *const c_char);
            DeallocateQuery(parsetree as *mut DeallocateStmt);
        }

        T_GrantRoleStmt => {
            /* no event triggers for global objects */
            GrantRole(pstate, parsetree as *mut GrantRoleStmt);
        }

        T_CreatedbStmt => {
            /* no event triggers for global objects */
            PreventInTransactionBlock(isTopLevel, b"CREATE DATABASE\0".as_ptr() as *const c_char);
            createdb(pstate, parsetree as *mut CreatedbStmt);
        }

        T_AlterDatabaseStmt => {
            /* no event triggers for global objects */
            AlterDatabase(pstate, parsetree as *mut AlterDatabaseStmt, isTopLevel);
        }

        T_AlterDatabaseRefreshCollStmt => {
            /* no event triggers for global objects */
            AlterDatabaseRefreshColl(parsetree as *mut AlterDatabaseRefreshCollStmt);
        }

        T_AlterDatabaseSetStmt => {
            /* no event triggers for global objects */
            AlterDatabaseSet(parsetree as *mut AlterDatabaseSetStmt);
        }

        T_DropdbStmt => {
            /* no event triggers for global objects */
            PreventInTransactionBlock(isTopLevel, b"DROP DATABASE\0".as_ptr() as *const c_char);
            DropDatabase(pstate, parsetree as *mut DropdbStmt);
        }

        /* Query-level asynchronous notification */
        T_NotifyStmt => {
            let stmt = parsetree as *mut NotifyStmt;

            Async_Notify((*stmt).conditionname, (*stmt).payload);
        }

        T_ListenStmt => {
            let stmt = parsetree as *mut ListenStmt;

            CheckRestrictedOperation(b"LISTEN\0".as_ptr() as *const c_char);

            /*
             * We don't allow LISTEN in background processes, as there is
             * no mechanism for them to collect NOTIFY messages, so they'd
             * just block cleanout of the async SLRU indefinitely.
             * (Authors of custom background workers could bypass this
             * restriction by calling Async_Listen directly, but then it's
             * on them to provide some mechanism to process the message
             * queue.)  Note there seems no reason to forbid UNLISTEN.
             */
            if MyBackendType != B_BACKEND {
                ereport!(ERROR, errmsg!("cannot execute {} within a background process", "LISTEN") /* C also: errcode!(ERRCODE_FEATURE_NOT_SUPPORTED) */);
            }

            Async_Listen((*stmt).conditionname);
        }

        T_UnlistenStmt => {
            let stmt = parsetree as *mut UnlistenStmt;

            CheckRestrictedOperation(b"UNLISTEN\0".as_ptr() as *const c_char);
            if !(*stmt).conditionname.is_null() {
                Async_Unlisten((*stmt).conditionname);
            } else {
                Async_UnlistenAll();
            }
        }

        T_LoadStmt => {
            let stmt = parsetree as *mut LoadStmt;

            closeAllVfds(); /* probably not necessary... */
            /* Allowed names are restricted if you're not superuser */
            load_file((*stmt).filename, !superuser());
        }

        T_CallStmt => {
            ExecuteCallStmt(parsetree as *mut CallStmt, params, isAtomicContext, dest);
        }

        T_ClusterStmt => {
            cluster(pstate, parsetree as *mut ClusterStmt, isTopLevel);
        }

        T_VacuumStmt => {
            ExecVacuum(pstate, parsetree as *mut VacuumStmt, isTopLevel);
        }

        T_ExplainStmt => {
            ExplainQuery(pstate, parsetree as *mut ExplainStmt, params, dest);
        }

        T_AlterSystemStmt => {
            PreventInTransactionBlock(isTopLevel, b"ALTER SYSTEM\0".as_ptr() as *const c_char);
            AlterSystemSetConfigFile(parsetree as *mut AlterSystemStmt);
        }

        T_VariableSetStmt => {
            ExecSetVariableStmt(parsetree as *mut VariableSetStmt, isTopLevel);
        }

        T_VariableShowStmt => {
            let n = parsetree as *mut VariableShowStmt;

            GetPGVariable((*n).name, dest);
        }

        T_DiscardStmt => {
            /* should we allow DISCARD PLANS? */
            CheckRestrictedOperation(b"DISCARD\0".as_ptr() as *const c_char);
            DiscardCommand(parsetree as *mut DiscardStmt, isTopLevel);
        }

        T_CreateEventTrigStmt => {
            /* no event triggers on event triggers */
            CreateEventTrigger(parsetree as *mut CreateEventTrigStmt);
        }

        T_AlterEventTrigStmt => {
            /* no event triggers on event triggers */
            AlterEventTrigger(parsetree as *mut AlterEventTrigStmt);
        }

        /*
         * ******************************** ROLE statements ****
         */
        T_CreateRoleStmt => {
            /* no event triggers for global objects */
            CreateRole(pstate, parsetree as *mut CreateRoleStmt);
        }

        T_AlterRoleStmt => {
            /* no event triggers for global objects */
            AlterRole(pstate, parsetree as *mut AlterRoleStmt);
        }

        T_AlterRoleSetStmt => {
            /* no event triggers for global objects */
            AlterRoleSet(parsetree as *mut AlterRoleSetStmt);
        }

        T_DropRoleStmt => {
            /* no event triggers for global objects */
            DropRole(parsetree as *mut DropRoleStmt);
        }

        T_ReassignOwnedStmt => {
            /* no event triggers for global objects */
            ReassignOwnedObjects(parsetree as *mut ReassignOwnedStmt);
        }

        T_LockStmt => {
            /*
             * Since the lock would just get dropped immediately, LOCK TABLE
             * outside a transaction block is presumed to be user error.
             */
            RequireTransactionBlock(isTopLevel, b"LOCK TABLE\0".as_ptr() as *const c_char);
            LockTableCommand(parsetree as *mut LockStmt);
        }

        T_ConstraintsSetStmt => {
            WarnNoTransactionBlock(isTopLevel, b"SET CONSTRAINTS\0".as_ptr() as *const c_char);
            AfterTriggerSetState(parsetree as *mut ConstraintsSetStmt);
        }

        T_CheckPointStmt => {
            if !has_privs_of_role(GetUserId(), ROLE_PG_CHECKPOINT) {
                ereport!(ERROR, errmsg!("permission denied to execute {} command", "CHECKPOINT") /* C also: errcode!(ERRCODE_INSUFFICIENT_PRIVILEGE); errdetail!("Only roles with privileges of the \"{}\" role may execute this command.", "pg_checkpoint") */);
            }

            RequestCheckpoint(
                CHECKPOINT_IMMEDIATE
                    | CHECKPOINT_WAIT
                    | (if RecoveryInProgress() { 0 } else { CHECKPOINT_FORCE }),
            );
        }

        /*
         * The following statements are supported by Event Triggers only
         * in some cases, so we "fast path" them in the other cases.
         */
        T_GrantStmt => {
            let stmt = parsetree as *mut GrantStmt;

            if EventTriggerSupportsObjectType((*stmt).objtype) {
                ProcessUtilitySlow(pstate, pstmt, queryString, context, params, queryEnv, dest, qc);
            } else {
                ExecGrantStmt(stmt);
            }
        }

        T_DropStmt => {
            let stmt = parsetree as *mut DropStmt;

            if EventTriggerSupportsObjectType((*stmt).removeType) {
                ProcessUtilitySlow(pstate, pstmt, queryString, context, params, queryEnv, dest, qc);
            } else {
                ExecDropStmt(stmt, isTopLevel);
            }
        }

        T_RenameStmt => {
            let stmt = parsetree as *mut RenameStmt;

            if EventTriggerSupportsObjectType((*stmt).renameType) {
                ProcessUtilitySlow(pstate, pstmt, queryString, context, params, queryEnv, dest, qc);
            } else {
                ExecRenameStmt(stmt);
            }
        }

        T_AlterObjectDependsStmt => {
            let stmt = parsetree as *mut AlterObjectDependsStmt;

            if EventTriggerSupportsObjectType((*stmt).objectType) {
                ProcessUtilitySlow(pstate, pstmt, queryString, context, params, queryEnv, dest, qc);
            } else {
                ExecAlterObjectDependsStmt(stmt, null_mut());
            }
        }

        T_AlterObjectSchemaStmt => {
            let stmt = parsetree as *mut AlterObjectSchemaStmt;

            if EventTriggerSupportsObjectType((*stmt).objectType) {
                ProcessUtilitySlow(pstate, pstmt, queryString, context, params, queryEnv, dest, qc);
            } else {
                ExecAlterObjectSchemaStmt(stmt, null_mut());
            }
        }

        T_AlterOwnerStmt => {
            let stmt = parsetree as *mut AlterOwnerStmt;

            if EventTriggerSupportsObjectType((*stmt).objectType) {
                ProcessUtilitySlow(pstate, pstmt, queryString, context, params, queryEnv, dest, qc);
            } else {
                ExecAlterOwnerStmt(stmt);
            }
        }

        T_CommentStmt => {
            let stmt = parsetree as *mut CommentStmt;

            if EventTriggerSupportsObjectType((*stmt).objtype) {
                ProcessUtilitySlow(pstate, pstmt, queryString, context, params, queryEnv, dest, qc);
            } else {
                CommentObject(stmt);
            }
        }

        T_SecLabelStmt => {
            let stmt = parsetree as *mut SecLabelStmt;

            if EventTriggerSupportsObjectType((*stmt).objtype) {
                ProcessUtilitySlow(pstate, pstmt, queryString, context, params, queryEnv, dest, qc);
            } else {
                ExecSecLabelStmt(stmt);
            }
        }

        _ => {
            /* All other statement types have event trigger support */
            ProcessUtilitySlow(pstate, pstmt, queryString, context, params, queryEnv, dest, qc);
        }
    }

    free_parsestate(pstate);

    /*
     * Make effects of commands visible, for instance so that
     * PreCommit_on_commit_actions() can see them (see for example bug
     * #15631).
     */
    CommandCounterIncrement();
}

// ---------------------------------------------------------------------------
// ProcessUtilitySlow - event trigger infrastructure
// ---------------------------------------------------------------------------

/*
 * The "Slow" variant of ProcessUtility should only receive statements
 * supported by the event triggers facility.  Therefore, we always
 * perform the trigger support calls if the context allows it.
 */
unsafe fn ProcessUtilitySlow(
    pstate: *mut ParseState,
    pstmt: *mut PlannedStmt,
    queryString: *const c_char,
    context: ProcessUtilityContext,
    params: *mut ParamListInfo,
    queryEnv: *mut QueryEnvironment,
    dest: *mut DestReceiver,
    qc: *mut QueryCompletion,
) {
    let parsetree = (*pstmt).utilityStmt;
    let isTopLevel = context == PROCESS_UTILITY_TOPLEVEL;
    let isCompleteQuery = context != PROCESS_UTILITY_SUBCOMMAND;
    let needCleanup: bool;
    let mut commandCollected: bool = false;
    let mut address: ObjectAddress;
    let mut secondaryObject: ObjectAddress = InvalidObjectAddress();

    /* All event trigger calls are done only when isCompleteQuery is true */
    needCleanup = isCompleteQuery && EventTriggerBeginCompleteQuery();

    /* PG_TRY block is to ensure we call EventTriggerEndCompleteQuery */
    // NOTE: Rust does not have PG_TRY; use a closure + catch_unwind pattern stub.
    // TODO(pg-port): wrap in PG_TRY equivalent when error handling is ported.
    // For now, implement the body inline and call EventTriggerEndCompleteQuery
    // in a defer-like fashion via a guard.
    struct CleanupGuard { needCleanup: bool }
    impl Drop for CleanupGuard {
        fn drop(&mut self) {
            if self.needCleanup {
                unsafe { EventTriggerEndCompleteQuery(); }
            }
        }
    }
    let _guard = CleanupGuard { needCleanup };

    if isCompleteQuery {
        EventTriggerDDLCommandStart(parsetree);
    }

    address = InvalidObjectAddress(); /* keep compiler happy */

    match nodeTag(parsetree) {
        /*
         * relation and attribute manipulation
         */
        T_CreateSchemaStmt => {
            CreateSchemaCommand(
                parsetree as *mut CreateSchemaStmt,
                queryString,
                (*pstmt).stmt_location,
                (*pstmt).stmt_len,
            );

            /*
             * EventTriggerCollectSimpleCommand called by
             * CreateSchemaCommand
             */
            commandCollected = true;
        }

        T_CreateStmt | T_CreateForeignTableStmt => {
            let mut stmts: *mut List;
            let mut table_rv: *mut RangeVar = null_mut();

            /* Run parse analysis ... */
            stmts = transformCreateStmt(parsetree as *mut CreateStmt, queryString);

            /*
             * ... and do it.  We can't use foreach() because we may
             * modify the list midway through, so pick off the
             * elements one at a time, the hard way.
             */
            while stmts != NIL {
                let stmt = linitial(stmts) as *mut Node;

                stmts = list_delete_first(stmts);

                if IsA!(stmt, T_CreateStmt) {
                    let cstmt = stmt as *mut CreateStmt;
                    let toast_options: Datum;
                    let validnsps = HEAP_RELOPT_NAMESPACES!();

                    /* Remember transformed RangeVar for LIKE */
                    table_rv = (*cstmt).relation;

                    /* Create the table itself */
                    address = DefineRelation(
                        cstmt,
                        RELKIND_RELATION,
                        InvalidOid,
                        null_mut(),
                        queryString,
                    );
                    EventTriggerCollectSimpleCommand(address, secondaryObject, stmt);

                    /*
                     * Let NewRelationCreateToastTable decide if this
                     * one needs a secondary relation too.
                     */
                    CommandCounterIncrement();

                    /*
                     * parse and validate reloptions for the toast
                     * table
                     */
                    toast_options = transformRelOptions(
                        0 as Datum,
                        (*cstmt).options,
                        b"toast\0".as_ptr() as *const c_char,
                        validnsps.as_ptr(),
                        true,
                        false,
                    );
                    heap_reloptions(RELKIND_TOASTVALUE, toast_options, true);

                    NewRelationCreateToastTable(address.objectId, toast_options);
                } else if IsA!(stmt, T_CreateForeignTableStmt) {
                    let cstmt = stmt as *mut CreateForeignTableStmt;

                    /* Remember transformed RangeVar for LIKE */
                    table_rv = (*cstmt).base.relation;

                    /* Create the table itself */
                    address = DefineRelation(
                        &mut (*cstmt).base as *mut CreateStmt,
                        RELKIND_FOREIGN_TABLE,
                        InvalidOid,
                        null_mut(),
                        queryString,
                    );
                    CreateForeignTable(cstmt, address.objectId);
                    EventTriggerCollectSimpleCommand(address, secondaryObject, stmt);
                } else if IsA!(stmt, T_TableLikeClause) {
                    /*
                     * Do delayed processing of LIKE options.  This
                     * will result in additional sub-statements for us
                     * to process.  Those should get done before any
                     * remaining actions, so prepend them to "stmts".
                     */
                    let like = stmt as *mut TableLikeClause;
                    let morestmts: *mut List;

                    Assert!(!table_rv.is_null());

                    morestmts = expandTableLikeClause(table_rv, like);
                    stmts = list_concat(morestmts, stmts);
                } else {
                    /*
                     * Recurse for anything else.  Note the recursive
                     * call will stash the objects so created into our
                     * event trigger context.
                     */
                    let wrapper: *mut PlannedStmt;

                    wrapper = makeNode!(PlannedStmt, T_PlannedStmt);
                    (*wrapper).commandType = CMD_UTILITY;
                    (*wrapper).canSetTag = false;
                    (*wrapper).utilityStmt = stmt;
                    (*wrapper).stmt_location = (*pstmt).stmt_location;
                    (*wrapper).stmt_len = (*pstmt).stmt_len;

                    ProcessUtility(
                        wrapper,
                        queryString,
                        false,
                        PROCESS_UTILITY_SUBCOMMAND,
                        params,
                        null_mut(),
                        None_Receiver(),
                        null_mut(),
                    );
                }

                /* Need CCI between commands */
                if stmts != NIL {
                    CommandCounterIncrement();
                }
            }

            /*
             * The multiple commands generated here are stashed
             * individually, so disable collection below.
             */
            commandCollected = true;
        }

        T_AlterTableStmt => {
            let atstmt = parsetree as *mut AlterTableStmt;
            let relid: Oid;
            let lockmode: LOCKMODE;
            let cell: *mut crate::nodes::pg_list::ListCell = null_mut();

            /*
             * Disallow ALTER TABLE .. DETACH CONCURRENTLY in a
             * transaction block or function.  (Perhaps it could be
             * allowed in a procedure, but don't hold your breath.)
             */
            foreach!(cell, (*atstmt).cmds, {
                let cmd = lfirst(crate::current_cell!(cell)) as *mut AlterTableCmd;

                /* Disallow DETACH CONCURRENTLY in a transaction block */
                if (*cmd).subtype == AlterTableType::AT_DetachPartition {
                    if (*((*cmd).def as *mut PartitionCmd)).concurrent {
                        PreventInTransactionBlock(
                            isTopLevel,
                            b"ALTER TABLE ... DETACH CONCURRENTLY\0".as_ptr() as *const c_char,
                        );
                    }
                }
            });

            /*
             * Figure out lock mode, and acquire lock.  This also does
             * basic permissions checks, so that we won't wait for a
             * lock on (for example) a relation on which we have no
             * permissions.
             */
            lockmode = AlterTableGetLockLevel((*atstmt).cmds);
            relid = AlterTableLookupRelation(atstmt, lockmode);

            if OidIsValid!(relid) {
                let mut atcontext = AlterTableUtilityContext {
                    pstmt,
                    queryString,
                    relid,
                    params,
                    queryEnv,
                };

                /* ... ensure we have an event trigger context ... */
                EventTriggerAlterTableStart(parsetree);
                EventTriggerAlterTableRelid(relid);

                /* ... and do it */
                AlterTable(atstmt, lockmode, &mut atcontext);

                /* done */
                EventTriggerAlterTableEnd();
            } else {
                ereport!(NOTICE, errmsg!("relation \"{}\" does not exist, skipping",
                            std::ffi::CStr::from_ptr((*(*atstmt).relation).relname).to_string_lossy()));
            }

            /* ALTER TABLE stashes commands internally */
            commandCollected = true;
        }

        T_AlterDomainStmt => {
            let stmt = parsetree as *mut AlterDomainStmt;

            /*
             * Some or all of these functions are recursive to cover
             * inherited things, so permission checks are done there.
             */
            match (*stmt).subtype as u8 {
                b'T' => {
                    /* ALTER DOMAIN DEFAULT */
                    /*
                     * Recursively alter column default for table and,
                     * if requested, for descendants
                     */
                    address = AlterDomainDefault((*stmt).typeName, (*stmt).def);
                }
                b'N' => {
                    /* ALTER DOMAIN DROP NOT NULL */
                    address = AlterDomainNotNull((*stmt).typeName, false);
                }
                b'O' => {
                    /* ALTER DOMAIN SET NOT NULL */
                    address = AlterDomainNotNull((*stmt).typeName, true);
                }
                b'C' => {
                    /* ADD CONSTRAINT */
                    address = AlterDomainAddConstraint(
                        (*stmt).typeName,
                        (*stmt).def,
                        &mut secondaryObject,
                    );
                }
                b'X' => {
                    /* DROP CONSTRAINT */
                    address = AlterDomainDropConstraint(
                        (*stmt).typeName,
                        (*stmt).name,
                        (*stmt).behavior,
                        (*stmt).missing_ok,
                    );
                }
                b'V' => {
                    /* VALIDATE CONSTRAINT */
                    address = AlterDomainValidateConstraint((*stmt).typeName, (*stmt).name);
                }
                _ => {
                    /* oops */
                    elog!(ERROR, "unrecognized alter domain type: {}", (*stmt).subtype as c_int);
                }
            }
        }

        /*
         * ************* object creation / destruction **************
         */
        T_DefineStmt => {
            let stmt = parsetree as *mut DefineStmt;

            match (*stmt).kind {
                OBJECT_AGGREGATE => {
                    address = DefineAggregate(
                        pstate,
                        (*stmt).defnames,
                        (*stmt).args,
                        (*stmt).oldstyle,
                        (*stmt).definition,
                        (*stmt).replace,
                    );
                }
                OBJECT_OPERATOR => {
                    Assert!((*stmt).args == NIL);
                    address = DefineOperator((*stmt).defnames, (*stmt).definition);
                }
                OBJECT_TYPE => {
                    Assert!((*stmt).args == NIL);
                    address = DefineType(pstate, (*stmt).defnames, (*stmt).definition);
                }
                OBJECT_TSPARSER => {
                    Assert!((*stmt).args == NIL);
                    address = DefineTSParser((*stmt).defnames, (*stmt).definition);
                }
                OBJECT_TSDICTIONARY => {
                    Assert!((*stmt).args == NIL);
                    address = DefineTSDictionary((*stmt).defnames, (*stmt).definition);
                }
                OBJECT_TSTEMPLATE => {
                    Assert!((*stmt).args == NIL);
                    address = DefineTSTemplate((*stmt).defnames, (*stmt).definition);
                }
                OBJECT_TSCONFIGURATION => {
                    Assert!((*stmt).args == NIL);
                    address = DefineTSConfiguration(
                        (*stmt).defnames,
                        (*stmt).definition,
                        &mut secondaryObject,
                    );
                }
                OBJECT_COLLATION => {
                    Assert!((*stmt).args == NIL);
                    address = DefineCollation(
                        pstate,
                        (*stmt).defnames,
                        (*stmt).definition,
                        (*stmt).if_not_exists,
                    );
                }
                _ => {
                    elog!(ERROR, "unrecognized define stmt type: {}", (*stmt).kind as c_int);
                }
            }
        }

        T_IndexStmt => {
            /* CREATE INDEX */
            let mut stmt = parsetree as *mut IndexStmt;
            let relid: Oid;
            let lockmode: LOCKMODE;
            let mut nparts: c_int = -1;
            let is_alter_table: bool;

            if (*stmt).concurrent {
                PreventInTransactionBlock(
                    isTopLevel,
                    b"CREATE INDEX CONCURRENTLY\0".as_ptr() as *const c_char,
                );
            }

            /*
             * Look up the relation OID just once, right here at the
             * beginning, so that we don't end up repeating the name
             * lookup later and latching onto a different relation
             * partway through.  To avoid lock upgrade hazards, it's
             * important that we take the strongest lock that will
             * eventually be needed here, so the lockmode calculation
             * needs to match what DefineIndex() does.
             */
            lockmode = if (*stmt).concurrent {
                ShareUpdateExclusiveLock
            } else {
                ShareLock
            };
            relid = RangeVarGetRelidExtended(
                (*stmt).relation,
                lockmode,
                0,
                Some(RangeVarCallbackOwnsRelation),
                null_mut(),
            );

            /*
             * CREATE INDEX on partitioned tables (but not regular
             * inherited tables) recurses to partitions, so we must
             * acquire locks early to avoid deadlocks.
             *
             * We also take the opportunity to verify that all
             * partitions are something we can put an index on, to
             * avoid building some indexes only to fail later.  While
             * at it, also count the partitions, so that DefineIndex
             * needn't do a duplicative find_all_inheritors search.
             */
            if (*(*stmt).relation).inh
                && get_rel_relkind(relid) == RELKIND_PARTITIONED_TABLE
            {
                let lc: *mut crate::nodes::pg_list::ListCell = null_mut();
                let mut inheritors: *mut List = NIL;

                inheritors = find_all_inheritors(relid, lockmode, null_mut());
                foreach!(lc, inheritors, {
                    let partrelid = lfirst_oid(crate::current_cell!(lc));
                    let relkind = get_rel_relkind(partrelid);

                    if relkind != RELKIND_RELATION
                        && relkind != RELKIND_MATVIEW
                        && relkind != RELKIND_PARTITIONED_TABLE
                        && relkind != RELKIND_FOREIGN_TABLE
                    {
                        elog!(
                            ERROR,
                            "unexpected relkind \"{}\" on partition \"{}\"",
                            relkind as u8 as char,
                            std::ffi::CStr::from_ptr((*(*stmt).relation).relname).to_string_lossy()
                        );
                    }

                    if relkind == RELKIND_FOREIGN_TABLE && ((*stmt).unique || (*stmt).primary) {
                        ereport!(ERROR, errmsg!("cannot create unique index on partitioned table \"{}\"",
                                    std::ffi::CStr::from_ptr((*(*stmt).relation).relname).to_string_lossy()) /* C also: errcode!(ERRCODE_WRONG_OBJECT_TYPE); errdetail!("Table \"{}\" contains partitions that are foreign tables.", std::ffi::CStr::from_ptr((*(*stmt).relation).relname).to_string_lossy()) */);
                    }
                });
                /* count direct and indirect children, but not rel */
                nparts = list_length(inheritors) - 1;
                list_free(inheritors);
            }

            /*
             * If the IndexStmt is already transformed, it must have
             * come from generateClonedIndexStmt, which in current
             * usage means it came from expandTableLikeClause rather
             * than from original parse analysis.  And that means we
             * must treat it like ALTER TABLE ADD INDEX, not CREATE.
             * (This is a bit grotty, but currently it doesn't seem
             * worth adding a separate bool field for the purpose.)
             */
            is_alter_table = (*stmt).transformed;

            /* Run parse analysis ... */
            stmt = transformIndexStmt(relid, stmt, queryString);

            /* ... and do it */
            EventTriggerAlterTableStart(parsetree);
            address = DefineIndex(
                relid,    /* OID of heap relation */
                stmt,
                InvalidOid, /* no predefined OID */
                InvalidOid, /* no parent index */
                InvalidOid, /* no parent constraint */
                nparts, /* # of partitions, or -1 */
                is_alter_table,
                true,   /* check_rights */
                true,   /* check_not_in_use */
                false,  /* skip_build */
                false,  /* quiet */
            );

            /*
             * Add the CREATE INDEX node itself to stash right away;
             * if there were any commands stashed in the ALTER TABLE
             * code, we need them to appear after this one.
             */
            EventTriggerCollectSimpleCommand(address, secondaryObject, parsetree);
            commandCollected = true;
            EventTriggerAlterTableEnd();
        }

        T_ReindexStmt => {
            ExecReindex(pstate, parsetree as *mut ReindexStmt, isTopLevel);

            /* EventTriggerCollectSimpleCommand is called directly */
            commandCollected = true;
        }

        T_CreateExtensionStmt => {
            address = CreateExtension(pstate, parsetree as *mut CreateExtensionStmt);
        }

        T_AlterExtensionStmt => {
            address = ExecAlterExtensionStmt(pstate, parsetree as *mut AlterExtensionStmt);
        }

        T_AlterExtensionContentsStmt => {
            address = ExecAlterExtensionContentsStmt(
                parsetree as *mut AlterExtensionContentsStmt,
                &mut secondaryObject,
            );
        }

        T_CreateFdwStmt => {
            address = CreateForeignDataWrapper(pstate, parsetree as *mut CreateFdwStmt);
        }

        T_AlterFdwStmt => {
            address = AlterForeignDataWrapper(pstate, parsetree as *mut AlterFdwStmt);
        }

        T_CreateForeignServerStmt => {
            address = CreateForeignServer(parsetree as *mut CreateForeignServerStmt);
        }

        T_AlterForeignServerStmt => {
            address = AlterForeignServer(parsetree as *mut AlterForeignServerStmt);
        }

        T_CreateUserMappingStmt => {
            address = CreateUserMapping(parsetree as *mut CreateUserMappingStmt);
        }

        T_AlterUserMappingStmt => {
            address = AlterUserMapping(parsetree as *mut AlterUserMappingStmt);
        }

        T_DropUserMappingStmt => {
            RemoveUserMapping(parsetree as *mut DropUserMappingStmt);
            /* no commands stashed for DROP */
            commandCollected = true;
        }

        T_ImportForeignSchemaStmt => {
            ImportForeignSchema(parsetree as *mut ImportForeignSchemaStmt);
            /* commands are stashed inside ImportForeignSchema */
            commandCollected = true;
        }

        T_CompositeTypeStmt => {
            /* CREATE TYPE (composite) */
            let stmt = parsetree as *mut CompositeTypeStmt;

            address = DefineCompositeType((*stmt).typevar, (*stmt).coldeflist);
        }

        T_CreateEnumStmt => {
            /* CREATE TYPE AS ENUM */
            address = DefineEnum(parsetree as *mut CreateEnumStmt);
        }

        T_CreateRangeStmt => {
            /* CREATE TYPE AS RANGE */
            address = DefineRange(pstate, parsetree as *mut CreateRangeStmt);
        }

        T_AlterEnumStmt => {
            /* ALTER TYPE (enum) */
            address = AlterEnum(parsetree as *mut AlterEnumStmt);
        }

        T_ViewStmt => {
            /* CREATE VIEW */
            EventTriggerAlterTableStart(parsetree);
            address = DefineView(
                parsetree as *mut ViewStmt,
                queryString,
                (*pstmt).stmt_location,
                (*pstmt).stmt_len,
            );
            EventTriggerCollectSimpleCommand(address, secondaryObject, parsetree);
            /* stashed internally */
            commandCollected = true;
            EventTriggerAlterTableEnd();
        }

        T_CreateFunctionStmt => {
            /* CREATE FUNCTION */
            address = CreateFunction(pstate, parsetree as *mut CreateFunctionStmt);
        }

        T_AlterFunctionStmt => {
            /* ALTER FUNCTION */
            address = AlterFunction(pstate, parsetree as *mut AlterFunctionStmt);
        }

        T_RuleStmt => {
            /* CREATE RULE */
            address = DefineRule(parsetree as *mut RuleStmt, queryString);
        }

        T_CreateSeqStmt => {
            address = DefineSequence(pstate, parsetree as *mut CreateSeqStmt);
        }

        T_AlterSeqStmt => {
            address = AlterSequence(pstate, parsetree as *mut AlterSeqStmt);
        }

        T_CreateTableAsStmt => {
            address = ExecCreateTableAs(
                pstate,
                parsetree as *mut CreateTableAsStmt,
                params,
                queryEnv,
                qc,
            );
        }

        T_RefreshMatViewStmt => {
            /*
             * REFRESH CONCURRENTLY executes some DDL commands internally.
             * Inhibit DDL command collection here to avoid those commands
             * from showing up in the deparsed command queue.  The refresh
             * command itself is queued, which is enough.
             */
            EventTriggerInhibitCommandCollection();
            // PG_TRY(2) - use a second guard
            struct InhibitGuard;
            impl Drop for InhibitGuard {
                fn drop(&mut self) { unsafe { EventTriggerUndoInhibitCommandCollection(); } }
            }
            let _inh_guard = InhibitGuard;
            address = ExecRefreshMatView(
                parsetree as *mut RefreshMatViewStmt,
                queryString,
                qc,
            );
        }

        T_CreateTrigStmt => {
            address = CreateTrigger(
                parsetree as *mut CreateTrigStmt,
                queryString,
                InvalidOid,
                InvalidOid,
                InvalidOid,
                InvalidOid,
                InvalidOid,
                InvalidOid,
                null_mut(),
                false,
                false,
            );
        }

        T_CreatePLangStmt => {
            address = CreateProceduralLanguage(parsetree as *mut CreatePLangStmt);
        }

        T_CreateDomainStmt => {
            address = DefineDomain(pstate, parsetree as *mut CreateDomainStmt);
        }

        T_CreateConversionStmt => {
            address = CreateConversionCommand(parsetree as *mut CreateConversionStmt);
        }

        T_CreateCastStmt => {
            address = CreateCast(parsetree as *mut CreateCastStmt);
        }

        T_CreateOpClassStmt => {
            DefineOpClass(parsetree as *mut CreateOpClassStmt);
            /* command is stashed in DefineOpClass */
            commandCollected = true;
        }

        T_CreateOpFamilyStmt => {
            address = DefineOpFamily(parsetree as *mut CreateOpFamilyStmt);

            /*
             * DefineOpFamily calls EventTriggerCollectSimpleCommand
             * directly.
             */
            commandCollected = true;
        }

        T_CreateTransformStmt => {
            address = CreateTransform(parsetree as *mut CreateTransformStmt);
        }

        T_AlterOpFamilyStmt => {
            AlterOpFamily(parsetree as *mut AlterOpFamilyStmt);
            /* commands are stashed in AlterOpFamily */
            commandCollected = true;
        }

        T_AlterTSDictionaryStmt => {
            address = AlterTSDictionary(parsetree as *mut AlterTSDictionaryStmt);
        }

        T_AlterTSConfigurationStmt => {
            AlterTSConfiguration(parsetree as *mut AlterTSConfigurationStmt);

            /*
             * Commands are stashed in MakeConfigurationMapping and
             * DropConfigurationMapping, which are called from
             * AlterTSConfiguration
             */
            commandCollected = true;
        }

        T_AlterTableMoveAllStmt => {
            AlterTableMoveAll(parsetree as *mut AlterTableMoveAllStmt);
            /* commands are stashed in AlterTableMoveAll */
            commandCollected = true;
        }

        T_DropStmt => {
            ExecDropStmt(parsetree as *mut DropStmt, isTopLevel);
            /* no commands stashed for DROP */
            commandCollected = true;
        }

        T_RenameStmt => {
            address = ExecRenameStmt(parsetree as *mut RenameStmt);
        }

        T_AlterObjectDependsStmt => {
            address = ExecAlterObjectDependsStmt(
                parsetree as *mut AlterObjectDependsStmt,
                &mut secondaryObject,
            );
        }

        T_AlterObjectSchemaStmt => {
            address = ExecAlterObjectSchemaStmt(
                parsetree as *mut AlterObjectSchemaStmt,
                &mut secondaryObject,
            );
        }

        T_AlterOwnerStmt => {
            address = ExecAlterOwnerStmt(parsetree as *mut AlterOwnerStmt);
        }

        T_AlterOperatorStmt => {
            address = AlterOperator(parsetree as *mut AlterOperatorStmt);
        }

        T_AlterTypeStmt => {
            address = AlterType(parsetree as *mut AlterTypeStmt);
        }

        T_CommentStmt => {
            address = CommentObject(parsetree as *mut CommentStmt);
        }

        T_GrantStmt => {
            ExecGrantStmt(parsetree as *mut GrantStmt);
            /* commands are stashed in ExecGrantStmt_oids */
            commandCollected = true;
        }

        T_DropOwnedStmt => {
            DropOwnedObjects(parsetree as *mut DropOwnedStmt);
            /* no commands stashed for DROP */
            commandCollected = true;
        }

        T_AlterDefaultPrivilegesStmt => {
            ExecAlterDefaultPrivilegesStmt(pstate, parsetree as *mut AlterDefaultPrivilegesStmt);
            EventTriggerCollectAlterDefPrivs(parsetree as *mut AlterDefaultPrivilegesStmt);
            commandCollected = true;
        }

        T_CreatePolicyStmt => {
            /* CREATE POLICY */
            address = CreatePolicy(parsetree as *mut CreatePolicyStmt);
        }

        T_AlterPolicyStmt => {
            /* ALTER POLICY */
            address = AlterPolicy(parsetree as *mut AlterPolicyStmt);
        }

        T_SecLabelStmt => {
            address = ExecSecLabelStmt(parsetree as *mut SecLabelStmt);
        }

        T_CreateAmStmt => {
            address = CreateAccessMethod(parsetree as *mut CreateAmStmt);
        }

        T_CreatePublicationStmt => {
            address = CreatePublication(pstate, parsetree as *mut CreatePublicationStmt);
        }

        T_AlterPublicationStmt => {
            AlterPublication(pstate, parsetree as *mut AlterPublicationStmt);

            /*
             * AlterPublication calls EventTriggerCollectSimpleCommand
             * directly
             */
            commandCollected = true;
        }

        T_CreateSubscriptionStmt => {
            address = CreateSubscription(
                pstate,
                parsetree as *mut CreateSubscriptionStmt,
                isTopLevel,
            );
        }

        T_AlterSubscriptionStmt => {
            address = AlterSubscription(
                pstate,
                parsetree as *mut AlterSubscriptionStmt,
                isTopLevel,
            );
        }

        T_DropSubscriptionStmt => {
            DropSubscription(parsetree as *mut DropSubscriptionStmt, isTopLevel);
            /* no commands stashed for DROP */
            commandCollected = true;
        }

        T_CreateStatsStmt => {
            let relid: Oid;
            let mut stmt = parsetree as *mut CreateStatsStmt;
            let rel = linitial((*stmt).relations) as *mut RangeVar;

            if !IsA!(rel as *mut Node, T_RangeVar) {
                ereport!(ERROR, errmsg!("CREATE STATISTICS only supports relation names in the FROM clause") /* C also: errcode!(ERRCODE_FEATURE_NOT_SUPPORTED) */);
            }

            /*
             * CREATE STATISTICS will influence future execution plans
             * but does not interfere with currently executing plans.
             * So it should be enough to take ShareUpdateExclusiveLock
             * on relation, conflicting with ANALYZE and other DDL
             * that sets statistical information, but not with normal
             * queries.
             *
             * XXX RangeVarCallbackOwnsRelation not needed here, to
             * keep the same behavior as before.
             */
            relid = RangeVarGetRelid(rel, ShareUpdateExclusiveLock, false);

            /* Run parse analysis ... */
            stmt = transformStatsStmt(relid, stmt, queryString);

            address = CreateStatistics(stmt, true);
        }

        T_AlterStatsStmt => {
            address = AlterStatistics(parsetree as *mut AlterStatsStmt);
        }

        T_AlterCollationStmt => {
            address = AlterCollation(parsetree as *mut AlterCollationStmt);
        }

        _ => {
            elog!(ERROR, "unrecognized node type: {}", nodeTag(parsetree) as c_int);
        }
    }

    /*
     * Remember the object so that ddl_command_end event triggers have
     * access to it.
     */
    if !commandCollected {
        EventTriggerCollectSimpleCommand(address, secondaryObject, parsetree);
    }

    if isCompleteQuery {
        EventTriggerSQLDrop(parsetree);
        EventTriggerDDLCommandEnd(parsetree);
    }
    // _guard drops here, calling EventTriggerEndCompleteQuery if needed.
}

// Additional imports needed by functions below
use crate::nodes::lockoptions::{LCS_FORKEYSHARE, LCS_FORSHARE, LCS_FORNOKEYUPDATE, LCS_FORUPDATE};
use crate::nodes::plannodes::PlanRowMark;
use crate::nodes::parsenodes::RowMarkClause;

/*
 * ProcessUtilityForAlterTable
 *		Recursive entry from ALTER TABLE
 *
 * ALTER TABLE sometimes generates subcommands such as CREATE INDEX.
 * It calls this, not the main entry point ProcessUtility, to execute
 * such subcommands.
 *
 * stmt: the utility command to execute
 * context: opaque passthrough struct with the info we need
 *
 * It's caller's responsibility to do CommandCounterIncrement after
 * calling this, if needed.
 */
pub unsafe fn ProcessUtilityForAlterTable(stmt: *mut Node, context: *mut AlterTableUtilityContext) {
    let wrapper: *mut PlannedStmt;

    /*
     * For event triggers, we must "close" the current complex-command set,
     * and start a new one afterwards; this is needed to ensure the ordering
     * of command events is consistent with the way they were executed.
     */
    EventTriggerAlterTableEnd();

    /* Create a suitable wrapper */
    wrapper = makeNode!(PlannedStmt, T_PlannedStmt);
    (*wrapper).commandType = CMD_UTILITY;
    (*wrapper).canSetTag = false;
    (*wrapper).utilityStmt = stmt;
    (*wrapper).stmt_location = (*(*context).pstmt).stmt_location;
    (*wrapper).stmt_len = (*(*context).pstmt).stmt_len;

    ProcessUtility(
        wrapper,
        (*context).queryString,
        false,
        PROCESS_UTILITY_SUBCOMMAND,
        (*context).params,
        (*context).queryEnv,
        None_Receiver(),
        std::ptr::null_mut(),
    );

    EventTriggerAlterTableStart((*(*context).pstmt).utilityStmt);
    EventTriggerAlterTableRelid((*context).relid);
}

/*
 * Dispatch function for DropStmt
 */
unsafe fn ExecDropStmt(stmt: *mut DropStmt, isTopLevel: bool) {
    match (*stmt).removeType {
        OBJECT_INDEX => {
            if (*stmt).concurrent {
                PreventInTransactionBlock(
                    isTopLevel,
                    b"DROP INDEX CONCURRENTLY\0".as_ptr() as *const c_char,
                );
            }
            /* fall through */
            RemoveRelations(stmt);
        }
        OBJECT_TABLE | OBJECT_SEQUENCE | OBJECT_VIEW | OBJECT_MATVIEW | OBJECT_FOREIGN_TABLE => {
            RemoveRelations(stmt);
        }
        _ => {
            RemoveObjects(stmt);
        }
    }
}

/*
 * UtilityReturnsTuples
 *		Return "true" if this utility statement will send output to the
 *		destination.
 *
 * Generally, there should be a case here for each case in ProcessUtility
 * where "dest" is passed on.
 */
pub unsafe fn UtilityReturnsTuples(parsetree: *mut Node) -> bool {
    match nodeTag(parsetree) {
        T_CallStmt => {
            let stmt = parsetree as *mut CallStmt;
            return (*(*stmt).funcexpr).funcresulttype == RECORDOID;
        }
        T_FetchStmt => {
            let stmt = parsetree as *mut FetchStmt;
            if (*stmt).ismove {
                return false;
            }
            let portal = GetPortalByName((*stmt).portalname);
            if !PortalIsValid(portal) {
                return false; /* not our business to raise error */
            }
            return !(*portal).tupDesc.is_null();
        }
        T_ExecuteStmt => {
            let stmt = parsetree as *mut ExecuteStmt;
            let entry = FetchPreparedStatement((*stmt).name, false);
            if entry.is_null() {
                return false; /* not our business to raise error */
            }
            if !(*(*entry).plansource).resultDesc.is_null() {
                return true;
            }
            return false;
        }
        T_ExplainStmt => {
            return true;
        }
        T_VariableShowStmt => {
            return true;
        }
        _ => {
            return false;
        }
    }
}

/*
 * UtilityTupleDescriptor
 *		Fetch the actual output tuple descriptor for a utility statement
 *		for which UtilityReturnsTuples() previously returned "true".
 *
 * The returned descriptor is created in (or copied into) the current memory
 * context.
 */
pub unsafe fn UtilityTupleDescriptor(parsetree: *mut Node) -> TupleDesc {
    match nodeTag(parsetree) {
        T_CallStmt => {
            return CallStmtResultDesc(parsetree as *mut CallStmt);
        }
        T_FetchStmt => {
            let stmt = parsetree as *mut FetchStmt;
            if (*stmt).ismove {
                return std::ptr::null_mut();
            }
            let portal = GetPortalByName((*stmt).portalname);
            if !PortalIsValid(portal) {
                return std::ptr::null_mut(); /* not our business to raise error */
            }
            return CreateTupleDescCopy((*portal).tupDesc);
        }
        T_ExecuteStmt => {
            let stmt = parsetree as *mut ExecuteStmt;
            let entry = FetchPreparedStatement((*stmt).name, false);
            if entry.is_null() {
                return std::ptr::null_mut(); /* not our business to raise error */
            }
            return FetchPreparedStatementResultDesc(entry);
        }
        T_ExplainStmt => {
            return ExplainResultDesc(parsetree as *mut ExplainStmt);
        }
        T_VariableShowStmt => {
            let n = parsetree as *mut VariableShowStmt;
            return GetPGVariableResultDesc((*n).name);
        }
        _ => {
            return std::ptr::null_mut();
        }
    }
}

/*
 * QueryReturnsTuples
 *		Return "true" if this Query will send output to the destination.
 */
/* #[cfg(not_used)] -- matches #ifdef NOT_USED in C */
#[allow(dead_code)]
unsafe fn QueryReturnsTuples(parsetree: *mut Query) -> bool {
    match (*parsetree).commandType {
        CMD_SELECT => {
            /* returns tuples */
            return true;
        }
        CMD_INSERT | CMD_UPDATE | CMD_DELETE | CMD_MERGE => {
            /* the forms with RETURNING return tuples */
            if !(*parsetree).returningList.is_null() {
                return true;
            }
        }
        CMD_UTILITY => {
            return UtilityReturnsTuples((*parsetree).utilityStmt);
        }
        CMD_UNKNOWN | CMD_NOTHING => {
            /* probably shouldn't get here */
        }
        _ => {}
    }
    false /* default */
}

/*
 * UtilityContainsQuery
 *		Return the contained Query, or NULL if there is none
 *
 * Certain utility statements, such as EXPLAIN, contain a plannable Query.
 * This function encapsulates knowledge of exactly which ones do.
 * We assume it is invoked only on already-parse-analyzed statements
 * (else the contained parsetree isn't a Query yet).
 *
 * In some cases (currently, only EXPLAIN of CREATE TABLE AS/SELECT INTO and
 * CREATE MATERIALIZED VIEW), potentially Query-containing utility statements
 * can be nested.  This function will drill down to a non-utility Query, or
 * return NULL if none.
 */
pub unsafe fn UtilityContainsQuery(parsetree: *mut Node) -> *mut Query {
    let qry: *mut Query;
    match nodeTag(parsetree) {
        T_DeclareCursorStmt => {
            qry = castNode!(Query, T_Query, (*(parsetree as *mut DeclareCursorStmt)).query);
            if (*qry).commandType == CMD_UTILITY {
                return UtilityContainsQuery((*qry).utilityStmt);
            }
            return qry;
        }
        T_ExplainStmt => {
            qry = castNode!(Query, T_Query, (*(parsetree as *mut ExplainStmt)).query);
            if (*qry).commandType == CMD_UTILITY {
                return UtilityContainsQuery((*qry).utilityStmt);
            }
            return qry;
        }
        T_CreateTableAsStmt => {
            qry = castNode!(Query, T_Query, (*(parsetree as *mut CreateTableAsStmt)).query);
            if (*qry).commandType == CMD_UTILITY {
                return UtilityContainsQuery((*qry).utilityStmt);
            }
            return qry;
        }
        _ => {
            return std::ptr::null_mut();
        }
    }
}

/*
 * AlterObjectTypeCommandTag
 *		helper function for CreateCommandTag
 *
 * This covers most cases where ALTER is used with an ObjectType enum.
 */
unsafe fn AlterObjectTypeCommandTag(objtype: ObjectType) -> CommandTag {
    let tag: CommandTag;
    match objtype {
        OBJECT_AGGREGATE => { tag = CMDTAG_ALTER_AGGREGATE; }
        OBJECT_ATTRIBUTE => { tag = CMDTAG_ALTER_TYPE; }
        OBJECT_CAST => { tag = CMDTAG_ALTER_CAST; }
        OBJECT_COLLATION => { tag = CMDTAG_ALTER_COLLATION; }
        OBJECT_COLUMN => { tag = CMDTAG_ALTER_TABLE; }
        OBJECT_CONVERSION => { tag = CMDTAG_ALTER_CONVERSION; }
        OBJECT_DATABASE => { tag = CMDTAG_ALTER_DATABASE; }
        OBJECT_DOMAIN | OBJECT_DOMCONSTRAINT => { tag = CMDTAG_ALTER_DOMAIN; }
        OBJECT_EXTENSION => { tag = CMDTAG_ALTER_EXTENSION; }
        OBJECT_FDW => { tag = CMDTAG_ALTER_FOREIGN_DATA_WRAPPER; }
        OBJECT_FOREIGN_SERVER => { tag = CMDTAG_ALTER_SERVER; }
        OBJECT_FOREIGN_TABLE => { tag = CMDTAG_ALTER_FOREIGN_TABLE; }
        OBJECT_FUNCTION => { tag = CMDTAG_ALTER_FUNCTION; }
        OBJECT_INDEX => { tag = CMDTAG_ALTER_INDEX; }
        OBJECT_LANGUAGE => { tag = CMDTAG_ALTER_LANGUAGE; }
        OBJECT_LARGEOBJECT => { tag = CMDTAG_ALTER_LARGE_OBJECT; }
        OBJECT_OPCLASS => { tag = CMDTAG_ALTER_OPERATOR_CLASS; }
        OBJECT_OPERATOR => { tag = CMDTAG_ALTER_OPERATOR; }
        OBJECT_OPFAMILY => { tag = CMDTAG_ALTER_OPERATOR_FAMILY; }
        OBJECT_POLICY => { tag = CMDTAG_ALTER_POLICY; }
        OBJECT_PROCEDURE => { tag = CMDTAG_ALTER_PROCEDURE; }
        OBJECT_ROLE => { tag = CMDTAG_ALTER_ROLE; }
        OBJECT_ROUTINE => { tag = CMDTAG_ALTER_ROUTINE; }
        OBJECT_RULE => { tag = CMDTAG_ALTER_RULE; }
        OBJECT_SCHEMA => { tag = CMDTAG_ALTER_SCHEMA; }
        OBJECT_SEQUENCE => { tag = CMDTAG_ALTER_SEQUENCE; }
        OBJECT_TABLE | OBJECT_TABCONSTRAINT => { tag = CMDTAG_ALTER_TABLE; }
        OBJECT_TABLESPACE => { tag = CMDTAG_ALTER_TABLESPACE; }
        OBJECT_TRIGGER => { tag = CMDTAG_ALTER_TRIGGER; }
        OBJECT_EVENT_TRIGGER => { tag = CMDTAG_ALTER_EVENT_TRIGGER; }
        OBJECT_TSCONFIGURATION => { tag = CMDTAG_ALTER_TEXT_SEARCH_CONFIGURATION; }
        OBJECT_TSDICTIONARY => { tag = CMDTAG_ALTER_TEXT_SEARCH_DICTIONARY; }
        OBJECT_TSPARSER => { tag = CMDTAG_ALTER_TEXT_SEARCH_PARSER; }
        OBJECT_TSTEMPLATE => { tag = CMDTAG_ALTER_TEXT_SEARCH_TEMPLATE; }
        OBJECT_TYPE => { tag = CMDTAG_ALTER_TYPE; }
        OBJECT_VIEW => { tag = CMDTAG_ALTER_VIEW; }
        OBJECT_MATVIEW => { tag = CMDTAG_ALTER_MATERIALIZED_VIEW; }
        OBJECT_PUBLICATION => { tag = CMDTAG_ALTER_PUBLICATION; }
        OBJECT_SUBSCRIPTION => { tag = CMDTAG_ALTER_SUBSCRIPTION; }
        OBJECT_STATISTIC_EXT => { tag = CMDTAG_ALTER_STATISTICS; }
        _ => { tag = CMDTAG_UNKNOWN; }
    }
    tag
}

/*
 * CreateCommandTag
 *		utility to get a CommandTag for the command operation,
 *		given either a raw (un-analyzed) parsetree, an analyzed Query,
 *		or a PlannedStmt.
 *
 * This must handle all command types, but since the vast majority
 * of 'em are utility commands, it seems sensible to keep it here.
 */
pub unsafe fn CreateCommandTag(parsetree: *mut Node) -> CommandTag {
    let tag: CommandTag;
    match nodeTag(parsetree) {
        /* recurse if we're given a RawStmt */
        T_RawStmt => {
            tag = CreateCommandTag((*(parsetree as *mut RawStmt)).stmt);
        }

        /* raw plannable queries */
        T_InsertStmt => { tag = CMDTAG_INSERT; }
        T_DeleteStmt => { tag = CMDTAG_DELETE; }
        T_UpdateStmt => { tag = CMDTAG_UPDATE; }
        T_MergeStmt => { tag = CMDTAG_MERGE; }
        T_SelectStmt => { tag = CMDTAG_SELECT; }
        T_PLAssignStmt => { tag = CMDTAG_SELECT; }

        /* utility statements --- same whether raw or cooked */
        T_TransactionStmt => {
            let stmt = parsetree as *mut TransactionStmt;
            match (*stmt).kind {
                TRANS_STMT_BEGIN => { tag = CMDTAG_BEGIN; }
                TRANS_STMT_START => { tag = CMDTAG_START_TRANSACTION; }
                TRANS_STMT_COMMIT => { tag = CMDTAG_COMMIT; }
                TRANS_STMT_ROLLBACK | TRANS_STMT_ROLLBACK_TO => { tag = CMDTAG_ROLLBACK; }
                TRANS_STMT_SAVEPOINT => { tag = CMDTAG_SAVEPOINT; }
                TRANS_STMT_RELEASE => { tag = CMDTAG_RELEASE; }
                TRANS_STMT_PREPARE => { tag = CMDTAG_PREPARE_TRANSACTION; }
                TRANS_STMT_COMMIT_PREPARED => { tag = CMDTAG_COMMIT_PREPARED; }
                TRANS_STMT_ROLLBACK_PREPARED => { tag = CMDTAG_ROLLBACK_PREPARED; }
                _ => { tag = CMDTAG_UNKNOWN; }
            }
        }

        T_DeclareCursorStmt => { tag = CMDTAG_DECLARE_CURSOR; }

        T_ClosePortalStmt => {
            let stmt = parsetree as *mut ClosePortalStmt;
            if (*stmt).portalname.is_null() {
                tag = CMDTAG_CLOSE_CURSOR_ALL;
            } else {
                tag = CMDTAG_CLOSE_CURSOR;
            }
        }

        T_FetchStmt => {
            let stmt = parsetree as *mut FetchStmt;
            tag = if (*stmt).ismove { CMDTAG_MOVE } else { CMDTAG_FETCH };
        }

        T_CreateDomainStmt => { tag = CMDTAG_CREATE_DOMAIN; }
        T_CreateSchemaStmt => { tag = CMDTAG_CREATE_SCHEMA; }
        T_CreateStmt => { tag = CMDTAG_CREATE_TABLE; }
        T_CreateTableSpaceStmt => { tag = CMDTAG_CREATE_TABLESPACE; }
        T_DropTableSpaceStmt => { tag = CMDTAG_DROP_TABLESPACE; }
        T_AlterTableSpaceOptionsStmt => { tag = CMDTAG_ALTER_TABLESPACE; }
        T_CreateExtensionStmt => { tag = CMDTAG_CREATE_EXTENSION; }
        T_AlterExtensionStmt => { tag = CMDTAG_ALTER_EXTENSION; }
        T_AlterExtensionContentsStmt => { tag = CMDTAG_ALTER_EXTENSION; }
        T_CreateFdwStmt => { tag = CMDTAG_CREATE_FOREIGN_DATA_WRAPPER; }
        T_AlterFdwStmt => { tag = CMDTAG_ALTER_FOREIGN_DATA_WRAPPER; }
        T_CreateForeignServerStmt => { tag = CMDTAG_CREATE_SERVER; }
        T_AlterForeignServerStmt => { tag = CMDTAG_ALTER_SERVER; }
        T_CreateUserMappingStmt => { tag = CMDTAG_CREATE_USER_MAPPING; }
        T_AlterUserMappingStmt => { tag = CMDTAG_ALTER_USER_MAPPING; }
        T_DropUserMappingStmt => { tag = CMDTAG_DROP_USER_MAPPING; }
        T_CreateForeignTableStmt => { tag = CMDTAG_CREATE_FOREIGN_TABLE; }
        T_ImportForeignSchemaStmt => { tag = CMDTAG_IMPORT_FOREIGN_SCHEMA; }

        T_DropStmt => {
            match (*(parsetree as *mut DropStmt)).removeType {
                OBJECT_TABLE => { tag = CMDTAG_DROP_TABLE; }
                OBJECT_SEQUENCE => { tag = CMDTAG_DROP_SEQUENCE; }
                OBJECT_VIEW => { tag = CMDTAG_DROP_VIEW; }
                OBJECT_MATVIEW => { tag = CMDTAG_DROP_MATERIALIZED_VIEW; }
                OBJECT_INDEX => { tag = CMDTAG_DROP_INDEX; }
                OBJECT_TYPE => { tag = CMDTAG_DROP_TYPE; }
                OBJECT_DOMAIN => { tag = CMDTAG_DROP_DOMAIN; }
                OBJECT_COLLATION => { tag = CMDTAG_DROP_COLLATION; }
                OBJECT_CONVERSION => { tag = CMDTAG_DROP_CONVERSION; }
                OBJECT_SCHEMA => { tag = CMDTAG_DROP_SCHEMA; }
                OBJECT_TSPARSER => { tag = CMDTAG_DROP_TEXT_SEARCH_PARSER; }
                OBJECT_TSDICTIONARY => { tag = CMDTAG_DROP_TEXT_SEARCH_DICTIONARY; }
                OBJECT_TSTEMPLATE => { tag = CMDTAG_DROP_TEXT_SEARCH_TEMPLATE; }
                OBJECT_TSCONFIGURATION => { tag = CMDTAG_DROP_TEXT_SEARCH_CONFIGURATION; }
                OBJECT_FOREIGN_TABLE => { tag = CMDTAG_DROP_FOREIGN_TABLE; }
                OBJECT_EXTENSION => { tag = CMDTAG_DROP_EXTENSION; }
                OBJECT_FUNCTION => { tag = CMDTAG_DROP_FUNCTION; }
                OBJECT_PROCEDURE => { tag = CMDTAG_DROP_PROCEDURE; }
                OBJECT_ROUTINE => { tag = CMDTAG_DROP_ROUTINE; }
                OBJECT_AGGREGATE => { tag = CMDTAG_DROP_AGGREGATE; }
                OBJECT_OPERATOR => { tag = CMDTAG_DROP_OPERATOR; }
                OBJECT_LANGUAGE => { tag = CMDTAG_DROP_LANGUAGE; }
                OBJECT_CAST => { tag = CMDTAG_DROP_CAST; }
                OBJECT_TRIGGER => { tag = CMDTAG_DROP_TRIGGER; }
                OBJECT_EVENT_TRIGGER => { tag = CMDTAG_DROP_EVENT_TRIGGER; }
                OBJECT_RULE => { tag = CMDTAG_DROP_RULE; }
                OBJECT_FDW => { tag = CMDTAG_DROP_FOREIGN_DATA_WRAPPER; }
                OBJECT_FOREIGN_SERVER => { tag = CMDTAG_DROP_SERVER; }
                OBJECT_OPCLASS => { tag = CMDTAG_DROP_OPERATOR_CLASS; }
                OBJECT_OPFAMILY => { tag = CMDTAG_DROP_OPERATOR_FAMILY; }
                OBJECT_POLICY => { tag = CMDTAG_DROP_POLICY; }
                OBJECT_TRANSFORM => { tag = CMDTAG_DROP_TRANSFORM; }
                OBJECT_ACCESS_METHOD => { tag = CMDTAG_DROP_ACCESS_METHOD; }
                OBJECT_PUBLICATION => { tag = CMDTAG_DROP_PUBLICATION; }
                OBJECT_STATISTIC_EXT => { tag = CMDTAG_DROP_STATISTICS; }
                _ => { tag = CMDTAG_UNKNOWN; }
            }
        }

        T_TruncateStmt => { tag = CMDTAG_TRUNCATE_TABLE; }
        T_CommentStmt => { tag = CMDTAG_COMMENT; }
        T_SecLabelStmt => { tag = CMDTAG_SECURITY_LABEL; }
        T_CopyStmt => { tag = CMDTAG_COPY; }

        T_RenameStmt => {
            /*
             * When the column is renamed, the command tag is created from its
             * relation type
             */
            let s = parsetree as *mut RenameStmt;
            tag = AlterObjectTypeCommandTag(
                if (*s).renameType == OBJECT_COLUMN { (*s).relationType } else { (*s).renameType }
            );
        }

        T_AlterObjectDependsStmt => {
            tag = AlterObjectTypeCommandTag((*(parsetree as *mut AlterObjectDependsStmt)).objectType);
        }
        T_AlterObjectSchemaStmt => {
            tag = AlterObjectTypeCommandTag((*(parsetree as *mut AlterObjectSchemaStmt)).objectType);
        }
        T_AlterOwnerStmt => {
            tag = AlterObjectTypeCommandTag((*(parsetree as *mut AlterOwnerStmt)).objectType);
        }
        T_AlterTableMoveAllStmt => {
            tag = AlterObjectTypeCommandTag((*(parsetree as *mut AlterTableMoveAllStmt)).objtype);
        }
        T_AlterTableStmt => {
            tag = AlterObjectTypeCommandTag((*(parsetree as *mut AlterTableStmt)).objtype);
        }

        T_AlterDomainStmt => { tag = CMDTAG_ALTER_DOMAIN; }

        T_AlterFunctionStmt => {
            match (*(parsetree as *mut AlterFunctionStmt)).objtype {
                OBJECT_FUNCTION => { tag = CMDTAG_ALTER_FUNCTION; }
                OBJECT_PROCEDURE => { tag = CMDTAG_ALTER_PROCEDURE; }
                OBJECT_ROUTINE => { tag = CMDTAG_ALTER_ROUTINE; }
                _ => { tag = CMDTAG_UNKNOWN; }
            }
        }

        T_GrantStmt => {
            let stmt = parsetree as *mut GrantStmt;
            tag = if (*stmt).is_grant { CMDTAG_GRANT } else { CMDTAG_REVOKE };
        }

        T_GrantRoleStmt => {
            let stmt = parsetree as *mut GrantRoleStmt;
            tag = if (*stmt).is_grant { CMDTAG_GRANT_ROLE } else { CMDTAG_REVOKE_ROLE };
        }

        T_AlterDefaultPrivilegesStmt => { tag = CMDTAG_ALTER_DEFAULT_PRIVILEGES; }

        T_DefineStmt => {
            match (*(parsetree as *mut DefineStmt)).kind {
                OBJECT_AGGREGATE => { tag = CMDTAG_CREATE_AGGREGATE; }
                OBJECT_OPERATOR => { tag = CMDTAG_CREATE_OPERATOR; }
                OBJECT_TYPE => { tag = CMDTAG_CREATE_TYPE; }
                OBJECT_TSPARSER => { tag = CMDTAG_CREATE_TEXT_SEARCH_PARSER; }
                OBJECT_TSDICTIONARY => { tag = CMDTAG_CREATE_TEXT_SEARCH_DICTIONARY; }
                OBJECT_TSTEMPLATE => { tag = CMDTAG_CREATE_TEXT_SEARCH_TEMPLATE; }
                OBJECT_TSCONFIGURATION => { tag = CMDTAG_CREATE_TEXT_SEARCH_CONFIGURATION; }
                OBJECT_COLLATION => { tag = CMDTAG_CREATE_COLLATION; }
                OBJECT_ACCESS_METHOD => { tag = CMDTAG_CREATE_ACCESS_METHOD; }
                _ => { tag = CMDTAG_UNKNOWN; }
            }
        }

        T_CompositeTypeStmt => { tag = CMDTAG_CREATE_TYPE; }
        T_CreateEnumStmt => { tag = CMDTAG_CREATE_TYPE; }
        T_CreateRangeStmt => { tag = CMDTAG_CREATE_TYPE; }
        T_AlterEnumStmt => { tag = CMDTAG_ALTER_TYPE; }
        T_ViewStmt => { tag = CMDTAG_CREATE_VIEW; }

        T_CreateFunctionStmt => {
            if (*(parsetree as *mut CreateFunctionStmt)).is_procedure {
                tag = CMDTAG_CREATE_PROCEDURE;
            } else {
                tag = CMDTAG_CREATE_FUNCTION;
            }
        }

        T_IndexStmt => { tag = CMDTAG_CREATE_INDEX; }
        T_RuleStmt => { tag = CMDTAG_CREATE_RULE; }
        T_CreateSeqStmt => { tag = CMDTAG_CREATE_SEQUENCE; }
        T_AlterSeqStmt => { tag = CMDTAG_ALTER_SEQUENCE; }
        T_DoStmt => { tag = CMDTAG_DO; }
        T_CreatedbStmt => { tag = CMDTAG_CREATE_DATABASE; }

        T_AlterDatabaseStmt | T_AlterDatabaseRefreshCollStmt | T_AlterDatabaseSetStmt => {
            tag = CMDTAG_ALTER_DATABASE;
        }

        T_DropdbStmt => { tag = CMDTAG_DROP_DATABASE; }
        T_NotifyStmt => { tag = CMDTAG_NOTIFY; }
        T_ListenStmt => { tag = CMDTAG_LISTEN; }
        T_UnlistenStmt => { tag = CMDTAG_UNLISTEN; }
        T_LoadStmt => { tag = CMDTAG_LOAD; }
        T_CallStmt => { tag = CMDTAG_CALL; }
        T_ClusterStmt => { tag = CMDTAG_CLUSTER; }

        T_VacuumStmt => {
            if (*(parsetree as *mut VacuumStmt)).is_vacuumcmd {
                tag = CMDTAG_VACUUM;
            } else {
                tag = CMDTAG_ANALYZE;
            }
        }

        T_ExplainStmt => { tag = CMDTAG_EXPLAIN; }

        T_CreateTableAsStmt => {
            match (*(parsetree as *mut CreateTableAsStmt)).objtype {
                OBJECT_TABLE => {
                    if (*(parsetree as *mut CreateTableAsStmt)).is_select_into {
                        tag = CMDTAG_SELECT_INTO;
                    } else {
                        tag = CMDTAG_CREATE_TABLE_AS;
                    }
                }
                OBJECT_MATVIEW => { tag = CMDTAG_CREATE_MATERIALIZED_VIEW; }
                _ => { tag = CMDTAG_UNKNOWN; }
            }
        }

        T_RefreshMatViewStmt => { tag = CMDTAG_REFRESH_MATERIALIZED_VIEW; }
        T_AlterSystemStmt => { tag = CMDTAG_ALTER_SYSTEM; }

        T_VariableSetStmt => {
            match (*(parsetree as *mut VariableSetStmt)).kind {
                VAR_SET_VALUE | VAR_SET_CURRENT | VAR_SET_DEFAULT | VAR_SET_MULTI => {
                    tag = CMDTAG_SET;
                }
                VAR_RESET | VAR_RESET_ALL => {
                    tag = CMDTAG_RESET;
                }
                _ => { tag = CMDTAG_UNKNOWN; }
            }
        }

        T_VariableShowStmt => { tag = CMDTAG_SHOW; }

        T_DiscardStmt => {
            match (*(parsetree as *mut DiscardStmt)).target {
                DISCARD_ALL => { tag = CMDTAG_DISCARD_ALL; }
                DISCARD_PLANS => { tag = CMDTAG_DISCARD_PLANS; }
                DISCARD_TEMP => { tag = CMDTAG_DISCARD_TEMP; }
                DISCARD_SEQUENCES => { tag = CMDTAG_DISCARD_SEQUENCES; }
                _ => { tag = CMDTAG_UNKNOWN; }
            }
        }

        T_CreateTransformStmt => { tag = CMDTAG_CREATE_TRANSFORM; }
        T_CreateTrigStmt => { tag = CMDTAG_CREATE_TRIGGER; }
        T_CreateEventTrigStmt => { tag = CMDTAG_CREATE_EVENT_TRIGGER; }
        T_AlterEventTrigStmt => { tag = CMDTAG_ALTER_EVENT_TRIGGER; }
        T_CreatePLangStmt => { tag = CMDTAG_CREATE_LANGUAGE; }
        T_CreateRoleStmt => { tag = CMDTAG_CREATE_ROLE; }
        T_AlterRoleStmt => { tag = CMDTAG_ALTER_ROLE; }
        T_AlterRoleSetStmt => { tag = CMDTAG_ALTER_ROLE; }
        T_DropRoleStmt => { tag = CMDTAG_DROP_ROLE; }
        T_DropOwnedStmt => { tag = CMDTAG_DROP_OWNED; }
        T_ReassignOwnedStmt => { tag = CMDTAG_REASSIGN_OWNED; }
        T_LockStmt => { tag = CMDTAG_LOCK_TABLE; }
        T_ConstraintsSetStmt => { tag = CMDTAG_SET_CONSTRAINTS; }
        T_CheckPointStmt => { tag = CMDTAG_CHECKPOINT; }
        T_ReindexStmt => { tag = CMDTAG_REINDEX; }
        T_CreateConversionStmt => { tag = CMDTAG_CREATE_CONVERSION; }
        T_CreateCastStmt => { tag = CMDTAG_CREATE_CAST; }
        T_CreateOpClassStmt => { tag = CMDTAG_CREATE_OPERATOR_CLASS; }
        T_CreateOpFamilyStmt => { tag = CMDTAG_CREATE_OPERATOR_FAMILY; }
        T_AlterOpFamilyStmt => { tag = CMDTAG_ALTER_OPERATOR_FAMILY; }
        T_AlterOperatorStmt => { tag = CMDTAG_ALTER_OPERATOR; }
        T_AlterTypeStmt => { tag = CMDTAG_ALTER_TYPE; }
        T_AlterTSDictionaryStmt => { tag = CMDTAG_ALTER_TEXT_SEARCH_DICTIONARY; }
        T_AlterTSConfigurationStmt => { tag = CMDTAG_ALTER_TEXT_SEARCH_CONFIGURATION; }
        T_CreatePolicyStmt => { tag = CMDTAG_CREATE_POLICY; }
        T_AlterPolicyStmt => { tag = CMDTAG_ALTER_POLICY; }
        T_CreateAmStmt => { tag = CMDTAG_CREATE_ACCESS_METHOD; }
        T_CreatePublicationStmt => { tag = CMDTAG_CREATE_PUBLICATION; }
        T_AlterPublicationStmt => { tag = CMDTAG_ALTER_PUBLICATION; }
        T_CreateSubscriptionStmt => { tag = CMDTAG_CREATE_SUBSCRIPTION; }
        T_AlterSubscriptionStmt => { tag = CMDTAG_ALTER_SUBSCRIPTION; }
        T_DropSubscriptionStmt => { tag = CMDTAG_DROP_SUBSCRIPTION; }
        T_AlterCollationStmt => { tag = CMDTAG_ALTER_COLLATION; }
        T_PrepareStmt => { tag = CMDTAG_PREPARE; }
        T_ExecuteStmt => { tag = CMDTAG_EXECUTE; }
        T_CreateStatsStmt => { tag = CMDTAG_CREATE_STATISTICS; }
        T_AlterStatsStmt => { tag = CMDTAG_ALTER_STATISTICS; }

        T_DeallocateStmt => {
            let stmt = parsetree as *mut DeallocateStmt;
            if (*stmt).name.is_null() {
                tag = CMDTAG_DEALLOCATE_ALL;
            } else {
                tag = CMDTAG_DEALLOCATE;
            }
        }

        /* already-planned queries */
        T_PlannedStmt => {
            let stmt = parsetree as *mut PlannedStmt;
            match (*stmt).commandType {
                CMD_SELECT => {
                    /*
                     * We take a little extra care here so that the result
                     * will be useful for complaints about read-only
                     * statements
                     */
                    if (*stmt).rowMarks != NIL {
                        /* not 100% but probably close enough */
                        let strength = (*(linitial((*stmt).rowMarks) as *mut PlanRowMark)).strength;
                        match strength {
                            LCS_FORKEYSHARE => { tag = CMDTAG_SELECT_FOR_KEY_SHARE; }
                            LCS_FORSHARE => { tag = CMDTAG_SELECT_FOR_SHARE; }
                            LCS_FORNOKEYUPDATE => { tag = CMDTAG_SELECT_FOR_NO_KEY_UPDATE; }
                            LCS_FORUPDATE => { tag = CMDTAG_SELECT_FOR_UPDATE; }
                            _ => { tag = CMDTAG_SELECT; }
                        }
                    } else {
                        tag = CMDTAG_SELECT;
                    }
                }
                CMD_UPDATE => { tag = CMDTAG_UPDATE; }
                CMD_INSERT => { tag = CMDTAG_INSERT; }
                CMD_DELETE => { tag = CMDTAG_DELETE; }
                CMD_MERGE => { tag = CMDTAG_MERGE; }
                CMD_UTILITY => {
                    tag = CreateCommandTag((*stmt).utilityStmt);
                }
                _ => {
                    elog!(WARNING, "unrecognized commandType: {}", (*stmt).commandType as c_int);
                    tag = CMDTAG_UNKNOWN;
                }
            }
        }

        /* parsed-and-rewritten-but-not-planned queries */
        T_Query => {
            let stmt = parsetree as *mut Query;
            match (*stmt).commandType {
                CMD_SELECT => {
                    /*
                     * We take a little extra care here so that the result
                     * will be useful for complaints about read-only
                     * statements
                     */
                    if (*stmt).rowMarks != NIL {
                        /* not 100% but probably close enough */
                        let strength = (*(linitial((*stmt).rowMarks) as *mut RowMarkClause)).strength;
                        match strength {
                            LCS_FORKEYSHARE => { tag = CMDTAG_SELECT_FOR_KEY_SHARE; }
                            LCS_FORSHARE => { tag = CMDTAG_SELECT_FOR_SHARE; }
                            LCS_FORNOKEYUPDATE => { tag = CMDTAG_SELECT_FOR_NO_KEY_UPDATE; }
                            LCS_FORUPDATE => { tag = CMDTAG_SELECT_FOR_UPDATE; }
                            _ => { tag = CMDTAG_UNKNOWN; }
                        }
                    } else {
                        tag = CMDTAG_SELECT;
                    }
                }
                CMD_UPDATE => { tag = CMDTAG_UPDATE; }
                CMD_INSERT => { tag = CMDTAG_INSERT; }
                CMD_DELETE => { tag = CMDTAG_DELETE; }
                CMD_MERGE => { tag = CMDTAG_MERGE; }
                CMD_UTILITY => {
                    tag = CreateCommandTag((*stmt).utilityStmt);
                }
                _ => {
                    elog!(WARNING, "unrecognized commandType: {}", (*stmt).commandType as c_int);
                    tag = CMDTAG_UNKNOWN;
                }
            }
        }

        _ => {
            elog!(WARNING, "unrecognized node type: {}", nodeTag(parsetree) as c_int);
            tag = CMDTAG_UNKNOWN;
        }
    }

    tag
}

/*
 * GetCommandLogLevel
 *		utility to get the minimum log_statement level for a command,
 *		given either a raw (un-analyzed) parsetree, an analyzed Query,
 *		or a PlannedStmt.
 *
 * This must handle all command types, but since the vast majority
 * of 'em are utility commands, it seems sensible to keep it here.
 */
pub unsafe fn GetCommandLogLevel(parsetree: *mut Node) -> LogStmtLevel {
    let lev: LogStmtLevel;
    match nodeTag(parsetree) {
        /* recurse if we're given a RawStmt */
        T_RawStmt => {
            lev = GetCommandLogLevel((*(parsetree as *mut RawStmt)).stmt);
        }

        /* raw plannable queries */
        T_InsertStmt | T_DeleteStmt | T_UpdateStmt | T_MergeStmt => {
            lev = LOGSTMT_MOD;
        }

        T_SelectStmt => {
            if !(*(parsetree as *mut SelectStmt)).intoClause.is_null() {
                lev = LOGSTMT_DDL; /* SELECT INTO */
            } else {
                lev = LOGSTMT_ALL;
            }
        }

        T_PLAssignStmt => { lev = LOGSTMT_ALL; }

        /* utility statements --- same whether raw or cooked */
        T_TransactionStmt => { lev = LOGSTMT_ALL; }
        T_DeclareCursorStmt => { lev = LOGSTMT_ALL; }
        T_ClosePortalStmt => { lev = LOGSTMT_ALL; }
        T_FetchStmt => { lev = LOGSTMT_ALL; }
        T_CreateSchemaStmt => { lev = LOGSTMT_DDL; }
        T_CreateStmt | T_CreateForeignTableStmt => { lev = LOGSTMT_DDL; }

        T_CreateTableSpaceStmt | T_DropTableSpaceStmt | T_AlterTableSpaceOptionsStmt => {
            lev = LOGSTMT_DDL;
        }

        T_CreateExtensionStmt | T_AlterExtensionStmt | T_AlterExtensionContentsStmt => {
            lev = LOGSTMT_DDL;
        }

        T_CreateFdwStmt
        | T_AlterFdwStmt
        | T_CreateForeignServerStmt
        | T_AlterForeignServerStmt
        | T_CreateUserMappingStmt
        | T_AlterUserMappingStmt
        | T_DropUserMappingStmt
        | T_ImportForeignSchemaStmt => {
            lev = LOGSTMT_DDL;
        }

        T_DropStmt => { lev = LOGSTMT_DDL; }
        T_TruncateStmt => { lev = LOGSTMT_MOD; }
        T_CommentStmt => { lev = LOGSTMT_DDL; }
        T_SecLabelStmt => { lev = LOGSTMT_DDL; }

        T_CopyStmt => {
            if (*(parsetree as *mut CopyStmt)).is_from {
                lev = LOGSTMT_MOD;
            } else {
                lev = LOGSTMT_ALL;
            }
        }

        T_PrepareStmt => {
            let stmt = parsetree as *mut PrepareStmt;
            /* Look through a PREPARE to the contained stmt */
            lev = GetCommandLogLevel((*stmt).query);
        }

        T_ExecuteStmt => {
            let stmt = parsetree as *mut ExecuteStmt;
            let ps = FetchPreparedStatement((*stmt).name, false);
            /* Look through an EXECUTE to the referenced stmt */
            if !ps.is_null() && !(*(*ps).plansource).raw_parse_tree.is_null() {
                lev = GetCommandLogLevel((*(*(*ps).plansource).raw_parse_tree).stmt);
            } else {
                lev = LOGSTMT_ALL;
            }
        }

        T_DeallocateStmt => { lev = LOGSTMT_ALL; }
        T_RenameStmt => { lev = LOGSTMT_DDL; }
        T_AlterObjectDependsStmt => { lev = LOGSTMT_DDL; }
        T_AlterObjectSchemaStmt => { lev = LOGSTMT_DDL; }
        T_AlterOwnerStmt => { lev = LOGSTMT_DDL; }
        T_AlterOperatorStmt => { lev = LOGSTMT_DDL; }
        T_AlterTypeStmt => { lev = LOGSTMT_DDL; }
        T_AlterTableMoveAllStmt | T_AlterTableStmt => { lev = LOGSTMT_DDL; }
        T_AlterDomainStmt => { lev = LOGSTMT_DDL; }
        T_GrantStmt => { lev = LOGSTMT_DDL; }
        T_GrantRoleStmt => { lev = LOGSTMT_DDL; }
        T_AlterDefaultPrivilegesStmt => { lev = LOGSTMT_DDL; }
        T_DefineStmt => { lev = LOGSTMT_DDL; }
        T_CompositeTypeStmt => { lev = LOGSTMT_DDL; }
        T_CreateEnumStmt => { lev = LOGSTMT_DDL; }
        T_CreateRangeStmt => { lev = LOGSTMT_DDL; }
        T_AlterEnumStmt => { lev = LOGSTMT_DDL; }
        T_ViewStmt => { lev = LOGSTMT_DDL; }
        T_CreateFunctionStmt => { lev = LOGSTMT_DDL; }
        T_AlterFunctionStmt => { lev = LOGSTMT_DDL; }
        T_IndexStmt => { lev = LOGSTMT_DDL; }
        T_RuleStmt => { lev = LOGSTMT_DDL; }
        T_CreateSeqStmt => { lev = LOGSTMT_DDL; }
        T_AlterSeqStmt => { lev = LOGSTMT_DDL; }
        T_DoStmt => { lev = LOGSTMT_ALL; }
        T_CreatedbStmt => { lev = LOGSTMT_DDL; }

        T_AlterDatabaseStmt | T_AlterDatabaseRefreshCollStmt | T_AlterDatabaseSetStmt => {
            lev = LOGSTMT_DDL;
        }

        T_DropdbStmt => { lev = LOGSTMT_DDL; }
        T_NotifyStmt => { lev = LOGSTMT_ALL; }
        T_ListenStmt => { lev = LOGSTMT_ALL; }
        T_UnlistenStmt => { lev = LOGSTMT_ALL; }
        T_LoadStmt => { lev = LOGSTMT_ALL; }
        T_CallStmt => { lev = LOGSTMT_ALL; }
        T_ClusterStmt => { lev = LOGSTMT_DDL; }
        T_VacuumStmt => { lev = LOGSTMT_ALL; }

        T_ExplainStmt => {
            let stmt = parsetree as *mut ExplainStmt;
            let mut analyze = false;
            /* Look through an EXPLAIN ANALYZE to the contained stmt */
            foreach!(lc, (*stmt).options, {
                let opt = crate::current_cell!(lc) as *mut DefElem;
                if strcmp((*opt).defname, b"analyze\0".as_ptr() as *const c_char) == 0 {
                    analyze = defGetBoolean(opt);
                }
                /* don't "break", as explain.c will use the last value */
            });
            if analyze {
                return GetCommandLogLevel((*stmt).query);
            }
            /* Plain EXPLAIN isn't so interesting */
            lev = LOGSTMT_ALL;
        }

        T_CreateTableAsStmt => { lev = LOGSTMT_DDL; }
        T_RefreshMatViewStmt => { lev = LOGSTMT_DDL; }
        T_AlterSystemStmt => { lev = LOGSTMT_DDL; }
        T_VariableSetStmt => { lev = LOGSTMT_ALL; }
        T_VariableShowStmt => { lev = LOGSTMT_ALL; }
        T_DiscardStmt => { lev = LOGSTMT_ALL; }
        T_CreateTrigStmt => { lev = LOGSTMT_DDL; }
        T_CreateEventTrigStmt => { lev = LOGSTMT_DDL; }
        T_AlterEventTrigStmt => { lev = LOGSTMT_DDL; }
        T_CreatePLangStmt => { lev = LOGSTMT_DDL; }
        T_CreateDomainStmt => { lev = LOGSTMT_DDL; }
        T_CreateRoleStmt => { lev = LOGSTMT_DDL; }
        T_AlterRoleStmt => { lev = LOGSTMT_DDL; }
        T_AlterRoleSetStmt => { lev = LOGSTMT_DDL; }
        T_DropRoleStmt => { lev = LOGSTMT_DDL; }
        T_DropOwnedStmt => { lev = LOGSTMT_DDL; }
        T_ReassignOwnedStmt => { lev = LOGSTMT_DDL; }
        T_LockStmt => { lev = LOGSTMT_ALL; }
        T_ConstraintsSetStmt => { lev = LOGSTMT_ALL; }
        T_CheckPointStmt => { lev = LOGSTMT_ALL; }
        T_ReindexStmt => { lev = LOGSTMT_ALL; /* should this be DDL? */ }
        T_CreateConversionStmt => { lev = LOGSTMT_DDL; }
        T_CreateCastStmt => { lev = LOGSTMT_DDL; }
        T_CreateOpClassStmt => { lev = LOGSTMT_DDL; }
        T_CreateOpFamilyStmt => { lev = LOGSTMT_DDL; }
        T_CreateTransformStmt => { lev = LOGSTMT_DDL; }
        T_AlterOpFamilyStmt => { lev = LOGSTMT_DDL; }
        T_CreatePolicyStmt => { lev = LOGSTMT_DDL; }
        T_AlterPolicyStmt => { lev = LOGSTMT_DDL; }
        T_AlterTSDictionaryStmt => { lev = LOGSTMT_DDL; }
        T_AlterTSConfigurationStmt => { lev = LOGSTMT_DDL; }
        T_CreateAmStmt => { lev = LOGSTMT_DDL; }
        T_CreatePublicationStmt => { lev = LOGSTMT_DDL; }
        T_AlterPublicationStmt => { lev = LOGSTMT_DDL; }
        T_CreateSubscriptionStmt => { lev = LOGSTMT_DDL; }
        T_AlterSubscriptionStmt => { lev = LOGSTMT_DDL; }
        T_DropSubscriptionStmt => { lev = LOGSTMT_DDL; }
        T_CreateStatsStmt => { lev = LOGSTMT_DDL; }
        T_AlterStatsStmt => { lev = LOGSTMT_DDL; }
        T_AlterCollationStmt => { lev = LOGSTMT_DDL; }

        /* already-planned queries */
        T_PlannedStmt => {
            let stmt = parsetree as *mut PlannedStmt;
            match (*stmt).commandType {
                CMD_SELECT => { lev = LOGSTMT_ALL; }
                CMD_UPDATE | CMD_INSERT | CMD_DELETE | CMD_MERGE => { lev = LOGSTMT_MOD; }
                CMD_UTILITY => {
                    lev = GetCommandLogLevel((*stmt).utilityStmt);
                }
                _ => {
                    elog!(WARNING, "unrecognized commandType: {}", (*stmt).commandType as c_int);
                    lev = LOGSTMT_ALL;
                }
            }
        }

        /* parsed-and-rewritten-but-not-planned queries */
        T_Query => {
            let stmt = parsetree as *mut Query;
            match (*stmt).commandType {
                CMD_SELECT => { lev = LOGSTMT_ALL; }
                CMD_UPDATE | CMD_INSERT | CMD_DELETE | CMD_MERGE => { lev = LOGSTMT_MOD; }
                CMD_UTILITY => {
                    lev = GetCommandLogLevel((*stmt).utilityStmt);
                }
                _ => {
                    elog!(WARNING, "unrecognized commandType: {}", (*stmt).commandType as c_int);
                    lev = LOGSTMT_ALL;
                }
            }
        }

        _ => {
            elog!(WARNING, "unrecognized node type: {}", nodeTag(parsetree) as c_int);
            lev = LOGSTMT_ALL;
        }
    }

    lev
}
