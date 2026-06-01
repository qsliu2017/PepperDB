//! src/backend/tcop/postgres.c
//!
//! POSTGRES C Backend Interface
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/tcop/postgres.c
//!
//! NOTES
//!   this is the "main" module of the postgres backend and
//!   hence the main module of the "traffic cop".

use crate::prelude::*;

use crate::nodes::nodes::{Node, NodeTag};
use crate::nodes::pg_list::{lfirst, lnext, list_length, List, NIL};
use crate::catalog::pg_type_d::UNKNOWNOID;
use crate::parser::parser::RawParseMode::RAW_PARSE_DEFAULT;
use crate::storage::ipc::sinval::catchupInterruptPending;
use crate::utils::elog::COMMERROR;
use crate::{
    castNode, current_cell, foreach, lfirst_node, linitial_node, list_make1, makeNode, IsA,
};
use std::ffi::{CStr, CString};

// On macOS the C global stdio handles are exposed as __stdoutp / __stdinp, and
// the standard `stdout`/`stdin` macros are not symbols.  Declare them directly.
extern "C" {
    #[link_name = "__stdoutp"]
    static stdout_ptr: *mut libc::FILE;
    #[link_name = "__stdinp"]
    static stdin_ptr: *mut libc::FILE;
}

macro_rules! errdetail { ($($arg:tt)*) => { () }; }
macro_rules! errhint { ($($arg:tt)*) => { () }; }
macro_rules! errcontext { ($($arg:tt)*) => { () }; }
macro_rules! errmsg_internal { ($fmt:literal $(, $arg:expr)*) => { errmsg!($fmt $(, $arg)*) }; }
macro_rules! errdetail_internal { ($($arg:tt)*) => { () }; }
macro_rules! errdetail_abort { ($($arg:tt)*) => { () }; }
macro_rules! errdetail_execute { ($($arg:tt)*) => { () }; }
macro_rules! errdetail_params { ($($arg:tt)*) => { () }; }
macro_rules! errdetail_recovery_conflict { ($($arg:tt)*) => { () }; }
macro_rules! CHECK_FOR_INTERRUPTS { () => {{}}; }
macro_rules! HOLD_CANCEL_INTERRUPTS { () => {{}}; }
macro_rules! RESUME_CANCEL_INTERRUPTS { () => {{}}; }
macro_rules! TRACE_POSTGRESQL_QUERY_START { ($($arg:tt)*) => { () }; }
macro_rules! TRACE_POSTGRESQL_QUERY_DONE { ($($arg:tt)*) => { () }; }
macro_rules! TRACE_POSTGRESQL_QUERY_PLAN_START { ($($arg:tt)*) => { () }; }
macro_rules! TRACE_POSTGRESQL_QUERY_PLAN_DONE { ($($arg:tt)*) => { () }; }
macro_rules! TRACE_POSTGRESQL_QUERY_PARSE_START { ($($arg:tt)*) => { () }; }
macro_rules! TRACE_POSTGRESQL_QUERY_PARSE_DONE { ($($arg:tt)*) => { () }; }
macro_rules! TRACE_POSTGRESQL_QUERY_REWRITE_START { ($($arg:tt)*) => { () }; }
macro_rules! TRACE_POSTGRESQL_QUERY_REWRITE_DONE { ($($arg:tt)*) => { () }; }

// ---------------------------------------------------------------------------
// Type aliases matching C typedefs used in this file
// ---------------------------------------------------------------------------

/// CommandDest enum (matches dest.h)
pub type CommandDest = c_int;
pub const DestDebug: CommandDest = 0;
pub const DestNone: CommandDest = 1;
pub const DestRemote: CommandDest = 2;
pub const DestRemoteExecute: CommandDest = 3;
pub const DestTuplestore: CommandDest = 4;
pub const DestIntoRel: CommandDest = 5;
pub const DestCopyOut: CommandDest = 6;
pub const DestSQLFunction: CommandDest = 7;
pub const DestTransientRel: CommandDest = 8;
pub const DestErrorFunc: CommandDest = 9;

/// log_statement values (from GUC)
pub type LogStmtLevel = c_int;
pub const LOGSTMT_NONE: LogStmtLevel = 0;
pub const LOGSTMT_DDL: LogStmtLevel = 1;
pub const LOGSTMT_MOD: LogStmtLevel = 2;
pub const LOGSTMT_ALL: LogStmtLevel = 3;

/// type of argument for bind_param_error_callback
#[repr(C)]
pub struct BindParamCbData {
    pub portalName: *const c_char,
    pub paramno: c_int,    // zero-based param number, or -1 initially
    pub paramval: *const c_char, // textual input string, if available
}

// ---------------------------------------------------------------------------
// Stubs for unported dependencies
// ---------------------------------------------------------------------------

pub type Oid = u32;
pub type Datum = usize;
pub type CommandTag = c_int;
pub type QueryCompletion = c_void;
pub type Portal = *mut c_void;
pub type DestReceiver = c_void;
pub type ParamListInfo = *mut c_void;
pub type ParserSetupHook = *mut c_void;
pub type QueryEnvironment = c_void;
pub type CachedPlanSource = c_void;
pub type CachedPlan = c_void;
pub type PreparedStatement = c_void;
pub type RawStmt = c_void;
pub type Query = c_void;
pub type PlannedStmt = c_void;
pub type GucContext = c_int;
pub type GucSource = c_int;
pub type ProcSignalReason = c_int;
pub type ProtocolVersion = u32;
pub type ParamsErrorCbData = c_void;
pub type ErrorContextCallback = c_void;
pub type Port = c_void;
pub type StringInfo = *mut StringInfoData;

#[repr(C)]
pub struct StringInfoData {
    pub data: *mut c_char,
    pub len: c_int,
    pub maxlen: c_int,
    pub cursor: c_int,
}

// Maximum expected message sizes for pq_getmessage (src/include/libpq/libpq.h).
// PQ_LARGE_MESSAGE_LIMIT is (MaxAllocSize - 1) in C; it fits in an int and is
// assigned to the int variable maxmsglen, so we keep it as c_int here.
pub const PQ_SMALL_MESSAGE_LIMIT: c_int = 10000;
pub const PQ_LARGE_MESSAGE_LIMIT: c_int = (MaxAllocSize - 1) as c_int;

// Stub extern functions - TODO(pg-port): wire to real impls when available
unsafe fn raw_parser(_query_string: *const c_char, _mode: c_int) -> *mut List {
    unimplemented!() // TODO(pg-port): src/backend/parser/parser.c
}

unsafe fn parse_analyze_fixedparams(
    _parsetree: *mut RawStmt,
    _query_string: *const c_char,
    _paramTypes: *const Oid,
    _numParams: c_int,
    _queryEnv: *mut QueryEnvironment,
) -> *mut Query {
    unimplemented!() // TODO(pg-port): src/backend/parser/analyze.c
}

unsafe fn parse_analyze_varparams(
    _parsetree: *mut RawStmt,
    _query_string: *const c_char,
    _paramTypes: *mut *mut Oid,
    _numParams: *mut c_int,
    _queryEnv: *mut QueryEnvironment,
) -> *mut Query {
    unimplemented!() // TODO(pg-port): src/backend/parser/analyze.c
}

unsafe fn parse_analyze_withcb(
    _parsetree: *mut RawStmt,
    _query_string: *const c_char,
    _parserSetup: ParserSetupHook,
    _parserSetupArg: *mut c_void,
    _queryEnv: *mut QueryEnvironment,
) -> *mut Query {
    unimplemented!() // TODO(pg-port): src/backend/parser/analyze.c
}

unsafe fn QueryRewrite(_query: *mut Query) -> *mut List {
    unimplemented!() // TODO(pg-port): src/backend/rewrite/rewriteHandler.c
}

unsafe fn planner(
    _querytree: *mut Query,
    _query_string: *const c_char,
    _cursorOptions: c_int,
    _boundParams: ParamListInfo,
) -> *mut PlannedStmt {
    unimplemented!() // TODO(pg-port): src/backend/optimizer/plan/planner.c
}

unsafe fn analyze_requires_snapshot(_parsetree: *mut RawStmt) -> bool {
    unimplemented!() // TODO(pg-port): src/backend/parser/analyze.c
}

unsafe fn CreatePortal(_name: *const c_char, _allowDup: bool, _dupSilent: bool) -> Portal {
    unimplemented!() // TODO(pg-port): src/backend/utils/mmgr/portalmem.c
}

unsafe fn PortalDefineQuery(
    _portal: Portal,
    _prepStmtName: *const c_char,
    _sourceText: *const c_char,
    _commandTag: CommandTag,
    _stmts: *mut List,
    _cplan: *mut CachedPlan,
) {
    unimplemented!() // TODO(pg-port): src/backend/utils/mmgr/portalmem.c
}

unsafe fn PortalStart(_portal: Portal, _params: ParamListInfo, _eflags: c_int, _snapshot: *mut c_void) {
    unimplemented!() // TODO(pg-port): src/backend/tcop/pquery.c
}

unsafe fn PortalSetResultFormat(_portal: Portal, _nFormats: c_int, _formats: *mut i16) {
    unimplemented!() // TODO(pg-port): src/backend/tcop/pquery.c
}

unsafe fn PortalRun(
    _portal: Portal,
    _count: i64,
    _isTopLevel: bool,
    _dest: *mut DestReceiver,
    _altdest: *mut DestReceiver,
    _qc: *mut QueryCompletion,
) -> bool {
    unimplemented!() // TODO(pg-port): src/backend/tcop/pquery.c
}

unsafe fn PortalDrop(_portal: Portal, _isTopCommit: bool) {
    unimplemented!() // TODO(pg-port): src/backend/utils/mmgr/portalmem.c
}

unsafe fn PortalIsValid(_portal: Portal) -> bool {
    unimplemented!() // TODO(pg-port): src/backend/utils/mmgr/portalmem.c
}

unsafe fn GetPortalByName(_name: *const c_char) -> Portal {
    unimplemented!() // TODO(pg-port): src/backend/utils/mmgr/portalmem.c
}

unsafe fn CreateDestReceiver(_dest: CommandDest) -> *mut DestReceiver {
    unimplemented!() // TODO(pg-port): src/backend/tcop/dest.c
}

unsafe fn SetRemoteDestReceiverParams(_self_: *mut DestReceiver, _portal: Portal) {
    unimplemented!() // TODO(pg-port): src/backend/access/common/printtup.c
}

unsafe fn BeginCommand(_commandTag: CommandTag, _dest: CommandDest) {
    unimplemented!() // TODO(pg-port): src/backend/tcop/dest.c
}

unsafe fn EndCommand(_qc: *const QueryCompletion, _dest: CommandDest, _force_undecorated_output: bool) {
    unimplemented!() // TODO(pg-port): src/backend/tcop/dest.c
}

unsafe fn NullCommand(_dest: CommandDest) {
    unimplemented!() // TODO(pg-port): src/backend/tcop/dest.c
}

unsafe fn ReadyForQuery(_dest: CommandDest) {
    unimplemented!() // TODO(pg-port): src/backend/libpq/pqcomm.c
}

unsafe fn CreateCommandTag(_parsetree: *mut Node) -> CommandTag {
    unimplemented!() // TODO(pg-port): src/backend/tcop/cmdtag.c
}

unsafe fn GetCommandTagNameAndLen(_commandTag: CommandTag, _len: *mut usize) -> *const c_char {
    unimplemented!() // TODO(pg-port): src/backend/tcop/cmdtag.c
}

unsafe fn GetCommandLogLevel(_parsetree: *mut Node) -> c_int {
    unimplemented!() // TODO(pg-port): src/backend/tcop/utility.c
}

unsafe fn set_ps_display(_activity: *const c_char) {
    unimplemented!() // TODO(pg-port): src/backend/utils/misc/ps_status.c
}

unsafe fn set_ps_display_with_len(_activity: *const c_char, _len: usize) {
    unimplemented!() // TODO(pg-port): src/backend/utils/misc/ps_status.c
}

unsafe fn pgstat_report_activity(_state: c_int, _cmd_str: *const c_char) {
    unimplemented!() // TODO(pg-port): src/backend/utils/activity/pgstat_activity.c
}

unsafe fn pgstat_report_query_id(_queryId: u64, _force: bool) {
    unimplemented!() // TODO(pg-port): src/backend/utils/activity/pgstat_activity.c
}

unsafe fn pgstat_report_plan_id(_planId: u64, _force: bool) {
    unimplemented!() // TODO(pg-port): src/backend/utils/activity/pgstat_activity.c
}

unsafe fn pgstat_report_connect(_dbid: Oid) {
    unimplemented!() // TODO(pg-port): src/backend/utils/activity/pgstat_activity.c
}

unsafe fn pgstat_report_recovery_conflict(_reason: ProcSignalReason) {
    unimplemented!() // TODO(pg-port): src/backend/utils/activity/pgstat.c
}

unsafe fn pgstat_report_stat(_force: bool) -> i64 {
    unimplemented!() // TODO(pg-port): src/backend/utils/activity/pgstat.c
}

unsafe fn StartTransactionCommand() {
    unimplemented!() // TODO(pg-port): src/backend/access/transam/xact.c
}

unsafe fn CommitTransactionCommand() {
    unimplemented!() // TODO(pg-port): src/backend/access/transam/xact.c
}

unsafe fn AbortCurrentTransaction() {
    unimplemented!() // TODO(pg-port): src/backend/access/transam/xact.c
}

unsafe fn CommandCounterIncrement() {
    unimplemented!() // TODO(pg-port): src/backend/access/transam/xact.c
}

unsafe fn IsTransactionState() -> bool {
    unimplemented!() // TODO(pg-port): src/backend/access/transam/xact.c
}

unsafe fn IsTransactionOrTransactionBlock() -> bool {
    unimplemented!() // TODO(pg-port): src/backend/access/transam/xact.c
}

unsafe fn IsAbortedTransactionBlockState() -> bool {
    unimplemented!() // TODO(pg-port): src/backend/access/transam/xact.c
}

unsafe fn IsSubTransaction() -> bool {
    unimplemented!() // TODO(pg-port): src/backend/access/transam/xact.c
}

unsafe fn BeginImplicitTransactionBlock() {
    unimplemented!() // TODO(pg-port): src/backend/access/transam/xact.c
}

unsafe fn EndImplicitTransactionBlock() {
    unimplemented!() // TODO(pg-port): src/backend/access/transam/xact.c
}

unsafe fn GetTransactionSnapshot() -> *mut c_void {
    unimplemented!() // TODO(pg-port): src/backend/utils/time/snapmgr.c
}

unsafe fn PushActiveSnapshot(_snap: *mut c_void) {
    unimplemented!() // TODO(pg-port): src/backend/utils/time/snapmgr.c
}

unsafe fn PopActiveSnapshot() {
    unimplemented!() // TODO(pg-port): src/backend/utils/time/snapmgr.c
}

unsafe fn ActiveSnapshotSet() -> bool {
    unimplemented!() // TODO(pg-port): src/backend/utils/time/snapmgr.c
}

unsafe fn InvalidateCatalogSnapshotConditionally() {
    unimplemented!() // TODO(pg-port): src/backend/utils/time/snapmgr.c
}

unsafe fn AllocSetContextCreate(
    _parent: *mut c_void,
    _name: *const c_char,
    _minContextSize: usize,
    _initBlockSize: usize,
    _maxBlockSize: usize,
) -> *mut c_void {
    unimplemented!() // TODO(pg-port): src/backend/utils/mmgr/aset.c
}

unsafe fn MemoryContextSwitchTo(_context: *mut c_void) -> *mut c_void {
    unimplemented!() // TODO(pg-port): src/include/utils/palloc.h
}

unsafe fn MemoryContextDelete(_context: *mut c_void) {
    unimplemented!() // TODO(pg-port): src/backend/utils/mmgr/mcxt.c
}

unsafe fn MemoryContextReset(_context: *mut c_void) {
    unimplemented!() // TODO(pg-port): src/backend/utils/mmgr/mcxt.c
}

unsafe fn MemoryContextSetParent(_context: *mut c_void, _newparent: *mut c_void) {
    unimplemented!() // TODO(pg-port): src/backend/utils/mmgr/mcxt.c
}

unsafe fn MemoryContextCheck(_context: *mut c_void) {
    unimplemented!() // TODO(pg-port): src/backend/utils/mmgr/mcxt.c
}

unsafe fn MemoryContextStats(_context: *mut c_void) {
    unimplemented!() // TODO(pg-port): src/backend/utils/mmgr/mcxt.c
}

unsafe fn FlushErrorState() {
    unimplemented!() // TODO(pg-port): src/backend/utils/error/elog.c
}

unsafe fn EmitErrorReport() {
    unimplemented!() // TODO(pg-port): src/backend/utils/error/elog.c
}

unsafe fn elog_node_display(_lev: c_int, _title: *const c_char, _obj: *mut c_void, _pretty: bool) {
    unimplemented!() // TODO(pg-port): src/backend/nodes/print.c
}

unsafe fn copyObject(_obj: *mut c_void) -> *mut c_void {
    unimplemented!() // TODO(pg-port): src/backend/nodes/copyfuncs.c
}

unsafe fn equal(_a: *mut c_void, _b: *mut c_void) -> bool {
    unimplemented!() // TODO(pg-port): src/backend/nodes/equalfuncs.c
}

unsafe fn nodeToStringWithLocations(_obj: *mut c_void) -> *mut c_char {
    unimplemented!() // TODO(pg-port): src/backend/nodes/outfuncs.c
}

unsafe fn stringToNodeWithLocations(_str: *const c_char) -> *mut c_void {
    unimplemented!() // TODO(pg-port): src/backend/nodes/readfuncs.c
}

unsafe fn resetStringInfo(_str: StringInfo) {
    unimplemented!() // TODO(pg-port): src/backend/lib/stringinfo.c
}

unsafe fn initStringInfo(_str: *mut StringInfoData) {
    unimplemented!() // TODO(pg-port): src/backend/lib/stringinfo.c
}

unsafe fn initReadOnlyStringInfo(_str: *mut StringInfoData, _data: *mut c_char, _len: c_int) {
    unimplemented!() // TODO(pg-port): src/backend/lib/stringinfo.c
}

unsafe fn appendStringInfoChar(_str: StringInfo, _ch: c_char) {
    unimplemented!() // TODO(pg-port): src/backend/lib/stringinfo.c
}

unsafe fn appendStringInfoString(_str: StringInfo, _s: *const c_char) {
    unimplemented!() // TODO(pg-port): src/backend/lib/stringinfo.c
}

unsafe fn appendStringInfo(_str: StringInfo, _fmt: *const c_char) {
    unimplemented!() // TODO(pg-port): src/backend/lib/stringinfo.c
}

unsafe fn appendStringInfoStringQuoted(_str: StringInfo, _s: *const c_char, _maxlen: c_int) {
    unimplemented!() // TODO(pg-port): src/backend/mb/stringinfo_mb.c
}

unsafe fn pq_startmsgread() {
    unimplemented!() // TODO(pg-port): src/backend/libpq/pqmq.c
}

unsafe fn pq_getbyte() -> c_int {
    unimplemented!() // TODO(pg-port): src/backend/libpq/pqcomm.c
}

unsafe fn pq_getmessage(_s: StringInfo, _maxlen: c_int) -> c_int {
    unimplemented!() // TODO(pg-port): src/backend/libpq/pqcomm.c
}

unsafe fn pq_getmsgstring(_s: StringInfo) -> *const c_char {
    unimplemented!() // TODO(pg-port): src/backend/libpq/pqformat.c
}

unsafe fn pq_getmsgint(_s: StringInfo, _b: c_int) -> c_int {
    unimplemented!() // TODO(pg-port): src/backend/libpq/pqformat.c
}

unsafe fn pq_getmsgbyte(_s: StringInfo) -> c_int {
    unimplemented!() // TODO(pg-port): src/backend/libpq/pqformat.c
}

unsafe fn pq_getmsgbytes(_s: StringInfo, _datalen: c_int) -> *const c_char {
    unimplemented!() // TODO(pg-port): src/backend/libpq/pqformat.c
}

unsafe fn pq_getmsgend(_s: StringInfo) {
    unimplemented!() // TODO(pg-port): src/backend/libpq/pqformat.c
}

unsafe fn pq_putemptymessage(_msgtype: c_int) {
    unimplemented!() // TODO(pg-port): src/backend/libpq/pqformat.c
}

unsafe fn pq_beginmessage(_s: *mut StringInfoData, _msgtype: c_int) {
    unimplemented!() // TODO(pg-port): src/backend/libpq/pqformat.c
}

unsafe fn pq_beginmessage_reuse(_s: *mut StringInfoData, _msgtype: c_int) {
    unimplemented!() // TODO(pg-port): src/backend/libpq/pqformat.c
}

unsafe fn pq_sendint16(_s: *mut StringInfoData, _i: i16) {
    unimplemented!() // TODO(pg-port): src/backend/libpq/pqformat.c
}

unsafe fn pq_sendint32(_s: *mut StringInfoData, _i: i32) {
    unimplemented!() // TODO(pg-port): src/backend/libpq/pqformat.c
}

unsafe fn pq_sendbytes(_s: *mut StringInfoData, _data: *const c_char, _datalen: usize) {
    unimplemented!() // TODO(pg-port): src/backend/libpq/pqformat.c
}

unsafe fn pq_endmessage(_s: *mut StringInfoData) {
    unimplemented!() // TODO(pg-port): src/backend/libpq/pqformat.c
}

unsafe fn pq_endmessage_reuse(_s: *mut StringInfoData) {
    unimplemented!() // TODO(pg-port): src/backend/libpq/pqformat.c
}

unsafe fn pq_flush() {
    unimplemented!() // TODO(pg-port): src/backend/libpq/pqcomm.c
}

unsafe fn pq_comm_reset() {
    unimplemented!() // TODO(pg-port): src/backend/libpq/pqcomm.c
}

unsafe fn pq_check_connection() -> bool {
    unimplemented!() // TODO(pg-port): src/backend/libpq/pqcomm.c
}

unsafe fn pq_is_reading_msg() -> bool {
    unimplemented!() // TODO(pg-port): src/backend/libpq/pqcomm.c
}

unsafe fn CreateCachedPlan(
    _raw_parse_tree: *mut RawStmt,
    _query_string: *const c_char,
    _commandTag: CommandTag,
) -> *mut CachedPlanSource {
    unimplemented!() // TODO(pg-port): src/backend/utils/cache/plancache.c
}

unsafe fn CompleteCachedPlan(
    _plansource: *mut CachedPlanSource,
    _querytree_list: *mut List,
    _queryCacheContext: *mut c_void,
    _param_types: *mut Oid,
    _num_params: c_int,
    _parserSetup: ParserSetupHook,
    _parserSetupArg: *mut c_void,
    _cursor_options: c_int,
    _fixed_result: bool,
) {
    unimplemented!() // TODO(pg-port): src/backend/utils/cache/plancache.c
}

unsafe fn SaveCachedPlan(_plansource: *mut CachedPlanSource) {
    unimplemented!() // TODO(pg-port): src/backend/utils/cache/plancache.c
}

unsafe fn DropCachedPlan(_plansource: *mut CachedPlanSource) {
    unimplemented!() // TODO(pg-port): src/backend/utils/cache/plancache.c
}

unsafe fn StorePreparedStatement(
    _stmt_name: *const c_char,
    _plansource: *mut CachedPlanSource,
    _from_sql: bool,
) {
    unimplemented!() // TODO(pg-port): src/backend/commands/prepare.c
}

unsafe fn FetchPreparedStatement(_stmt_name: *const c_char, _throwError: bool) -> *mut PreparedStatement {
    unimplemented!() // TODO(pg-port): src/backend/commands/prepare.c
}

unsafe fn DropPreparedStatement(_stmt_name: *const c_char, _showError: bool) {
    unimplemented!() // TODO(pg-port): src/backend/commands/prepare.c
}

unsafe fn GetCachedPlan(
    _plansource: *mut CachedPlanSource,
    _boundParams: ParamListInfo,
    _snapshot: *mut c_void,
    _queryEnv: *mut QueryEnvironment,
) -> *mut CachedPlan {
    unimplemented!() // TODO(pg-port): src/backend/utils/cache/plancache.c
}

unsafe fn CachedPlanGetTargetList(
    _plansource: *mut CachedPlanSource,
    _queryEnv: *mut QueryEnvironment,
) -> *mut List {
    unimplemented!() // TODO(pg-port): src/backend/utils/cache/plancache.c
}

unsafe fn SendRowDescriptionMessage(
    _buf: *mut StringInfoData,
    _typeinfo: *mut c_void,
    _targetlist: *mut List,
    _formats: *mut i16,
) {
    unimplemented!() // TODO(pg-port): src/backend/access/common/printtup.c
}

unsafe fn FetchPortalTargetList(_portal: Portal) -> *mut List {
    unimplemented!() // TODO(pg-port): src/backend/tcop/pquery.c
}

unsafe fn makeParamList(_numParams: c_int) -> ParamListInfo {
    unimplemented!() // TODO(pg-port): src/backend/nodes/params.c
}

unsafe fn BuildParamLogString(
    _params: ParamListInfo,
    _knownTextValues: *mut *mut c_char,
    _maxlen: c_int,
) -> *mut c_char {
    unimplemented!() // TODO(pg-port): src/backend/nodes/params.c
}

unsafe fn ParamsErrorCallback(_arg: *mut c_void) {
    unimplemented!() // TODO(pg-port): src/backend/nodes/params.c
}

unsafe fn getTypeInputInfo(_type_: Oid, _typinput: *mut Oid, _typioparam: *mut Oid) {
    unimplemented!() // TODO(pg-port): src/backend/utils/lsyscache.c
}

unsafe fn getTypeBinaryInputInfo(_type_: Oid, _typreceive: *mut Oid, _typioparam: *mut Oid) {
    unimplemented!() // TODO(pg-port): src/backend/utils/lsyscache.c
}

unsafe fn OidInputFunctionCall(_functionId: Oid, _str: *mut c_char, _typioparam: Oid, _typmod: i32) -> Datum {
    unimplemented!() // TODO(pg-port): src/backend/utils/fmgr.c
}

unsafe fn OidReceiveFunctionCall(
    _functionId: Oid,
    _buf: *mut StringInfoData,
    _typioparam: Oid,
    _typmod: i32,
) -> Datum {
    unimplemented!() // TODO(pg-port): src/backend/utils/fmgr.c
}

unsafe fn pg_client_to_server(_s: *const c_char, _len: c_int) -> *mut c_char {
    unimplemented!() // TODO(pg-port): src/backend/mb/mbutils.c
}

unsafe fn HandleFunctionRequest(_input_message: StringInfo) {
    unimplemented!() // TODO(pg-port): src/backend/tcop/fastpath.c
}

unsafe fn exec_replication_command(_query_string: *const c_char) -> bool {
    unimplemented!() // TODO(pg-port): src/backend/replication/walsender.c
}

unsafe fn enable_timeout_after(_timeoutId: c_int, _delay_ms: i64) {
    unimplemented!() // TODO(pg-port): src/backend/utils/misc/timeout.c
}

unsafe fn disable_timeout(_timeoutId: c_int, _keepOffset: bool) {
    unimplemented!() // TODO(pg-port): src/backend/utils/misc/timeout.c
}

unsafe fn disable_all_timeouts(_keepOffset: bool) {
    unimplemented!() // TODO(pg-port): src/backend/utils/misc/timeout.c
}

unsafe fn get_timeout_active(_timeoutId: c_int) -> bool {
    unimplemented!() // TODO(pg-port): src/backend/utils/misc/timeout.c
}

unsafe fn get_timeout_indicator(_timeoutId: c_int, _reset_indicator: bool) -> bool {
    unimplemented!() // TODO(pg-port): src/backend/utils/misc/timeout.c
}

unsafe fn get_timeout_finish_time(_timeoutId: c_int) -> i64 {
    unimplemented!() // TODO(pg-port): src/backend/utils/misc/timeout.c
}

unsafe fn InitializeTimeouts() {
    unimplemented!() // TODO(pg-port): src/backend/utils/misc/timeout.c
}

unsafe fn GetCurrentStatementStartTimestamp() -> i64 {
    unimplemented!() // TODO(pg-port): src/backend/access/transam/xact.c
}

unsafe fn GetCurrentTimestamp() -> i64 {
    unimplemented!() // TODO(pg-port): src/backend/utils/adt/timestamp.c
}

unsafe fn TimestampDifference(_start_time: i64, _stop_time: i64, _secs: *mut i64, _microsecs: *mut c_int) {
    unimplemented!() // TODO(pg-port): src/backend/utils/adt/timestamp.c
}

unsafe fn TimestampDifferenceMicroseconds(_start_time: i64, _stop_time: i64) -> u64 {
    unimplemented!() // TODO(pg-port): src/backend/utils/adt/timestamp.c
}

unsafe fn SetCurrentStatementStartTimestamp() {
    unimplemented!() // TODO(pg-port): src/backend/access/transam/xact.c
}

unsafe fn LockErrorCleanup() {
    unimplemented!() // TODO(pg-port): src/backend/storage/lmgr/lock.c
}

unsafe fn GetAwaitedLock() -> *mut c_void {
    unimplemented!() // TODO(pg-port): src/backend/storage/lmgr/lock.c
}

unsafe fn HoldingBufferPinThatDelaysRecovery() -> bool {
    unimplemented!() // TODO(pg-port): src/backend/storage/buffer/bufmgr.c
}

unsafe fn GetStartupBufferPinWaitBufId() -> c_int {
    unimplemented!() // TODO(pg-port): src/backend/storage/buffer/bufmgr.c
}

unsafe fn CheckDeadLockAlert() {
    unimplemented!() // TODO(pg-port): src/backend/storage/lmgr/deadlock.c
}

unsafe fn ProcessCatchupInterrupt() {
    unimplemented!() // TODO(pg-port): src/backend/storage/ipc/sinval.c
}

unsafe fn ProcessNotifyInterrupt(_flush: bool) {
    unimplemented!() // TODO(pg-port): src/backend/commands/async.c
}

unsafe fn ProcessParallelMessages() {
    unimplemented!() // TODO(pg-port): src/backend/access/transam/parallel.c
}

unsafe fn ProcessParallelApplyMessages() {
    unimplemented!() // TODO(pg-port): src/backend/replication/logicalworker.c
}

unsafe fn ProcessProcSignalBarrier() {
    unimplemented!() // TODO(pg-port): src/backend/storage/ipc/procsignal.c
}

unsafe fn ProcessLogMemoryContextInterrupt() {
    unimplemented!() // TODO(pg-port): src/backend/utils/mmgr/mcxt.c
}

unsafe fn PortalErrorCleanup() {
    unimplemented!() // TODO(pg-port): src/backend/utils/mmgr/portalmem.c
}

unsafe fn ReplicationSlotRelease() {
    unimplemented!() // TODO(pg-port): src/backend/replication/slot.c
}

unsafe fn ReplicationSlotCleanup(_forWalSender: bool) {
    unimplemented!() // TODO(pg-port): src/backend/replication/slot.c
}

unsafe fn jit_reset_after_error() {
    unimplemented!() // TODO(pg-port): src/backend/jit/jit.c
}

unsafe fn WalSndSignals() {
    unimplemented!() // TODO(pg-port): src/backend/replication/walsender.c
}

unsafe fn WalSndErrorCleanup() {
    unimplemented!() // TODO(pg-port): src/backend/replication/walsender.c
}

unsafe fn InitWalSender() {
    unimplemented!() // TODO(pg-port): src/backend/replication/walsender.c
}

unsafe fn SetConfigOption(
    _name: *const c_char,
    _value: *const c_char,
    _context: GucContext,
    _source: GucSource,
) {
    unimplemented!() // TODO(pg-port): src/backend/utils/misc/guc.c
}

unsafe fn GetQuitSignalReason() -> c_int {
    unimplemented!() // TODO(pg-port): src/backend/postmaster/pmsignal.c
}

unsafe fn InitStandaloneProcess(_progname: *const c_char) {
    unimplemented!() // TODO(pg-port): src/backend/postmaster/postmaster.c
}

unsafe fn InitializeGUCOptions() {
    unimplemented!() // TODO(pg-port): src/backend/utils/misc/guc.c
}

unsafe fn SelectConfigFiles(_userDoption: *const c_char, _progname: *const c_char) -> bool {
    unimplemented!() // TODO(pg-port): src/backend/utils/misc/guc.c
}

unsafe fn checkDataDir() {
    unimplemented!() // TODO(pg-port): src/backend/utils/misc/miscadmin.c
}

unsafe fn ChangeToDataDir() {
    unimplemented!() // TODO(pg-port): src/backend/utils/misc/miscadmin.c
}

unsafe fn CreateDataDirLockFile(_amPostmaster: bool) {
    unimplemented!() // TODO(pg-port): src/backend/utils/misc/miscadmin.c
}

unsafe fn LocalProcessControlFile(_update_cksum: bool) {
    unimplemented!() // TODO(pg-port): src/backend/postmaster/postmaster.c
}

unsafe fn process_shared_preload_libraries() {
    unimplemented!() // TODO(pg-port): src/backend/utils/fmgr/dfmgr.c
}

unsafe fn InitializeMaxBackends() {
    unimplemented!() // TODO(pg-port): src/backend/postmaster/postmaster.c
}

unsafe fn InitPostmasterChildSlots() {
    unimplemented!() // TODO(pg-port): src/backend/postmaster/postmaster.c
}

unsafe fn InitializeFastPathLocks() {
    unimplemented!() // TODO(pg-port): src/backend/storage/lmgr/lock.c
}

unsafe fn process_shmem_requests() {
    unimplemented!() // TODO(pg-port): src/backend/postmaster/postmaster.c
}

unsafe fn InitializeShmemGUCs() {
    unimplemented!() // TODO(pg-port): src/backend/postmaster/postmaster.c
}

unsafe fn InitializeWalConsistencyChecking() {
    unimplemented!() // TODO(pg-port): src/backend/access/transam/xlog.c
}

unsafe fn CreateSharedMemoryAndSemaphores() {
    unimplemented!() // TODO(pg-port): src/backend/storage/ipc/ipci.c
}

unsafe fn set_max_safe_fds() {
    unimplemented!() // TODO(pg-port): src/backend/storage/file/fd.c
}

unsafe fn InitProcess() {
    unimplemented!() // TODO(pg-port): src/backend/storage/lmgr/proc.c
}

unsafe fn InitPostgres(
    _in_dbname: *const c_char,
    _dboid: Oid,
    _username: *const c_char,
    _useroid: Oid,
    _flags: c_int,
    _out_dbname: *mut *const c_char,
) {
    unimplemented!() // TODO(pg-port): src/backend/utils/init/postinit.c
}

unsafe fn BaseInit() {
    unimplemented!() // TODO(pg-port): src/backend/utils/init/globals.c
}

unsafe fn BeginReportingGUCOptions() {
    unimplemented!() // TODO(pg-port): src/backend/utils/misc/guc.c
}

unsafe fn ReportChangedGUCOptions() {
    unimplemented!() // TODO(pg-port): src/backend/utils/misc/guc.c
}

unsafe fn EventTriggerOnLogin() {
    unimplemented!() // TODO(pg-port): src/backend/commands/event_trigger.c
}

unsafe fn on_proc_exit(_function: unsafe extern "C" fn(c_int, Datum), _arg: Datum) {
    unimplemented!() // TODO(pg-port): src/backend/storage/ipc/ipc.c
}

unsafe fn proc_exit(_code: c_int) -> ! {
    unimplemented!() // TODO(pg-port): src/backend/storage/ipc/ipc.c
}

unsafe fn ProcessConfigFile(_context: GucContext) {
    unimplemented!() // TODO(pg-port): src/backend/utils/misc/guc.c
}

unsafe fn ParseLongOption(_string: *const c_char, _name: *mut *mut c_char, _value: *mut *mut c_char) {
    unimplemented!() // TODO(pg-port): src/backend/utils/misc/guc.c
}

unsafe fn SplitIdentifierString(
    _rawstring: *mut c_char,
    _separator: c_char,
    _namelist: *mut *mut List,
) -> bool {
    unimplemented!() // TODO(pg-port): src/backend/utils/adt/varlena.c
}

unsafe fn pg_strcasecmp(_s1: *const c_char, _s2: *const c_char) -> c_int {
    unimplemented!() // TODO(pg-port): src/port/pgstrcasecmp.c
}

unsafe fn guc_malloc(_elevel: c_int, _size: usize) -> *mut c_void {
    unimplemented!() // TODO(pg-port): src/backend/utils/misc/guc.c
}

unsafe fn WaitEventSetCanReportClosed() -> bool {
    unimplemented!() // TODO(pg-port): src/backend/storage/ipc/latch.c
}

unsafe fn pg_prng_double(_state: *mut c_void) -> f64 {
    unimplemented!() // TODO(pg-port): src/common/pg_prng.c
}

unsafe fn pg_strong_random(_buf: *mut c_void, _len: usize) -> bool {
    unimplemented!() // TODO(pg-port): src/common/pg_strong_random.c
}

unsafe fn INJECTION_POINT(_name: *const c_char, _arg: *mut c_void) {
    unimplemented!() // TODO(pg-port): src/backend/utils/misc/injection_point.c
}

unsafe fn parse_dispatch_option(_opt: *const c_char) -> c_int {
    unimplemented!() // TODO(pg-port): src/backend/main/main.c
}

unsafe fn SignalHandlerForConfigReload(_sig: c_int) {
    unimplemented!() // TODO(pg-port): src/backend/postmaster/interrupt.c
}

unsafe fn procsignal_sigusr1_handler(_sig: c_int) {
    unimplemented!() // TODO(pg-port): src/backend/storage/ipc/procsignal.c
}

unsafe fn AmAutoVacuumWorkerProcess() -> bool {
    unimplemented!() // TODO(pg-port): src/backend/utils/misc/miscadmin.c
}

unsafe fn IsLogicalWorker() -> bool {
    unimplemented!() // TODO(pg-port): src/backend/replication/logicalworker.c
}

unsafe fn IsLogicalLauncher() -> bool {
    unimplemented!() // TODO(pg-port): src/backend/replication/logicallauncher.c
}

unsafe fn AmWalReceiverProcess() -> bool {
    unimplemented!() // TODO(pg-port): src/backend/utils/misc/miscadmin.c
}

unsafe fn AmBackgroundWorkerProcess() -> bool {
    unimplemented!() // TODO(pg-port): src/backend/utils/misc/miscadmin.c
}

unsafe fn AmIoWorkerProcess() -> bool {
    unimplemented!() // TODO(pg-port): src/backend/utils/misc/miscadmin.c
}

unsafe fn IsExternalConnectionBackend(_backendType: c_int) -> bool {
    unimplemented!() // TODO(pg-port): src/backend/utils/misc/miscadmin.c
}

unsafe fn GetProcessingMode() -> c_int {
    unimplemented!() // TODO(pg-port): src/backend/utils/misc/miscadmin.c
}

unsafe fn SetProcessingMode(_mode: c_int) {
    unimplemented!() // TODO(pg-port): src/backend/utils/misc/miscadmin.c
}

unsafe fn strlcpy(_dst: *mut c_char, _src: *const c_char, _siz: usize) -> usize {
    unimplemented!() // TODO(pg-port): src/port/strlcpy.c
}

unsafe fn SetLatch(_latch: *mut c_void) {
    unimplemented!() // TODO(pg-port): src/backend/storage/ipc/latch.c
}

// ---------------------------------------------------------------------------
// Global variables
// ----------------
// ----------------

/// client-supplied query string
pub static mut debug_query_string: *const c_char = std::ptr::null();

// Note: whereToSendOutput is initialized for the bootstrap/standalone case
pub static mut whereToSendOutput: CommandDest = DestDebug;

/// flag for logging end of session
pub static mut Log_disconnections: bool = false;

pub static mut log_statement: LogStmtLevel = LOGSTMT_NONE;

/// wait N seconds to allow attach from a debugger
pub static mut PostAuthDelay: c_int = 0;

/// Time between checks that the client is still connected.
pub static mut client_connection_check_interval: c_int = 0;

/// flags for non-system relation kinds to restrict use
pub static mut restrict_nonsystem_relation_kind: c_int = 0;

// ----------------
// private variables
// ----------------

/*
 * Flag to keep track of whether we have started a transaction.
 * For extended query protocol this has to be remembered across messages.
 */
static mut xact_started: bool = false;

/*
 * Flag to indicate that we are doing the outer loop's read-from-client,
 * as opposed to any random read from client that might happen within
 * commands like COPY FROM STDIN.
 */
static mut DoingCommandRead: bool = false;

/*
 * Flags to implement skip-till-Sync-after-error behavior for messages of
 * the extended query protocol.
 */
static mut doing_extended_query_message: bool = false;
static mut ignore_till_sync: bool = false;

/*
 * If an unnamed prepared statement exists, it's stored here.
 * We keep it separate from the hashtable kept by commands/prepare.c
 * in order to reduce overhead for short-lived queries.
 */
static mut unnamed_stmt_psrc: *mut CachedPlanSource = std::ptr::null_mut();

/* assorted command-line switches */
static mut userDoption: *const c_char = std::ptr::null(); /* -D switch */
static mut EchoQuery: bool = false; /* -E switch */
static mut UseSemiNewlineNewline: bool = false; /* -j switch */

/* whether or not, and why, we were canceled by conflict with recovery */
static mut RecoveryConflictPending: bool = false;
static mut RecoveryConflictPendingReasons: [bool; 64] = [false; 64]; /* NUM_PROCSIGNALS */

/* reused buffer to pass to SendRowDescriptionMessage() */
static mut row_description_context: *mut c_void = std::ptr::null_mut();
static mut row_description_buf: StringInfoData = StringInfoData {
    data: std::ptr::null_mut(),
    len: 0,
    maxlen: 0,
    cursor: 0,
};

/* valgrind debugging (only if USE_VALGRIND) */
#[cfg(feature = "use_valgrind")]
static mut old_valgrind_error_count: c_uint = 0;

/*
 * If Valgrind detected any errors since old_valgrind_error_count was updated,
 * report the current query as the cause.  This should be called at the end
 * of message processing.
 */
#[inline]
#[allow(unused_variables)]
unsafe fn valgrind_report_error_query(_query: *const c_char) {
    // no-op unless compiled with USE_VALGRIND
}

// ----------------------------------------------------------------
//   routines to obtain user input
// ----------------------------------------------------------------

// ----------------
//  InteractiveBackend() is called for user interactive connections
//
//  the string entered by the user is placed in its parameter inBuf,
//  and we act like a Q message was received.
//
//  EOF is returned if end-of-file input is seen; time to shut down.
// ----------------

static InteractiveBackend_result: c_int = 0; // placeholder for PqMsg_Query

unsafe fn InteractiveBackend(inBuf: StringInfo) -> c_int {
    let mut c: c_int; // character read from getc()

    /*
     * display a prompt and obtain input from the user
     */
    libc::printf(b"backend> \0".as_ptr() as *const c_char);
    libc::fflush(stdout_ptr);

    resetStringInfo(inBuf);

    /*
     * Read characters until EOF or the appropriate delimiter is seen.
     */
    loop {
        c = interactive_getc();
        if c == libc::EOF {
            break;
        }
        if c == b'\n' as c_int {
            if UseSemiNewlineNewline {
                /*
                 * In -j mode, semicolon followed by two newlines ends the
                 * command; otherwise treat newline as regular character.
                 */
                if (*inBuf).len > 1
                    && *(*inBuf).data.offset(((*inBuf).len - 1) as isize) == b'\n' as c_char
                    && *(*inBuf).data.offset(((*inBuf).len - 2) as isize) == b';' as c_char
                {
                    /* might as well drop the second newline */
                    break;
                }
            } else {
                /*
                 * In plain mode, newline ends the command unless preceded by
                 * backslash.
                 */
                if (*inBuf).len > 0
                    && *(*inBuf).data.offset(((*inBuf).len - 1) as isize) == b'\\' as c_char
                {
                    /* discard backslash from inBuf */
                    (*inBuf).len -= 1;
                    *(*inBuf).data.offset((*inBuf).len as isize) = b'\0' as c_char;
                    /* discard newline too */
                    continue;
                } else {
                    /* keep the newline character, but end the command */
                    appendStringInfoChar(inBuf, b'\n' as c_char);
                    break;
                }
            }
        }

        /* Not newline, or newline treated as regular character */
        appendStringInfoChar(inBuf, c as c_char);
    }

    /* No input before EOF signal means time to quit. */
    if c == libc::EOF && (*inBuf).len == 0 {
        return libc::EOF;
    }

    /*
     * otherwise we have a user query so process it.
     */

    /* Add '\0' to make it look the same as message case. */
    appendStringInfoChar(inBuf, b'\0' as c_char);

    /*
     * if the query echo flag was given, print the query..
     */
    if EchoQuery {
        libc::printf(b"statement: %s\n\0".as_ptr() as *const c_char, (*inBuf).data);
    }
    libc::fflush(stdout_ptr);

    PqMsg_Query as c_int
}

/*
 * interactive_getc -- collect one character from stdin
 *
 * Even though we are not reading from a "client" process, we still want to
 * respond to signals, particularly SIGTERM/SIGQUIT.
 */
unsafe fn interactive_getc() -> c_int {
    let c: c_int;

    /*
     * This will not process catchup interrupts or notifications while
     * reading. But those can't really be relevant for a standalone backend
     * anyway. To properly handle SIGTERM there's a hack in die() that
     * directly processes interrupts at this stage...
     */
    CHECK_FOR_INTERRUPTS!();

    c = libc::fgetc(stdin_ptr);

    ProcessClientReadInterrupt(false);

    c
}

// ----------------
//  SocketBackend()   Is called for frontend-backend connections
//
//  Returns the message type code, and loads message body data into inBuf.
//
//  EOF is returned if the connection is lost.
// ----------------
unsafe fn SocketBackend(inBuf: StringInfo) -> c_int {
    let qtype: c_int;
    let maxmsglen: c_int;

    /*
     * Get message type code from the frontend.
     */
    HOLD_CANCEL_INTERRUPTS!();
    pq_startmsgread();
    qtype = pq_getbyte();

    if qtype == libc::EOF {
        /* frontend disconnected */
        if IsTransactionState() {
            ereport!(COMMERROR, errmsg!("unexpected EOF on client connection with an open transaction") /* C also: errcode(ERRCODE_CONNECTION_FAILURE) */);
        } else {
            /*
             * Can't send DEBUG log messages to client at this point. Since
             * we're disconnecting right away, we don't need to restore
             * whereToSendOutput.
             */
            whereToSendOutput = DestNone;
            ereport!(DEBUG1, errmsg!("unexpected EOF on client connection") /* C also: errcode(ERRCODE_CONNECTION_DOES_NOT_EXIST) */);
        }
        return qtype;
    }

    /*
     * Validate message type code before trying to read body; if we have lost
     * sync, better to say "command unknown" than to run out of memory because
     * we used garbage as a length word.  We can also select a type-dependent
     * limit on what a sane length word could be.  (The limit could be chosen
     * more granularly, but it's not clear it's worth fussing over.)
     *
     * This also gives us a place to set the doing_extended_query_message flag
     * as soon as possible.
     */
    match qtype as u8 as char {
        _ if qtype == PqMsg_Query as c_int => {
            maxmsglen = PQ_LARGE_MESSAGE_LIMIT;
            doing_extended_query_message = false;
        }
        _ if qtype == PqMsg_FunctionCall as c_int => {
            maxmsglen = PQ_LARGE_MESSAGE_LIMIT;
            doing_extended_query_message = false;
        }
        _ if qtype == PqMsg_Terminate as c_int => {
            maxmsglen = PQ_SMALL_MESSAGE_LIMIT;
            doing_extended_query_message = false;
            ignore_till_sync = false;
        }
        _ if qtype == PqMsg_Bind as c_int || qtype == PqMsg_Parse as c_int => {
            maxmsglen = PQ_LARGE_MESSAGE_LIMIT;
            doing_extended_query_message = true;
        }
        _ if qtype == PqMsg_Close as c_int
            || qtype == PqMsg_Describe as c_int
            || qtype == PqMsg_Execute as c_int
            || qtype == PqMsg_Flush as c_int =>
        {
            maxmsglen = PQ_SMALL_MESSAGE_LIMIT;
            doing_extended_query_message = true;
        }
        _ if qtype == PqMsg_Sync as c_int => {
            maxmsglen = PQ_SMALL_MESSAGE_LIMIT;
            /* stop any active skip-till-Sync */
            ignore_till_sync = false;
            /* mark not-extended, so that a new error doesn't begin skip */
            doing_extended_query_message = false;
        }
        _ if qtype == PqMsg_CopyData as c_int => {
            maxmsglen = PQ_LARGE_MESSAGE_LIMIT;
            doing_extended_query_message = false;
        }
        _ if qtype == PqMsg_CopyDone as c_int || qtype == PqMsg_CopyFail as c_int => {
            maxmsglen = PQ_SMALL_MESSAGE_LIMIT;
            doing_extended_query_message = false;
        }
        _ => {
            /*
             * Otherwise we got garbage from the frontend.  We treat this as
             * fatal because we have probably lost message boundary sync, and
             * there's no good way to recover.
             */
            ereport!(FATAL, errmsg!("invalid frontend message type {}", qtype) /* C also: errcode(ERRCODE_PROTOCOL_VIOLATION) */);
            maxmsglen = 0; /* keep compiler quiet */
        }
    }

    /*
     * In protocol version 3, all frontend messages have a length word next
     * after the type code; we can read the message contents independently of
     * the type.
     */
    if pq_getmessage(inBuf, maxmsglen) != 0 {
        return libc::EOF; /* suitable message already logged */
    }
    RESUME_CANCEL_INTERRUPTS!();

    qtype
}

// ----------------
//     ReadCommand reads a command from either the frontend or
//     standard input, places it in inBuf, and returns the
//     message type code (first byte of the message).
//     EOF is returned if end of file.
// ----------------
unsafe fn ReadCommand(inBuf: StringInfo) -> c_int {
    let result: c_int;

    if whereToSendOutput == DestRemote {
        result = SocketBackend(inBuf);
    } else {
        result = InteractiveBackend(inBuf);
    }
    result
}

/*
 * ProcessClientReadInterrupt() - Process interrupts specific to client reads
 *
 * This is called just before and after low-level reads.
 * 'blocked' is true if no data was available to read and we plan to retry,
 * false if about to read or done reading.
 *
 * Must preserve errno!
 */
pub unsafe fn ProcessClientReadInterrupt(blocked: bool) {
    let save_errno = *libc::__error();

    if DoingCommandRead {
        /* Check for general interrupts that arrived before/while reading */
        CHECK_FOR_INTERRUPTS!();

        /* Process sinval catchup interrupts, if any */
        if catchupInterruptPending != 0 {
            ProcessCatchupInterrupt();
        }

        /* Process notify interrupts, if any */
        if notifyInterruptPending {
            ProcessNotifyInterrupt(true);
        }
    } else if ProcDiePending {
        /*
         * We're dying.  If there is no data available to read, then it's safe
         * (and sane) to handle that now.  If we haven't tried to read yet,
         * make sure the process latch is set, so that if there is no data
         * then we'll come back here and die.  If we're done reading, also
         * make sure the process latch is set, as we might've undesirably
         * cleared it while reading.
         */
        if blocked {
            CHECK_FOR_INTERRUPTS!();
        } else {
            SetLatch(std::ptr::null_mut() /* MyLatch */);
        }
    }

    *libc::__error() = save_errno;
}

/*
 * ProcessClientWriteInterrupt() - Process interrupts specific to client writes
 *
 * This is called just before and after low-level writes.
 * 'blocked' is true if no data could be written and we plan to retry,
 * false if about to write or done writing.
 *
 * Must preserve errno!
 */
pub unsafe fn ProcessClientWriteInterrupt(blocked: bool) {
    let save_errno = *libc::__error();

    if ProcDiePending {
        /*
         * We're dying.  If it's not possible to write, then we should handle
         * that immediately, else a stuck client could indefinitely delay our
         * response to the signal.  If we haven't tried to write yet, make
         * sure the process latch is set, so that if the write would block
         * then we'll come back here and die.  If we're done writing, also
         * make sure the process latch is set, as we might've undesirably
         * cleared it while writing.
         */
        if blocked {
            /*
             * Don't mess with whereToSendOutput if ProcessInterrupts wouldn't
             * service ProcDiePending.
             */
            if InterruptHoldoffCount == 0 && CritSectionCount == 0 {
                /*
                 * We don't want to send the client the error message, as a)
                 * that would possibly block again, and b) it would likely
                 * lead to loss of protocol sync because we may have already
                 * sent a partial protocol message.
                 */
                if whereToSendOutput == DestRemote {
                    whereToSendOutput = DestNone;
                }

                CHECK_FOR_INTERRUPTS!();
            }
        } else {
            SetLatch(std::ptr::null_mut() /* MyLatch */);
        }
    }

    *libc::__error() = save_errno;
}

/*
 * Do raw parsing (only).
 *
 * A list of parsetrees (RawStmt nodes) is returned, since there might be
 * multiple commands in the given string.
 *
 * NOTE: for interactive queries, it is important to keep this routine
 * separate from the analysis & rewrite stages.  Analysis and rewriting
 * cannot be done in an aborted transaction, since they require access to
 * database tables.  So, we rely on the raw parser to determine whether
 * we've seen a COMMIT or ABORT command; when we are in abort state, other
 * commands are not processed any further than the raw parse stage.
 */
pub unsafe fn pg_parse_query(query_string: *const c_char) -> *mut List {
    let raw_parsetree_list: *mut List;

    TRACE_POSTGRESQL_QUERY_PARSE_START!(query_string);

    if log_parser_stats {
        ResetUsage();
    }

    raw_parsetree_list = raw_parser(query_string, RAW_PARSE_DEFAULT as c_int);

    if log_parser_stats {
        ShowUsage(b"PARSER STATISTICS\0".as_ptr() as *const c_char);
    }

    // DEBUG_NODE_TESTS_ENABLED block omitted (debug-only path)

    TRACE_POSTGRESQL_QUERY_PARSE_DONE!(query_string);

    raw_parsetree_list
}

/*
 * Given a raw parsetree (gram.y output), and optionally information about
 * types of parameter symbols ($n), perform parse analysis and rule rewriting.
 *
 * A list of Query nodes is returned, since either the analyzer or the
 * rewriter might expand one query to several.
 *
 * NOTE: for reasons mentioned above, this must be separate from raw parsing.
 */
pub unsafe fn pg_analyze_and_rewrite_fixedparams(
    parsetree: *mut RawStmt,
    query_string: *const c_char,
    paramTypes: *const Oid,
    numParams: c_int,
    queryEnv: *mut QueryEnvironment,
) -> *mut List {
    let query: *mut Query;
    let querytree_list: *mut List;

    TRACE_POSTGRESQL_QUERY_REWRITE_START!(query_string);

    /*
     * (1) Perform parse analysis.
     */
    if log_parser_stats {
        ResetUsage();
    }

    query = parse_analyze_fixedparams(parsetree, query_string, paramTypes, numParams, queryEnv);

    if log_parser_stats {
        ShowUsage(b"PARSE ANALYSIS STATISTICS\0".as_ptr() as *const c_char);
    }

    /*
     * (2) Rewrite the queries, as necessary
     */
    querytree_list = pg_rewrite_query(query);

    TRACE_POSTGRESQL_QUERY_REWRITE_DONE!(query_string);

    querytree_list
}

/*
 * Do parse analysis and rewriting.  This is the same as
 * pg_analyze_and_rewrite_fixedparams except that it's okay to deduce
 * information about $n symbol datatypes from context.
 */
pub unsafe fn pg_analyze_and_rewrite_varparams(
    parsetree: *mut RawStmt,
    query_string: *const c_char,
    paramTypes: *mut *mut Oid,
    numParams: *mut c_int,
    queryEnv: *mut QueryEnvironment,
) -> *mut List {
    let query: *mut Query;
    let querytree_list: *mut List;

    TRACE_POSTGRESQL_QUERY_REWRITE_START!(query_string);

    /*
     * (1) Perform parse analysis.
     */
    if log_parser_stats {
        ResetUsage();
    }

    query = parse_analyze_varparams(parsetree, query_string, paramTypes, numParams, queryEnv);

    /*
     * Check all parameter types got determined.
     */
    for i in 0..*numParams {
        let ptype: Oid = *(*paramTypes).offset(i as isize);

        if ptype == InvalidOid || ptype == UNKNOWNOID {
            ereport!(ERROR, errmsg!("could not determine data type of parameter ${}", i + 1) /* C also: errcode(ERRCODE_INDETERMINATE_DATATYPE) */);
        }
    }

    if log_parser_stats {
        ShowUsage(b"PARSE ANALYSIS STATISTICS\0".as_ptr() as *const c_char);
    }

    /*
     * (2) Rewrite the queries, as necessary
     */
    querytree_list = pg_rewrite_query(query);

    TRACE_POSTGRESQL_QUERY_REWRITE_DONE!(query_string);

    querytree_list
}

/*
 * Do parse analysis and rewriting.  This is the same as
 * pg_analyze_and_rewrite_fixedparams except that, instead of a fixed list of
 * parameter datatypes, a parser callback is supplied that can do
 * external-parameter resolution and possibly other things.
 */
pub unsafe fn pg_analyze_and_rewrite_withcb(
    parsetree: *mut RawStmt,
    query_string: *const c_char,
    parserSetup: ParserSetupHook,
    parserSetupArg: *mut c_void,
    queryEnv: *mut QueryEnvironment,
) -> *mut List {
    let query: *mut Query;
    let querytree_list: *mut List;

    TRACE_POSTGRESQL_QUERY_REWRITE_START!(query_string);

    /*
     * (1) Perform parse analysis.
     */
    if log_parser_stats {
        ResetUsage();
    }

    query = parse_analyze_withcb(parsetree, query_string, parserSetup, parserSetupArg, queryEnv);

    if log_parser_stats {
        ShowUsage(b"PARSE ANALYSIS STATISTICS\0".as_ptr() as *const c_char);
    }

    /*
     * (2) Rewrite the queries, as necessary
     */
    querytree_list = pg_rewrite_query(query);

    TRACE_POSTGRESQL_QUERY_REWRITE_DONE!(query_string);

    querytree_list
}

/*
 * Perform rewriting of a query produced by parse analysis.
 *
 * Note: query must just have come from the parser, because we do not do
 * AcquireRewriteLocks() on it.
 */
pub unsafe fn pg_rewrite_query(query: *mut Query) -> *mut List {
    let querytree_list: *mut List;

    if Debug_print_parse {
        elog_node_display(
            LOG,
            b"parse tree\0".as_ptr() as *const c_char,
            query as *mut c_void,
            Debug_pretty_print,
        );
    }

    if log_parser_stats {
        ResetUsage();
    }

    if (*(query as *mut QueryStub)).commandType == CMD_UTILITY {
        /* don't rewrite utilities, just dump 'em into result list */
        querytree_list = list_make1!(query);
    } else {
        /* rewrite regular queries */
        querytree_list = QueryRewrite(query);
    }

    if log_parser_stats {
        ShowUsage(b"REWRITER STATISTICS\0".as_ptr() as *const c_char);
    }

    // DEBUG_NODE_TESTS_ENABLED block omitted (debug-only path)

    if Debug_print_rewritten {
        elog_node_display(
            LOG,
            b"rewritten parse tree\0".as_ptr() as *const c_char,
            querytree_list as *mut c_void,
            Debug_pretty_print,
        );
    }

    querytree_list
}

// ---------------------------------------------------------------------------
// Additional stubs not yet defined
// ---------------------------------------------------------------------------

unsafe fn palloc(_size: usize) -> *mut c_void {
    unimplemented!() // TODO(pg-port): src/include/utils/palloc.h
}

unsafe fn pfree(_pointer: *mut c_void) {
    unimplemented!() // TODO(pg-port): src/include/utils/palloc.h
}

unsafe fn pstrdup(_s: *const c_char) -> *mut c_char {
    unimplemented!() // TODO(pg-port): src/include/utils/palloc.h
}

unsafe fn pnstrdup(_s: *const c_char, _len: c_int) -> *mut c_char {
    unimplemented!() // TODO(pg-port): src/include/utils/palloc.h
}

unsafe fn palloc_array(_size: usize, _count: usize) -> *mut c_void {
    unimplemented!() // TODO(pg-port): src/include/utils/palloc.h
}

unsafe fn palloc0_array(_size: usize, _count: usize) -> *mut c_void {
    unimplemented!() // TODO(pg-port): src/include/utils/palloc.h
}

unsafe fn lappend(_list: *mut List, _datum: *mut c_void) -> *mut List {
    unimplemented!() // TODO(pg-port): src/backend/lib/list.c
}

unsafe fn list_free(_list: *mut List) {
    unimplemented!() // TODO(pg-port): src/backend/lib/list.c
}

unsafe fn errmsg_internal(_fmt: *const c_char) -> c_int {
    unimplemented!() // TODO(pg-port)
}

unsafe fn errdetail_internal(_fmt: *const c_char) -> c_int {
    unimplemented!() // TODO(pg-port)
}

unsafe fn errcontext(_fmt: *const c_char) {
    unimplemented!() // TODO(pg-port)
}

unsafe fn errhidestmt(_b: bool) -> c_int {
    unimplemented!() // TODO(pg-port)
}

unsafe fn errhint(_fmt: *const c_char) -> c_int {
    unimplemented!() // TODO(pg-port)
}

unsafe fn pqsignal(_signo: c_int, _handler: *mut c_void) {
    unimplemented!() // TODO(pg-port)
}

unsafe fn sigaddset(_set: *mut c_void, _signo: c_int) {
    unimplemented!() // TODO(pg-port)
}

unsafe fn sigprocmask(_how: c_int, _set: *mut c_void, _oset: *mut c_void) {
    unimplemented!() // TODO(pg-port)
}

unsafe fn getrusage(_who: c_int, _rusage: *mut c_void) {
    unimplemented!() // TODO(pg-port)
}

unsafe fn gettimeofday(_tv: *mut c_void, _tz: *mut c_void) {
    unimplemented!() // TODO(pg-port)
}

unsafe fn pg_strcasecmp_dup(_s1: *const c_char, _s2: *const c_char) -> c_int {
    unimplemented!() // TODO(pg-port) -- pg_strcasecmp already defined above
}

unsafe fn memcpy_stub(_dst: *mut c_void, _src: *const c_void, _n: usize) {
    unimplemented!() // TODO(pg-port) -- libc memcpy
}

unsafe fn snprintf_stub(
    _str: *mut c_char,
    _size: usize,
    _fmt: *const c_char,
    _a: i64,
    _b: c_int,
    _c: c_int,
) {
    unimplemented!() // TODO(pg-port): libc snprintf
}

// Global statics not yet defined

pub static mut MyXactFlags: u32 = 0;

pub static mut MyProc: *mut c_void = std::ptr::null_mut();
pub static mut MyProcPort: *mut Port = std::ptr::null_mut();
pub static mut MyDatabaseId: Oid = 0;
pub static mut MyCancelKey: [u8; 32] = [0u8; 32];
pub static mut MyCancelKeyLength: c_int = 0;
pub static mut MyBgworkerEntry: *mut c_void = std::ptr::null_mut();
pub static mut MyBackendType: c_int = 0;
pub static mut MyStartTimestamp: i64 = 0;

pub static mut PostmasterContext: *mut c_void = std::ptr::null_mut();
pub static mut MessageContext: *mut c_void = std::ptr::null_mut();
pub static mut TopMemoryContext: *mut c_void = std::ptr::null_mut();

pub static mut PgStartTime: i64 = 0;

pub static mut am_walsender: bool = false;

pub static mut IsBinaryUpgrade: bool = false;
pub static mut FrontendProtocol: ProtocolVersion = 0;
pub static mut OutputFileName: [c_char; 1024] = [0i8; 1024];

pub static mut InterruptPending: bool = false;
pub static mut ProcDiePending: bool = false;
pub static mut QueryCancelPending: bool = false;
pub static mut QueryCancelHoldoffCount: u32 = 0;
pub static mut InterruptHoldoffCount: u32 = 0;
pub static mut CritSectionCount: u32 = 0;
pub static mut CheckClientConnectionPending: bool = false;
pub static mut ClientConnectionLost: bool = false;
pub static mut IdleInTransactionSessionTimeoutPending: bool = false;
pub static mut TransactionTimeoutPending: bool = false;
pub static mut IdleSessionTimeoutPending: bool = false;
pub static mut IdleStatsUpdateTimeoutPending: bool = false;
pub static mut ProcSignalBarrierPending: bool = false;
pub static mut ParallelMessagePending: bool = false;
pub static mut LogMemoryContextPending: bool = false;
pub static mut ParallelApplyMessagePending: bool = false;
pub static mut notifyInterruptPending: bool = false;
pub static mut ConfigReloadPending: bool = false;

pub static mut pgStatSessionEndCause: c_int = 0;

// GUC statics defined in guc_tables.c; referenced here for stats/debug logging.
// TODO(pg-port): unify with crate::utils::misc::guc_tables once those are pub.
pub static mut log_parser_stats: bool = false;
pub static mut log_planner_stats: bool = false;
pub static mut log_executor_stats: bool = false;
pub static mut log_statement_stats: bool = false;
pub static mut Debug_print_parse: bool = false;
pub static mut Debug_print_rewritten: bool = false;
pub static mut Debug_print_plan: bool = false;
pub static mut Debug_pretty_print: bool = false;

pub static mut log_duration: bool = false;
pub static mut log_min_duration_statement: i64 = -1;
pub static mut log_min_duration_sample: i64 = -1;
pub static mut log_statement_sample_rate: f64 = 1.0;
pub static mut log_parameter_max_length: c_int = -1;
pub static mut log_parameter_max_length_on_error: c_int = 0;
pub static mut xact_is_sampled: bool = false;
pub static mut StatementTimeout: i64 = 0;
pub static mut TransactionTimeout: i64 = 0;
pub static mut IdleInTransactionSessionTimeout: i64 = 0;
pub static mut IdleSessionTimeout: i64 = 0;

pub static mut IsUnderPostmaster: bool = false;
pub static mut ClientAuthInProgress: bool = false;

pub static mut pg_global_prng_state: PgPrngState = PgPrngState { s0: 0, s1: 0 };

/* C: pg_prng_state { uint64 s0; uint64 s1; } -- see src/include/common/pg_prng.h */
#[repr(C)]
pub struct PgPrngState {
    pub s0: u64,
    pub s1: u64,
}

pub static mut error_context_stack: *mut ErrorContextCallback = std::ptr::null_mut();

pub static mut log_connections: u32 = 0;

#[repr(C)]
pub struct ConnTimingData {
    pub socket_create: i64,
    pub fork_start: i64,
    pub fork_end: i64,
    pub auth_start: i64,
    pub auth_end: i64,
    pub ready_for_use: i64,
}

pub static mut conn_timing: ConnTimingData = ConnTimingData {
    socket_create: 0,
    fork_start: 0,
    fork_end: 0,
    auth_start: 0,
    auth_end: 0,
    ready_for_use: i64::MIN,
};

// Constants
pub const XACT_FLAGS_NEEDIMMEDIATECOMMIT: u32 = 0x0002;
pub const XACT_FLAGS_PIPELINING: u32 = 0x0004;
pub const PARAM_FLAG_CONST: u32 = 0x0001;
pub const CURSOR_OPT_PARALLEL_OK: c_int = 0x0010;
pub const CURSOR_OPT_BINARY: c_int = 0x0002;
pub const CMD_UTILITY: c_int = 5;
pub const TRANS_STMT_COMMIT: c_int = 0;
pub const TRANS_STMT_PREPARE: c_int = 1;
pub const TRANS_STMT_ROLLBACK: c_int = 2;
pub const TRANS_STMT_ROLLBACK_TO: c_int = 3;
pub const FETCH_ALL: i64 = i64::MAX;
pub const CMDTAG_UNKNOWN: CommandTag = 0;
pub const InvalidOid: Oid = 0;
pub const InvalidSnapshot: *mut c_void = std::ptr::null_mut() as *mut c_void;
pub const MAX_CANCEL_KEY_LENGTH: usize = 32;
pub const MAXPGPATH: usize = 1024;
pub const DISPATCH_POSTMASTER: c_int = 0;
pub const INIT_PG_LOAD_SESSION_LIBS: c_int = 0x0001;
pub const LOG_CONNECTION_SETUP_DURATIONS: u32 = 0x0001;
pub const SECS_PER_HOUR: i64 = 3600;
pub const SECS_PER_MINUTE: i64 = 60;
pub const NS_PER_US: u64 = 1000;
pub const DISCONNECT_CLIENT_EOF: c_int = 1;
pub const DISCONNECT_KILLED: c_int = 2;

// Log levels (duplicating from existing but needed in new fns)
pub const LOG: c_int = 15;
pub const DEBUG1: c_int = 10;
pub const DEBUG2: c_int = 9;
pub const WARNING: c_int = 14;
pub const WARNING_CLIENT_ONLY: c_int = 14;
pub const FATAL: c_int = 16;
pub const ERROR: c_int = 17;

// Timeout IDs
pub const STATEMENT_TIMEOUT: c_int = 0;
pub const LOCK_TIMEOUT: c_int = 1;
pub const IDLE_IN_TRANSACTION_SESSION_TIMEOUT: c_int = 2;
pub const IDLE_SESSION_TIMEOUT: c_int = 3;
pub const TRANSACTION_TIMEOUT: c_int = 4;
pub const CLIENT_CONNECTION_CHECK_TIMEOUT: c_int = 5;
pub const IDLE_STATS_UPDATE_TIMEOUT: c_int = 6;

// Process signal reasons
pub const PROCSIG_RECOVERY_CONFLICT_FIRST: ProcSignalReason = 10;
pub const PROCSIG_RECOVERY_CONFLICT_LAST: ProcSignalReason = 16;
pub const PROCSIG_RECOVERY_CONFLICT_BUFFERPIN: ProcSignalReason = 10;
pub const PROCSIG_RECOVERY_CONFLICT_LOCK: ProcSignalReason = 11;
pub const PROCSIG_RECOVERY_CONFLICT_TABLESPACE: ProcSignalReason = 12;
pub const PROCSIG_RECOVERY_CONFLICT_SNAPSHOT: ProcSignalReason = 13;
pub const PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT: ProcSignalReason = 14;
pub const PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK: ProcSignalReason = 15;
pub const PROCSIG_RECOVERY_CONFLICT_DATABASE: ProcSignalReason = 16;

// GUC context
pub const PGC_POSTMASTER: GucContext = 0;
pub const PGC_BACKEND: GucContext = 2;
pub const PGC_SU_BACKEND: GucContext = 3;
pub const PGC_S_ARGV: GucSource = 3;
pub const PGC_S_CLIENT: GucSource = 4;
pub const PGC_SIGHUP: GucContext = 1;

// Wire protocol message types (src/include/libpq/protocol.h)
pub const PqMsg_Bind: c_int = b'B' as c_int;
pub const PqMsg_Close: c_int = b'C' as c_int;
pub const PqMsg_Describe: c_int = b'D' as c_int;
pub const PqMsg_Execute: c_int = b'E' as c_int;
pub const PqMsg_FunctionCall: c_int = b'F' as c_int;
pub const PqMsg_Flush: c_int = b'H' as c_int;
pub const PqMsg_Parse: c_int = b'P' as c_int;
pub const PqMsg_Query: c_int = b'Q' as c_int;
pub const PqMsg_Sync: c_int = b'S' as c_int;
pub const PqMsg_Terminate: c_int = b'X' as c_int;
pub const PqMsg_CopyFail: c_int = b'f' as c_int;
pub const PqMsg_CopyDone: c_int = b'c' as c_int;
pub const PqMsg_CopyData: c_int = b'd' as c_int;
pub const PqMsg_ParameterDescription: c_int = b't' as c_int;
pub const PqMsg_NoData: c_int = b'n' as c_int;
pub const PqMsg_BindComplete: c_int = b'2' as c_int;
pub const PqMsg_ParseComplete: c_int = b'1' as c_int;
pub const PqMsg_CloseComplete: c_int = b'3' as c_int;
pub const PqMsg_BackendKeyData: c_int = b'K' as c_int;
pub const PqMsg_PortalSuspended: c_int = b's' as c_int;

// Activity states
pub const STATE_RUNNING: c_int = 2;
pub const STATE_IDLEINTRANSACTION: c_int = 3;
pub const STATE_IDLEINTRANSACTION_ABORTED: c_int = 4;
pub const STATE_IDLE: c_int = 1;
pub const STATE_FASTPATH: c_int = 5;

// RUSAGE_SELF
pub const RUSAGE_SELF: c_int = 0;

// Signal numbers (representative)
pub const SIGHUP: c_int = 1;
pub const SIGQUIT: c_int = 3;
pub const SIGTERM: c_int = 15;
pub const SIGINT: c_int = 2;
pub const SIGPIPE: c_int = 13;
pub const SIGUSR1: c_int = 10;
pub const SIGUSR2: c_int = 12;
pub const SIGFPE: c_int = 8;
pub const SIGCHLD: c_int = 20;
pub const SIGALRM: c_int = 14;

/* C: sigset_t BlockSig, UnBlockSig -- opaque OS signal masks (see pqsignal.h) */
pub static mut BlockSig: SigSet = SigSet { __val: [0; 16] };
pub static mut UnBlockSig: SigSet = SigSet { __val: [0; 16] };

#[repr(C)]
pub struct SigSet {
    pub __val: [u64; 16],
}
pub const SIG_IGN: usize = 1;
pub const SIG_DFL: usize = 0;

// RESTRICT_RELKIND flags
pub const RESTRICT_RELKIND_VIEW: c_int = 0x01;
pub const RESTRICT_RELKIND_FOREIGN_TABLE: c_int = 0x02;

// getopt globals
static mut optarg: *mut c_char = std::ptr::null_mut();
static mut optind: c_int = 1;
static mut opterr: c_int = 1;
static mut optreset: c_int = 0;

unsafe fn getopt(
    _argc: c_int,
    _argv: *mut *mut c_char,
    _optstring: *const c_char,
) -> c_int {
    unimplemented!() // TODO(pg-port): libc getopt
}

unsafe fn GUC_check_errdetail(_fmt: *const c_char) {
    unimplemented!() // TODO(pg-port)
}

unsafe fn guc_malloc_stub(_elevel: c_int, _size: usize) -> *mut c_void {
    unimplemented!() // TODO(pg-port) -- guc_malloc already defined above
}


// ---------------------------------------------------------------------------
// pg_plan_query
//
// Generate a plan for a single already-rewritten query.
// ---------------------------------------------------------------------------
pub unsafe fn pg_plan_query(
    querytree: *mut Query,
    query_string: *const c_char,
    cursorOptions: c_int,
    boundParams: ParamListInfo,
) -> *mut PlannedStmt {
    let plan: *mut PlannedStmt;

    /* Utility commands have no plans. */
    if (*(querytree as *mut QueryStub)).commandType == CMD_UTILITY {
        return std::ptr::null_mut();
    }

    /* Planner must have a snapshot in case it calls user-defined functions. */
    Assert!(ActiveSnapshotSet());

    TRACE_POSTGRESQL_QUERY_PLAN_START!();

    if log_planner_stats {
        ResetUsage();
    }

    /* call the optimizer */
    plan = planner(querytree, query_string, cursorOptions, boundParams);

    if log_planner_stats {
        ShowUsage(b"PLANNER STATISTICS\0".as_ptr() as *const c_char);
    }

    // DEBUG_NODE_TESTS_ENABLED block omitted (debug-only path)

    /*
     * Print plan if debugging.
     */
    if Debug_print_plan {
        elog_node_display(
            LOG,
            b"plan\0".as_ptr() as *const c_char,
            plan as *mut c_void,
            Debug_pretty_print,
        );
    }

    TRACE_POSTGRESQL_QUERY_PLAN_DONE!();

    plan
}

// ---------------------------------------------------------------------------
// pg_plan_queries
//
// Generate plans for a list of already-rewritten queries.
// ---------------------------------------------------------------------------
pub unsafe fn pg_plan_queries(
    querytrees: *mut List,
    query_string: *const c_char,
    cursorOptions: c_int,
    boundParams: ParamListInfo,
) -> *mut List {
    let mut stmt_list: *mut List = NIL;
    let query_list: *mut crate::nodes::pg_list::ListCell = std::ptr::null_mut();

    foreach!(query_list, querytrees, {
        let query: *mut Query = lfirst_node!(Query, T_Query, crate::current_cell!(query_list));
        let stmt: *mut PlannedStmt;

        if (*(query as *mut QueryStub)).commandType == CMD_UTILITY {
            /* Utility commands require no planning. */
            stmt = makeNode!(PlannedStmtStub, T_PlannedStmt) as *mut PlannedStmt;
            let stmt_s = stmt as *mut PlannedStmtStub;
            let query_s = query as *mut QueryStub;
            (*stmt_s).commandType = CMD_UTILITY;
            (*stmt_s).canSetTag = (*query_s).canSetTag;
            (*stmt_s).utilityStmt = (*query_s).utilityStmt;
            (*stmt_s).stmt_location = (*query_s).stmt_location;
            (*stmt_s).stmt_len = (*query_s).stmt_len;
            (*stmt_s).queryId = (*query_s).queryId;
        } else {
            stmt = pg_plan_query(query, query_string, cursorOptions, boundParams);
        }

        stmt_list = lappend(stmt_list, stmt as *mut c_void);
    });

    stmt_list
}


// ---------------------------------------------------------------------------
// exec_simple_query
//
// Execute a "simple Query" protocol message.
// ---------------------------------------------------------------------------
unsafe fn exec_simple_query(query_string: *const c_char) {
    let dest: CommandDest = whereToSendOutput;
    let mut oldcontext: *mut c_void;
    let parsetree_list: *mut List;
    let parsetree_item: *mut crate::nodes::pg_list::ListCell;
    let save_log_statement_stats: bool = log_statement_stats;
    let mut was_logged: bool = false;
    let use_implicit_block: bool;
    let mut msec_str: [c_char; 32] = [0; 32];

    /*
     * Report query to various monitoring facilities.
     */
    debug_query_string = query_string;

    pgstat_report_activity(STATE_RUNNING, query_string);

    TRACE_POSTGRESQL_QUERY_START!(query_string);

    /*
     * We use save_log_statement_stats so ShowUsage doesn't report incorrect
     * results because ResetUsage wasn't called.
     */
    if save_log_statement_stats {
        ResetUsage();
    }

    /*
     * Start up a transaction command.  All queries generated by the
     * query_string will be in this same command block, *unless* we find a
     * BEGIN/COMMIT/ABORT statement; we have to force a new xact command after
     * one of those, else bad things will happen in xact.c.
     */
    start_xact_command();

    /*
     * Zap any pre-existing unnamed statement.
     */
    drop_unnamed_stmt();

    /*
     * Switch to appropriate context for constructing parsetrees.
     */
    oldcontext = MemoryContextSwitchTo(MessageContext);

    /*
     * Do basic parsing of the query or queries (this should be safe even if
     * we are in aborted transaction state!)
     */
    parsetree_list = pg_parse_query(query_string);

    /* Log immediately if dictated by log_statement */
    if check_log_statement(parsetree_list) {
        ereport!(LOG, errmsg!("statement: {}", CStr::from_ptr(query_string).to_string_lossy()));
        /* C also: errhidestmt(true), errdetail_execute(parsetree_list) */
        was_logged = true;
    }

    /*
     * Switch back to transaction context to enter the loop.
     */
    MemoryContextSwitchTo(oldcontext);

    /*
     * For historical reasons, if multiple SQL statements are given in a
     * single "simple Query" message, we execute them as a single transaction,
     * unless explicit transaction control commands are included.
     */
    use_implicit_block = list_length(parsetree_list) > 1;

    /*
     * Run through the raw parsetree(s) and process each one.
     */
    parsetree_item = std::ptr::null_mut();
    foreach!(parsetree_item, parsetree_list, {
        let parsetree: *mut RawStmt =
            lfirst_node!(RawStmt, T_RawStmt, crate::current_cell!(parsetree_item));
        let mut snapshot_set: bool = false;
        let commandTag: CommandTag;
        let mut qc: QueryCompletion = std::mem::zeroed();
        let per_parsetree_context: *mut c_void;
        let querytree_list: *mut List;
        let plantree_list: *mut List;
        let portal: Portal;
        let receiver: *mut DestReceiver;
        let mut format: i16;
        let cmdtagname: *const c_char;
        let mut cmdtaglen: usize = 0;

        pgstat_report_query_id(0, true);
        pgstat_report_plan_id(0, true);

        /*
         * Get the command name for use in status display.
         */
        commandTag = CreateCommandTag((*(parsetree as *mut RawStmtStub)).stmt as *mut Node);
        cmdtagname = GetCommandTagNameAndLen(commandTag, &mut cmdtaglen);

        set_ps_display_with_len(cmdtagname, cmdtaglen);

        BeginCommand(commandTag, dest);

        /*
         * If we are in an aborted transaction, reject all commands except
         * COMMIT/ABORT.
         */
        if IsAbortedTransactionBlockState()
            && !IsTransactionExitStmt((*(parsetree as *mut RawStmtStub)).stmt as *mut Node)
        {
            ereport!(ERROR, errmsg!("current transaction is aborted, commands ignored until end of transaction block"));
            /* C also: errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION), errdetail_abort() */
        }

        /* Make sure we are in a transaction command */
        start_xact_command();

        /*
         * If using an implicit transaction block, start it.
         */
        if use_implicit_block {
            BeginImplicitTransactionBlock();
        }

        /* If we got a cancel signal in parsing or prior command, quit */
        if InterruptPending {
            ProcessInterrupts();
        }

        /*
         * Set up a snapshot if parse analysis/planning will need one.
         */
        if analyze_requires_snapshot(parsetree) {
            PushActiveSnapshot(GetTransactionSnapshot());
            snapshot_set = true;
        }

        /*
         * OK to analyze, rewrite, and plan this query.
         *
         * If we have multiple parsetrees, use a separate context for each one.
         * For the last parsetree, use MessageContext.
         */
        if lnext(parsetree_list, crate::current_cell!(parsetree_item)) != std::ptr::null_mut() {
            per_parsetree_context = AllocSetContextCreate(
                MessageContext,
                b"per-parsetree message context\0".as_ptr() as *const c_char,
                0, /* ALLOCSET_DEFAULT_MINSIZE */
                8 * 1024, /* ALLOCSET_DEFAULT_INITSIZE */
                8 * 1024 * 1024 /* ALLOCSET_DEFAULT_MAXSIZE */
            );
            oldcontext = MemoryContextSwitchTo(per_parsetree_context);
        } else {
            per_parsetree_context = std::ptr::null_mut();
            oldcontext = MemoryContextSwitchTo(MessageContext);
        }

        querytree_list = pg_analyze_and_rewrite_fixedparams(
            parsetree,
            query_string,
            std::ptr::null(),
            0,
            std::ptr::null_mut(),
        );

        plantree_list = pg_plan_queries(
            querytree_list,
            query_string,
            CURSOR_OPT_PARALLEL_OK,
            std::ptr::null_mut(),
        );

        /*
         * Done with the snapshot used for parsing/planning.
         */
        if snapshot_set {
            PopActiveSnapshot();
        }

        /* If we got a cancel signal in analysis or planning, quit */
        if InterruptPending {
            ProcessInterrupts();
        }

        /*
         * Create unnamed portal to run the query or queries in.
         */
        portal = CreatePortal(b"\0".as_ptr() as *const c_char, true, true);
        /* Don't display the portal in pg_cursors */
        (*(portal as *mut PortalStub)).visible = false;

        PortalDefineQuery(
            portal,
            std::ptr::null(),
            query_string,
            commandTag,
            plantree_list,
            std::ptr::null_mut(),
        );

        /*
         * Start the portal.  No parameters here.
         */
        PortalStart(portal, std::ptr::null_mut(), 0, std::ptr::null_mut());

        /*
         * Select the appropriate output format: text unless FETCH from binary cursor.
         */
        format = 0; /* TEXT is default */
        if IsA!((*(parsetree as *mut RawStmtStub)).stmt as *mut Node, T_FetchStmt) {
            let stmt: *mut FetchStmt = (*(parsetree as *mut RawStmtStub)).stmt as *mut FetchStmt;

            if !(*stmt).ismove {
                let fportal: Portal =
                    GetPortalByName((*stmt).portalname as *const c_char);

                if PortalIsValid(fportal)
                    && ((*(fportal as *mut PortalStub)).cursorOptions & CURSOR_OPT_BINARY) != 0
                {
                    format = 1; /* BINARY */
                }
            }
        }
        PortalSetResultFormat(portal, 1, &mut format);

        /*
         * Now we can create the destination receiver object.
         */
        receiver = CreateDestReceiver(dest);
        if dest == DestRemote {
            SetRemoteDestReceiverParams(receiver, portal);
        }

        /*
         * Switch back to transaction context for execution.
         */
        MemoryContextSwitchTo(oldcontext);

        /*
         * Run the portal to completion, and then drop it (and the receiver).
         */
        let _completed = PortalRun(
            portal,
            FETCH_ALL,
            true, /* always top level */
            receiver,
            receiver,
            &mut qc,
        );

        (*(receiver as *mut DestReceiverStub)).rDestroy.map(|f| f(receiver));

        PortalDrop(portal, false);

        if lnext(parsetree_list, crate::current_cell!(parsetree_item)) == std::ptr::null_mut() {
            /*
             * Last parsetree: close down transaction statement before
             * reporting command-complete.
             */
            if use_implicit_block {
                EndImplicitTransactionBlock();
            }
            finish_xact_command();
        } else if IsA!((*(parsetree as *mut RawStmtStub)).stmt as *mut Node, T_TransactionStmt) {
            /*
             * If this was a transaction control statement, commit it.
             */
            finish_xact_command();
        } else {
            /*
             * We had better not see XACT_FLAGS_NEEDIMMEDIATECOMMIT set.
             */
            Assert!((MyXactFlags & XACT_FLAGS_NEEDIMMEDIATECOMMIT) == 0);

            /*
             * We need a CommandCounterIncrement after every query, except
             * those that start or end a transaction block.
             */
            CommandCounterIncrement();

            /*
             * Disable statement timeout between queries.
             */
            disable_statement_timeout();
        }

        /*
         * Tell client that we're done with this query.
         */
        EndCommand(&qc, dest, false);

        /* Now we may drop the per-parsetree context, if one was created. */
        if !per_parsetree_context.is_null() {
            MemoryContextDelete(per_parsetree_context);
        }
    }); /* end loop over parsetrees */

    /*
     * Close down transaction statement, if one is open.
     */
    finish_xact_command();

    /*
     * If there were no parsetrees, return EmptyQueryResponse message.
     */
    if parsetree_list.is_null() {
        NullCommand(dest);
    }

    /*
     * Emit duration logging if appropriate.
     */
    match check_log_duration(msec_str.as_mut_ptr(), was_logged) {
        1 => {
            ereport!(LOG, errmsg!("duration: {} ms", CStr::from_ptr(msec_str.as_ptr()).to_string_lossy()));
            /* C also: errhidestmt(true) */
        }
        2 => {
            ereport!(LOG, errmsg!("duration: {} ms  statement: {}", CStr::from_ptr(msec_str.as_ptr()).to_string_lossy(), CStr::from_ptr(query_string).to_string_lossy()));
            /* C also: errhidestmt(true), errdetail_execute(parsetree_list) */
        }
        _ => {}
    }

    if save_log_statement_stats {
        ShowUsage(b"QUERY STATISTICS\0".as_ptr() as *const c_char);
    }

    TRACE_POSTGRESQL_QUERY_DONE!(query_string);

    debug_query_string = std::ptr::null();
}

// Stub inner structs for portal/receiver/fetchstmt field access
#[repr(C)]
struct PortalStub {
    pub name: *const c_char,
    pub visible: bool,
    pub cursorOptions: c_int,
    pub commandTag: CommandTag,
    pub stmts: *mut List,
    pub sourceText: *const c_char,
    pub prepStmtName: *const c_char,
    pub portalParams: ParamListInfo,
    pub atStart: bool,
    pub tupDesc: *mut c_void,
    pub formats: *mut i16,
    pub portalContext: *mut c_void,
}

#[repr(C)]
struct DestReceiverStub {
    pub rDestroy: Option<unsafe fn(*mut DestReceiver)>,
}

#[repr(C)]
struct FetchStmt {
    pub r#type: c_int,
    pub direction: c_int,
    pub howMany: i64,
    pub portalname: *mut c_char,
    pub ismove: bool,
}

#[repr(C)]
struct RawStmtStub {
    pub r#type: c_int,
    pub stmt: *mut c_void,
    pub stmt_location: c_int,
    pub stmt_len: c_int,
}


// Stub structs for Query/PlannedStmt/PreparedStatement field access
#[repr(C)]
struct QueryStub {
    pub r#type: c_int,
    pub commandType: c_int,
    pub canSetTag: bool,
    pub utilityStmt: *mut c_void,
    pub stmt_location: c_int,
    pub stmt_len: c_int,
    pub queryId: u64,
}

#[repr(C)]
struct PlannedStmtStub {
    pub r#type: c_int,
    pub commandType: c_int,
    pub canSetTag: bool,
    pub utilityStmt: *mut c_void,
    pub stmt_location: c_int,
    pub stmt_len: c_int,
    pub queryId: u64,
    pub planId: u64,
}

#[repr(C)]
struct PreparedStatementStub {
    pub plansource: *mut CachedPlanSource,
}

#[repr(C)]
struct CachedPlanSourceStub {
    pub query_string: *const c_char,
    pub num_params: c_int,
    pub param_types: *mut Oid,
    pub query_list: *mut List,
    pub raw_parse_tree: *mut RawStmtStub,
    pub commandTag: CommandTag,
    pub fixed_result: bool,
    pub resultDesc: *mut c_void,
    pub context: *mut c_void,
    pub stmts: *mut List,
}

#[repr(C)]
struct CachedPlanStub {
    pub stmt_list: *mut List,
}

#[repr(C)]
struct ParamListStub {
    pub numParams: c_int,
    pub params: *mut ParamExternDataStub,
    pub paramValuesStr: *mut c_char,
}

#[repr(C)]
struct ParamExternDataStub {
    pub value: Datum,
    pub isnull: bool,
    pub pflags: u32,
    pub ptype: Oid,
}

#[repr(C)]
struct TransactionStmtStub {
    pub r#type: c_int,
    pub kind: c_int,
}

#[repr(C)]
struct ExecuteStmtStub {
    pub r#type: c_int,
    pub name: *mut c_char,
}

#[repr(C)]
struct BindParamCbDataInner {
    pub portalName: *const c_char,
    pub paramno: c_int,
    pub paramval: *const c_char,
}

#[repr(C)]
struct ErrorContextCallbackInner {
    pub previous: *mut ErrorContextCallbackInner,
    pub callback: unsafe fn(*mut c_void),
    pub arg: *mut c_void,
}

#[repr(C)]
struct ParamsErrorCbDataInner {
    pub portalName: *const c_char,
    pub params: ParamListInfo,
}

#[repr(C)]
struct ProcStub {
    pub recoveryConflictPending: bool,
}

pub static mut MyReplicationSlot: *mut c_void = std::ptr::null_mut();


// ---------------------------------------------------------------------------
// exec_parse_message
//
// Execute a "Parse" protocol message.
// ---------------------------------------------------------------------------
unsafe fn exec_parse_message(
    query_string: *const c_char,
    stmt_name: *const c_char,
    paramTypes: *mut Oid,
    numParams: c_int,
) {
    let mut unnamed_stmt_context: *mut c_void = std::ptr::null_mut();
    let oldcontext: *mut c_void;
    let parsetree_list: *mut List;
    let raw_parse_tree: *mut RawStmtStub;
    let querytree_list: *mut List;
    let psrc: *mut CachedPlanSource;
    let is_named: bool;
    let save_log_statement_stats: bool = log_statement_stats;
    let mut msec_str: [c_char; 32] = [0; 32];
    let mut numParams_mut: c_int = numParams;
    let mut paramTypes_mut: *mut Oid = paramTypes;

    /*
     * Report query to various monitoring facilities.
     */
    debug_query_string = query_string;

    pgstat_report_activity(STATE_RUNNING, query_string);

    set_ps_display(b"PARSE\0".as_ptr() as *const c_char);

    if save_log_statement_stats {
        ResetUsage();
    }

    ereport!(DEBUG2, errmsg!("parse {}: {}", CStr::from_ptr(if *stmt_name != 0 { stmt_name } else { b"<unnamed>\0".as_ptr() as *const c_char }).to_string_lossy(), CStr::from_ptr(query_string).to_string_lossy()));
    /* C also: errmsg_internal */

    /*
     * Start up a transaction command so we can run parse analysis etc.
     */
    start_xact_command();

    /*
     * Switch to appropriate context for constructing parsetrees.
     *
     * We have two strategies depending on whether the prepared statement is
     * named or not.
     */
    is_named = *stmt_name != 0;
    if is_named {
        /* Named prepared statement --- parse in MessageContext */
        oldcontext = MemoryContextSwitchTo(MessageContext);
    } else {
        /* Unnamed prepared statement --- release any prior unnamed stmt */
        drop_unnamed_stmt();
        /* Create context for parsing */
        unnamed_stmt_context = AllocSetContextCreate(
            MessageContext,
            b"unnamed prepared statement\0".as_ptr() as *const c_char,
            0, /* ALLOCSET_DEFAULT_MINSIZE */
            8 * 1024, /* ALLOCSET_DEFAULT_INITSIZE */
            8 * 1024 * 1024 /* ALLOCSET_DEFAULT_MAXSIZE */
        );
        oldcontext = MemoryContextSwitchTo(unnamed_stmt_context);
    }

    /*
     * Do basic parsing of the query or queries.
     */
    parsetree_list = pg_parse_query(query_string);

    /*
     * We only allow a single user statement in a prepared statement.
     */
    if list_length(parsetree_list) > 1 {
        ereport!(ERROR, errmsg!("cannot insert multiple commands into a prepared statement"));
        /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
    }

    if parsetree_list != NIL {
        let mut snapshot_set: bool = false;

        raw_parse_tree = linitial_node!(RawStmtStub, T_RawStmt, parsetree_list);

        /*
         * If we are in an aborted transaction, reject all commands except
         * COMMIT/ROLLBACK.
         */
        if IsAbortedTransactionBlockState()
            && !IsTransactionExitStmt((*raw_parse_tree).stmt as *mut Node)
        {
            ereport!(ERROR, errmsg!("current transaction is aborted, commands ignored until end of transaction block"));
            /* C also: errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION), errdetail_abort() */
        }

        /*
         * Create the CachedPlanSource before we do parse analysis.
         */
        psrc = CreateCachedPlan(
            raw_parse_tree as *mut RawStmt,
            query_string,
            CreateCommandTag((*raw_parse_tree).stmt as *mut Node),
        );

        /*
         * Set up a snapshot if parse analysis will need one.
         */
        if analyze_requires_snapshot(raw_parse_tree as *mut RawStmt) {
            PushActiveSnapshot(GetTransactionSnapshot());
            snapshot_set = true;
        }

        /*
         * Analyze and rewrite the query.
         */
        querytree_list = pg_analyze_and_rewrite_varparams(
            raw_parse_tree as *mut RawStmt,
            query_string,
            &mut paramTypes_mut,
            &mut numParams_mut,
            std::ptr::null_mut(),
        );

        /* Done with the snapshot used for parsing */
        if snapshot_set {
            PopActiveSnapshot();
        }
    } else {
        /* Empty input string.  This is legal. */
        raw_parse_tree = std::ptr::null_mut();
        psrc = CreateCachedPlan(std::ptr::null_mut(), query_string, CMDTAG_UNKNOWN);
        querytree_list = NIL;
    }

    /*
     * CachedPlanSource must be a direct child of MessageContext before we
     * reparent unnamed_stmt_context under it.
     */
    if !unnamed_stmt_context.is_null() {
        MemoryContextSetParent((*(psrc as *mut CachedPlanSourceStub)).context, MessageContext);
    }

    /* Finish filling in the CachedPlanSource */
    CompleteCachedPlan(
        psrc,
        querytree_list,
        unnamed_stmt_context,
        paramTypes_mut,
        numParams_mut,
        std::ptr::null_mut(),
        std::ptr::null_mut(),
        CURSOR_OPT_PARALLEL_OK, /* allow parallel mode */
        true,                   /* fixed result */
    );

    /* If we got a cancel signal during analysis, quit */
    if InterruptPending {
        ProcessInterrupts();
    }

    if is_named {
        /*
         * Store the query as a prepared statement.
         */
        StorePreparedStatement(stmt_name, psrc, false);
    } else {
        /*
         * We just save the CachedPlanSource into unnamed_stmt_psrc.
         */
        SaveCachedPlan(psrc);
        unnamed_stmt_psrc = psrc;
    }

    MemoryContextSwitchTo(oldcontext);

    /*
     * We do NOT close the open transaction command here; that only happens
     * when the client sends Sync.  Instead, do CommandCounterIncrement just
     * in case something happened during parse/plan.
     */
    CommandCounterIncrement();

    /*
     * Send ParseComplete.
     */
    if whereToSendOutput == DestRemote {
        pq_putemptymessage(PqMsg_ParseComplete);
    }

    /*
     * Emit duration logging if appropriate.
     */
    match check_log_duration(msec_str.as_mut_ptr(), false) {
        1 => {
            ereport!(LOG, errmsg!("duration: {} ms", CStr::from_ptr(msec_str.as_ptr()).to_string_lossy()));
            /* C also: errhidestmt(true) */
        }
        2 => {
            ereport!(LOG, errmsg!("duration: {} ms  parse {}: {}", CStr::from_ptr(msec_str.as_ptr()).to_string_lossy(), CStr::from_ptr(if *stmt_name != 0 { stmt_name } else { b"<unnamed>\0".as_ptr() as *const c_char }).to_string_lossy(), CStr::from_ptr(query_string).to_string_lossy()));
            /* C also: errhidestmt(true) */
        }
        _ => {}
    }

    if save_log_statement_stats {
        ShowUsage(b"PARSE MESSAGE STATISTICS\0".as_ptr() as *const c_char);
    }

    debug_query_string = std::ptr::null();
}


// ---------------------------------------------------------------------------
// exec_bind_message
//
// Process a "Bind" message to create a portal from a prepared statement.
// ---------------------------------------------------------------------------
unsafe fn exec_bind_message(input_message: StringInfo) {
    let portal_name: *const c_char;
    let stmt_name: *const c_char;
    let numPFormats: c_int;
    let mut pformats: *mut i16 = std::ptr::null_mut();
    let numParams: c_int;
    let numRFormats: c_int;
    let mut rformats: *mut i16 = std::ptr::null_mut();
    let psrc: *mut CachedPlanSource;
    let cplan: *mut CachedPlan;
    let portal: Portal;
    let query_string: *mut c_char;
    let saved_stmt_name: *mut c_char;
    let params: ParamListInfo;
    let oldContext: *mut c_void;
    let save_log_statement_stats: bool = log_statement_stats;
    let mut snapshot_set: bool = false;
    let mut msec_str: [c_char; 32] = [0; 32];
    let mut params_data: ParamsErrorCbDataInner;
    let mut params_errcxt: ErrorContextCallbackInner = std::mem::zeroed();
    let lc: *mut crate::nodes::pg_list::ListCell = std::ptr::null_mut();

    /* Get the fixed part of the message */
    portal_name = pq_getmsgstring(input_message);
    stmt_name = pq_getmsgstring(input_message);

    ereport!(DEBUG2, errmsg!("bind {} to {}", CStr::from_ptr(if *portal_name != 0 { portal_name } else { b"<unnamed>\0".as_ptr() as *const c_char }).to_string_lossy(), CStr::from_ptr(if *stmt_name != 0 { stmt_name } else { b"<unnamed>\0".as_ptr() as *const c_char }).to_string_lossy()));
    /* C also: errmsg_internal */

    /* Find prepared statement */
    if *stmt_name != 0 {
        let pstmt: *mut PreparedStatementStub =
            FetchPreparedStatement(stmt_name, true) as *mut PreparedStatementStub;
        psrc = (*pstmt).plansource;
    } else {
        /* special-case the unnamed statement */
        psrc = unnamed_stmt_psrc;
        if psrc.is_null() {
            ereport!(ERROR, errmsg!("unnamed prepared statement does not exist"));
            /* C also: errcode(ERRCODE_UNDEFINED_PSTATEMENT) */
        }
    }

    /*
     * Report query to various monitoring facilities.
     */
    debug_query_string = (*(psrc as *mut CachedPlanSourceStub)).query_string;

    pgstat_report_activity(STATE_RUNNING, debug_query_string);

    foreach!(lc, (*(psrc as *mut CachedPlanSourceStub)).query_list, {
        let query: *mut QueryStub =
            lfirst_node!(QueryStub, T_Query, crate::current_cell!(lc));
        if (*query).queryId != 0 {
            pgstat_report_query_id((*query).queryId, false);
            break;
        }
    });

    set_ps_display(b"BIND\0".as_ptr() as *const c_char);

    if save_log_statement_stats {
        ResetUsage();
    }

    /*
     * Start up a transaction command.
     */
    start_xact_command();

    /* Switch back to message context */
    MemoryContextSwitchTo(MessageContext);

    /* Get the parameter format codes */
    numPFormats = pq_getmsgint(input_message, 2);
    if numPFormats > 0 {
        pformats = palloc_array(
            std::mem::size_of::<i16>(),
            numPFormats as usize,
        ) as *mut i16;
        for i in 0..numPFormats {
            *pformats.offset(i as isize) = pq_getmsgint(input_message, 2) as i16;
        }
    }

    /* Get the parameter value count */
    numParams = pq_getmsgint(input_message, 2);

    if numPFormats > 1 && numPFormats != numParams {
        ereport!(ERROR, errmsg!("bind message has {} parameter formats but {} parameters", numPFormats, numParams));
        /* C also: errcode(ERRCODE_PROTOCOL_VIOLATION) */
    }

    if numParams != (*(psrc as *mut CachedPlanSourceStub)).num_params {
        ereport!(ERROR, errmsg!("bind message supplies {} parameters, but prepared statement \"{}\" requires {}", numParams, CStr::from_ptr(stmt_name).to_string_lossy(), (*(psrc as *mut CachedPlanSourceStub)).num_params));
        /* C also: errcode(ERRCODE_PROTOCOL_VIOLATION) */
    }

    /*
     * If we are in aborted transaction state, the only portals we can
     * actually run are those containing COMMIT or ROLLBACK commands.
     */
    if IsAbortedTransactionBlockState()
        && (!(!( *(psrc as *mut CachedPlanSourceStub)).raw_parse_tree.is_null()
                && IsTransactionExitStmt(
                    (*(*(psrc as *mut CachedPlanSourceStub)).raw_parse_tree).stmt as *mut Node,
                ))
            || numParams != 0)
    {
        ereport!(ERROR, errmsg!("current transaction is aborted, commands ignored until end of transaction block"));
        /* C also: errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION), errdetail_abort() */
    }

    /*
     * Create the portal.
     */
    if *portal_name == 0 {
        portal = CreatePortal(portal_name, true, true);
    } else {
        portal = CreatePortal(portal_name, false, false);
    }

    /*
     * Prepare to copy stuff into the portal's memory context.
     */
    oldContext = MemoryContextSwitchTo((*(portal as *mut PortalStub)).portalContext);

    /* Copy the plan's query string into the portal */
    query_string = pstrdup((*(psrc as *mut CachedPlanSourceStub)).query_string);

    /* Likewise make a copy of the statement name, unless it's unnamed */
    if *stmt_name != 0 {
        saved_stmt_name = pstrdup(stmt_name);
    } else {
        saved_stmt_name = std::ptr::null_mut();
    }

    /*
     * Set a snapshot if we have parameters to fetch or the query isn't a
     * utility command.
     */
    if numParams > 0
        || (!(*(psrc as *mut CachedPlanSourceStub)).raw_parse_tree.is_null()
            && analyze_requires_snapshot(
                (*(psrc as *mut CachedPlanSourceStub)).raw_parse_tree as *mut RawStmt,
            ))
    {
        PushActiveSnapshot(GetTransactionSnapshot());
        snapshot_set = true;
    }

    /*
     * Fetch parameters, if any, and store in the portal's memory context.
     */
    if numParams > 0 {
        let mut knownTextValues: *mut *mut c_char = std::ptr::null_mut();
        let mut one_param_data: BindParamCbDataInner = BindParamCbDataInner {
            portalName: (*(portal as *mut PortalStub)).name,
            paramno: -1,
            paramval: std::ptr::null(),
        };

        /*
         * Set up an error callback so that if there's an error in this phase,
         * we can report the specific parameter causing the problem.
         */
        params_errcxt.previous = error_context_stack as *mut ErrorContextCallbackInner;
        params_errcxt.callback = bind_param_error_callback;
        params_errcxt.arg = &mut one_param_data as *mut BindParamCbDataInner as *mut c_void;
        error_context_stack = &mut params_errcxt as *mut ErrorContextCallbackInner as *mut ErrorContextCallback;

        params = makeParamList(numParams);

        for paramno in 0..numParams {
            let ptype: Oid = *(*(psrc as *mut CachedPlanSourceStub))
                .param_types
                .offset(paramno as isize);
            let plength: i32;
            let pval: Datum;
            let isNull: bool;
            let mut pbuf: StringInfoData = std::mem::zeroed();
            let csave: c_char;
            let pformat: i16;

            one_param_data.paramno = paramno;
            one_param_data.paramval = std::ptr::null();

            plength = pq_getmsgint(input_message, 4) as i32;
            isNull = plength == -1;

            let pvalue: *mut c_char;
            if !isNull {
                /*
                 * Initialize a StringInfo pointing to the message buffer.
                 */
                pvalue = pq_getmsgbytes(input_message, plength) as *mut c_char;
                csave = *pvalue.offset(plength as isize);
                *pvalue.offset(plength as isize) = 0;
                initReadOnlyStringInfo(&mut pbuf, pvalue, plength);
            } else {
                pvalue = std::ptr::null_mut();
                csave = 0;
                pbuf.data = std::ptr::null_mut();
            }

            if numPFormats > 1 {
                pformat = *pformats.offset(paramno as isize);
            } else if numPFormats > 0 {
                pformat = *pformats.offset(0);
            } else {
                pformat = 0; /* default = text */
            }

            if pformat == 0 {
                /* text mode */
                let mut typinput: Oid = 0;
                let mut typioparam: Oid = 0;
                let pstring: *mut c_char;

                getTypeInputInfo(ptype, &mut typinput, &mut typioparam);

                if isNull {
                    pstring = std::ptr::null_mut();
                } else {
                    pstring = pg_client_to_server(pbuf.data, plength);
                }

                /* Now we can log the input string in case of error */
                one_param_data.paramval = pstring;

                pval = OidInputFunctionCall(typinput, pstring, typioparam, -1);

                one_param_data.paramval = std::ptr::null();

                /*
                 * If we might need to log parameters later, save a copy.
                 */
                if !pstring.is_null() {
                    if log_parameter_max_length_on_error != 0 {
                        let oldcxt = MemoryContextSwitchTo(MessageContext);

                        if knownTextValues.is_null() {
                            knownTextValues = palloc0_array(
                                std::mem::size_of::<*mut c_char>(),
                                numParams as usize,
                            ) as *mut *mut c_char;
                        }

                        if log_parameter_max_length_on_error < 0 {
                            *knownTextValues.offset(paramno as isize) = pstrdup(pstring);
                        } else {
                            /*
                             * We can trim the saved string, knowing that we
                             * won't print all of it.
                             */
                            *knownTextValues.offset(paramno as isize) = pnstrdup(
                                pstring,
                                log_parameter_max_length_on_error + 2 * 4, /* 2*MAX_MULTIBYTE_CHAR_LEN */
                            );
                        }

                        MemoryContextSwitchTo(oldcxt);
                    }
                    if pstring != pbuf.data {
                        pfree(pstring as *mut c_void);
                    }
                }
            } else if pformat == 1 {
                /* binary mode */
                let mut typreceive: Oid = 0;
                let mut typioparam: Oid = 0;
                let bufptr: *mut StringInfoData;

                getTypeBinaryInputInfo(ptype, &mut typreceive, &mut typioparam);

                if isNull {
                    bufptr = std::ptr::null_mut();
                } else {
                    bufptr = &mut pbuf;
                }

                pval = OidReceiveFunctionCall(typreceive, bufptr, typioparam, -1);

                /* Trouble if it didn't eat the whole buffer */
                if !isNull && pbuf.cursor != pbuf.len {
                    ereport!(ERROR, errmsg!("incorrect binary data format in bind parameter {}", paramno + 1));
                    /* C also: errcode(ERRCODE_INVALID_BINARY_REPRESENTATION) */
                }
            } else {
                ereport!(ERROR, errmsg!("unsupported format code: {}", pformat));
                /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
                pval = 0;
            }

            /* Restore message buffer contents */
            if !isNull {
                *pvalue.offset(plength as isize) = csave;
            }

            let param_entry = &mut *(*(params as *mut ParamListStub))
                .params
                .offset(paramno as isize);
            param_entry.value = pval;
            param_entry.isnull = isNull;
            param_entry.pflags = PARAM_FLAG_CONST;
            param_entry.ptype = ptype;
        }

        /* Pop the per-parameter error callback */
        error_context_stack = params_errcxt.previous as *mut ErrorContextCallback;

        /*
         * Once all parameters have been received, prepare for printing them.
         */
        if log_parameter_max_length_on_error != 0 {
            (*(params as *mut ParamListStub)).paramValuesStr = BuildParamLogString(
                params,
                knownTextValues,
                log_parameter_max_length_on_error,
            );
        }
    } else {
        params = std::ptr::null_mut();
    }

    /* Done storing stuff in portal's context */
    MemoryContextSwitchTo(oldContext);

    /*
     * Set up another error callback so that all the parameters are logged if
     * we get an error during the rest of the BIND processing.
     */
    params_data = ParamsErrorCbDataInner {
        portalName: (*(portal as *mut PortalStub)).name,
        params,
    };
    params_errcxt.previous = error_context_stack as *mut ErrorContextCallbackInner;
    params_errcxt.callback = ParamsErrorCallback as unsafe fn(*mut c_void);
    params_errcxt.arg = &mut params_data as *mut ParamsErrorCbDataInner as *mut c_void;
    error_context_stack = &mut params_errcxt as *mut ErrorContextCallbackInner as *mut ErrorContextCallback;

    /* Get the result format codes */
    numRFormats = pq_getmsgint(input_message, 2);
    if numRFormats > 0 {
        rformats = palloc_array(
            std::mem::size_of::<i16>(),
            numRFormats as usize,
        ) as *mut i16;
        for i in 0..numRFormats {
            *rformats.offset(i as isize) = pq_getmsgint(input_message, 2) as i16;
        }
    }

    pq_getmsgend(input_message);

    /*
     * Obtain a plan from the CachedPlanSource.
     */
    cplan = GetCachedPlan(psrc, params, std::ptr::null_mut(), std::ptr::null_mut());

    /*
     * Now we can define the portal.
     *
     * DO NOT put any code that could possibly throw an error between the
     * above GetCachedPlan call and here.
     */
    PortalDefineQuery(
        portal,
        saved_stmt_name,
        query_string,
        (*(psrc as *mut CachedPlanSourceStub)).commandTag,
        (*(cplan as *mut CachedPlanStub)).stmt_list,
        cplan,
    );

    /* Portal is defined, set the plan ID based on its contents. */
    let lc2: *mut crate::nodes::pg_list::ListCell = std::ptr::null_mut();
    foreach!(lc2, (*(portal as *mut PortalStub)).stmts, {
        let plan: *mut PlannedStmtStub =
            lfirst_node!(PlannedStmtStub, T_PlannedStmt, crate::current_cell!(lc2));
        if (*plan).planId != 0 {
            pgstat_report_plan_id((*plan).planId, false);
            break;
        }
    });

    /* Done with the snapshot used for parameter I/O and parsing/planning */
    if snapshot_set {
        PopActiveSnapshot();
    }

    /*
     * And we're ready to start portal execution.
     */
    PortalStart(portal, params, 0, std::ptr::null_mut());

    /*
     * Apply the result format requests to the portal.
     */
    PortalSetResultFormat(portal, numRFormats, rformats);

    /*
     * Done binding; remove the parameters error callback.
     */
    error_context_stack = params_errcxt.previous as *mut ErrorContextCallback;

    /*
     * Send BindComplete.
     */
    if whereToSendOutput == DestRemote {
        pq_putemptymessage(PqMsg_BindComplete);
    }

    /*
     * Emit duration logging if appropriate.
     */
    match check_log_duration(msec_str.as_mut_ptr(), false) {
        1 => {
            ereport!(LOG, errmsg!("duration: {} ms", CStr::from_ptr(msec_str.as_ptr()).to_string_lossy()));
            /* C also: errhidestmt(true) */
        }
        2 => {
            ereport!(LOG, errmsg!("duration: {} ms  bind {}{}{}: {}", CStr::from_ptr(msec_str.as_ptr()).to_string_lossy(), CStr::from_ptr(if *stmt_name != 0 { stmt_name } else { b"<unnamed>\0".as_ptr() as *const c_char }).to_string_lossy(), if *portal_name != 0 { "/" } else { "" }, CStr::from_ptr(if *portal_name != 0 { portal_name } else { b"\0".as_ptr() as *const c_char }).to_string_lossy(), CStr::from_ptr((*(psrc as *mut CachedPlanSourceStub)).query_string).to_string_lossy()));
            /* C also: errhidestmt(true), errdetail_params(params) */
        }
        _ => {}
    }

    if save_log_statement_stats {
        ShowUsage(b"BIND MESSAGE STATISTICS\0".as_ptr() as *const c_char);
    }

    valgrind_report_error_query(debug_query_string);

    debug_query_string = std::ptr::null();
}


// ---------------------------------------------------------------------------
// exec_execute_message
//
// Process an "Execute" message for a portal.
// ---------------------------------------------------------------------------
unsafe fn exec_execute_message(portal_name: *const c_char, max_rows: i64) {
    let dest: CommandDest;
    let receiver: *mut DestReceiver;
    let portal: Portal;
    let completed: bool;
    let mut qc: QueryCompletion = std::mem::zeroed();
    let sourceText: *const c_char;
    let prepStmtName: *const c_char;
    let portalParams: ParamListInfo;
    let save_log_statement_stats: bool = log_statement_stats;
    let is_xact_command: bool;
    let execute_is_fetch: bool;
    let mut was_logged: bool = false;
    let mut msec_str: [c_char; 32] = [0; 32];
    let mut params_data: ParamsErrorCbDataInner;
    let mut params_errcxt: ErrorContextCallbackInner = std::mem::zeroed();
    let cmdtagname: *const c_char;
    let mut cmdtaglen: usize = 0;
    let lc: *mut crate::nodes::pg_list::ListCell = std::ptr::null_mut();

    /* Adjust destination to tell printtup.c what to do */
    dest = whereToSendOutput;
    let dest = if dest == DestRemote { DestRemoteExecute } else { dest };

    portal = GetPortalByName(portal_name);
    if !PortalIsValid(portal) {
        ereport!(ERROR, errmsg!("portal \"{}\" does not exist", CStr::from_ptr(portal_name).to_string_lossy()));
        /* C also: errcode(ERRCODE_UNDEFINED_CURSOR) */
    }

    /*
     * If the original query was a null string, just return EmptyQueryResponse.
     */
    if (*(portal as *mut PortalStub)).commandTag == CMDTAG_UNKNOWN {
        Assert!((*(portal as *mut PortalStub)).stmts == NIL);
        NullCommand(dest);
        return;
    }

    /* Does the portal contain a transaction command? */
    is_xact_command = IsTransactionStmtList((*(portal as *mut PortalStub)).stmts);

    /*
     * We must copy the sourceText and prepStmtName into MessageContext in
     * case the portal is destroyed during finish_xact_command.
     */
    sourceText = pstrdup((*(portal as *mut PortalStub)).sourceText);
    if !(*(portal as *mut PortalStub)).prepStmtName.is_null() {
        prepStmtName = pstrdup((*(portal as *mut PortalStub)).prepStmtName);
    } else {
        prepStmtName = b"<unnamed>\0".as_ptr() as *const c_char;
    }
    portalParams = (*(portal as *mut PortalStub)).portalParams;

    /*
     * Report query to various monitoring facilities.
     */
    debug_query_string = sourceText;

    pgstat_report_activity(STATE_RUNNING, sourceText);

    foreach!(lc, (*(portal as *mut PortalStub)).stmts, {
        let stmt: *mut PlannedStmtStub =
            lfirst_node!(PlannedStmtStub, T_PlannedStmt, crate::current_cell!(lc));
        if (*stmt).queryId != 0 {
            pgstat_report_query_id((*stmt).queryId, false);
            break;
        }
    });

    let lc2: *mut crate::nodes::pg_list::ListCell = std::ptr::null_mut();
    foreach!(lc2, (*(portal as *mut PortalStub)).stmts, {
        let stmt: *mut PlannedStmtStub =
            lfirst_node!(PlannedStmtStub, T_PlannedStmt, crate::current_cell!(lc2));
        if (*stmt).planId != 0 {
            pgstat_report_plan_id((*stmt).planId, false);
            break;
        }
    });

    cmdtagname = GetCommandTagNameAndLen(
        (*(portal as *mut PortalStub)).commandTag,
        &mut cmdtaglen,
    );

    set_ps_display_with_len(cmdtagname, cmdtaglen);

    if save_log_statement_stats {
        ResetUsage();
    }

    BeginCommand((*(portal as *mut PortalStub)).commandTag, dest);

    /*
     * Create dest receiver in MessageContext.
     */
    receiver = CreateDestReceiver(dest);
    if dest == DestRemoteExecute {
        SetRemoteDestReceiverParams(receiver, portal);
    }

    /*
     * Ensure we are in a transaction command.
     */
    start_xact_command();

    /*
     * If we re-issue an Execute protocol request against an existing portal,
     * then we are only fetching more rows.
     */
    execute_is_fetch = !(*(portal as *mut PortalStub)).atStart;

    /* Log immediately if dictated by log_statement */
    if check_log_statement((*(portal as *mut PortalStub)).stmts) {
        /* C: errmsg("execute fetch from"/"execute" ...) */
        ereport!(LOG, errmsg!("{} {}{}{}: {}", if execute_is_fetch { "execute fetch from" } else { "execute" }, CStr::from_ptr(prepStmtName).to_string_lossy(), if *portal_name != 0 { "/" } else { "" }, CStr::from_ptr(if *portal_name != 0 { portal_name } else { b"\0".as_ptr() as *const c_char }).to_string_lossy(), CStr::from_ptr(sourceText).to_string_lossy()));
        /* C also: errhidestmt(true), errdetail_params(portalParams) */
        was_logged = true;
    }

    /*
     * If we are in aborted transaction state, the only portals we can
     * actually run are those containing COMMIT or ROLLBACK commands.
     */
    if IsAbortedTransactionBlockState()
        && !IsTransactionExitStmtList((*(portal as *mut PortalStub)).stmts)
    {
        ereport!(ERROR, errmsg!("current transaction is aborted, commands ignored until end of transaction block"));
        /* C also: errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION), errdetail_abort() */
    }

    /* Check for cancel signal before we start execution */
    if InterruptPending {
        ProcessInterrupts();
    }

    /*
     * Okay to run the portal.  Set the error callback so that parameters are
     * logged.
     */
    params_data = ParamsErrorCbDataInner {
        portalName: (*(portal as *mut PortalStub)).name,
        params: portalParams,
    };
    params_errcxt.previous = error_context_stack as *mut ErrorContextCallbackInner;
    params_errcxt.callback = ParamsErrorCallback as unsafe fn(*mut c_void);
    params_errcxt.arg = &mut params_data as *mut ParamsErrorCbDataInner as *mut c_void;
    error_context_stack = &mut params_errcxt as *mut ErrorContextCallbackInner as *mut ErrorContextCallback;

    let max_rows = if max_rows <= 0 { FETCH_ALL } else { max_rows };

    completed = PortalRun(
        portal,
        max_rows,
        true, /* always top level */
        receiver,
        receiver,
        &mut qc,
    );

    (*(receiver as *mut DestReceiverStub)).rDestroy.map(|f| f(receiver));

    /* Done executing; remove the params error callback */
    error_context_stack = params_errcxt.previous as *mut ErrorContextCallback;

    if completed {
        if is_xact_command || (MyXactFlags & XACT_FLAGS_NEEDIMMEDIATECOMMIT) != 0 {
            /*
             * If this was a transaction control statement, commit it.
             */
            finish_xact_command();

            /*
             * These commands typically don't have any parameters.
             */
            let portalParams: ParamListInfo = std::ptr::null_mut();
            let _ = portalParams; // shadow to avoid unused warning
        } else {
            /*
             * We need a CommandCounterIncrement after every query.
             */
            CommandCounterIncrement();

            /*
             * Set XACT_FLAGS_PIPELINING whenever we complete an Execute
             * message without immediately committing.
             */
            MyXactFlags |= XACT_FLAGS_PIPELINING;

            /*
             * Disable statement timeout whenever we complete an Execute message.
             */
            disable_statement_timeout();
        }

        /* Send appropriate CommandComplete to client */
        EndCommand(&qc, dest, false);
    } else {
        /* Portal run not complete, so send PortalSuspended */
        if whereToSendOutput == DestRemote {
            pq_putemptymessage(PqMsg_PortalSuspended);
        }

        /*
         * Set XACT_FLAGS_PIPELINING whenever we suspend an Execute message.
         */
        MyXactFlags |= XACT_FLAGS_PIPELINING;
    }

    /*
     * Emit duration logging if appropriate.
     */
    match check_log_duration(msec_str.as_mut_ptr(), was_logged) {
        1 => {
            ereport!(LOG, errmsg!("duration: {} ms", CStr::from_ptr(msec_str.as_ptr()).to_string_lossy()));
            /* C also: errhidestmt(true) */
        }
        2 => {
            ereport!(LOG, errmsg!("duration: {} ms  {} {}{}{}: {}", CStr::from_ptr(msec_str.as_ptr()).to_string_lossy(), if execute_is_fetch { "execute fetch from" } else { "execute" }, CStr::from_ptr(prepStmtName).to_string_lossy(), if *portal_name != 0 { "/" } else { "" }, CStr::from_ptr(if *portal_name != 0 { portal_name } else { b"\0".as_ptr() as *const c_char }).to_string_lossy(), CStr::from_ptr(sourceText).to_string_lossy()));
            /* C also: errhidestmt(true), errdetail_params(portalParams) */
        }
        _ => {}
    }

    if save_log_statement_stats {
        ShowUsage(b"EXECUTE MESSAGE STATISTICS\0".as_ptr() as *const c_char);
    }

    valgrind_report_error_query(debug_query_string);

    debug_query_string = std::ptr::null();
}


// ---------------------------------------------------------------------------
// check_log_statement
//
// Determine whether command should be logged because of log_statement.
// ---------------------------------------------------------------------------
unsafe fn check_log_statement(stmt_list: *mut List) -> bool {
    let stmt_item: *mut crate::nodes::pg_list::ListCell = std::ptr::null_mut();

    if log_statement == LOGSTMT_NONE {
        return false;
    }
    if log_statement == LOGSTMT_ALL {
        return true;
    }

    /* Else we have to inspect the statement(s) to see whether to log */
    foreach!(stmt_item, stmt_list, {
        let stmt: *mut Node = lfirst(crate::current_cell!(stmt_item)) as *mut Node;

        if GetCommandLogLevel(stmt) <= log_statement {
            return true;
        }
    });

    false
}

// ---------------------------------------------------------------------------
// check_log_duration
//
// Determine whether current command's duration should be logged.
//
// Returns:
//   0 if no logging is needed
//   1 if just the duration should be logged
//   2 if duration and query details should be logged
// ---------------------------------------------------------------------------
pub unsafe fn check_log_duration(msec_str: *mut c_char, was_logged: bool) -> c_int {
    if log_duration
        || log_min_duration_sample >= 0
        || log_min_duration_statement >= 0
        || xact_is_sampled
    {
        let mut secs: i64 = 0;
        let mut usecs: c_int = 0;
        let msecs: i32;
        let exceeded_duration: bool;
        let exceeded_sample_duration: bool;
        let mut in_sample: bool = false;

        TimestampDifference(
            GetCurrentStatementStartTimestamp(),
            GetCurrentTimestamp(),
            &mut secs,
            &mut usecs,
        );
        msecs = usecs / 1000;

        /*
         * This odd-looking test for log_min_duration_* being exceeded is
         * designed to avoid integer overflow with very long durations.
         */
        exceeded_duration = log_min_duration_statement == 0
            || (log_min_duration_statement > 0
                && (secs > log_min_duration_statement / 1000
                    || secs * 1000 + msecs as i64 >= log_min_duration_statement));

        exceeded_sample_duration = log_min_duration_sample == 0
            || (log_min_duration_sample > 0
                && (secs > log_min_duration_sample / 1000
                    || secs * 1000 + msecs as i64 >= log_min_duration_sample));

        /*
         * Do not log if log_statement_sample_rate = 0.
         */
        if exceeded_sample_duration {
            in_sample = log_statement_sample_rate != 0.0
                && (log_statement_sample_rate == 1.0
                    || pg_prng_double(&mut pg_global_prng_state as *mut PgPrngState as *mut c_void)
                        <= log_statement_sample_rate);
        }

        if exceeded_duration || in_sample || log_duration || xact_is_sampled {
            // format: "%ld.%03d" into msec_str
            let s = format!("{}.{:03}", secs * 1000 + msecs as i64, usecs % 1000);
            let bytes = s.as_bytes();
            let len = bytes.len().min(31);
            for i in 0..len {
                *msec_str.add(i) = bytes[i] as c_char;
            }
            *msec_str.add(len) = 0;

            if (exceeded_duration || in_sample || xact_is_sampled) && !was_logged {
                return 2;
            } else {
                return 1;
            }
        }
    }

    0
}

// ---------------------------------------------------------------------------
// errdetail_execute
//
// Add an errdetail() line showing the query referenced by an EXECUTE.
// ---------------------------------------------------------------------------
unsafe fn errdetail_execute(raw_parsetree_list: *mut List) -> c_int {
    let parsetree_item: *mut crate::nodes::pg_list::ListCell = std::ptr::null_mut();

    foreach!(parsetree_item, raw_parsetree_list, {
        let parsetree: *mut RawStmtStub =
            lfirst_node!(RawStmtStub, T_RawStmt, crate::current_cell!(parsetree_item));

        if IsA!((*parsetree).stmt as *mut Node, T_ExecuteStmt) {
            let stmt: *mut ExecuteStmtStub = (*parsetree).stmt as *mut ExecuteStmtStub;
            let pstmt: *mut PreparedStatementStub =
                FetchPreparedStatement((*stmt).name, false) as *mut PreparedStatementStub;
            if !pstmt.is_null() {
                errdetail!(
                    "prepare: {}",
                    CStr::from_ptr((*((*pstmt).plansource as *mut CachedPlanSourceStub)).query_string).to_string_lossy()
                );
                return 0;
            }
        }
    });

    0
}

// ---------------------------------------------------------------------------
// errdetail_params
//
// Add an errdetail() line showing bind-parameter data.
// ---------------------------------------------------------------------------
unsafe fn errdetail_params(params: ParamListInfo) -> c_int {
    if !params.is_null()
        && (*(params as *mut ParamListStub)).numParams > 0
        && log_parameter_max_length != 0
    {
        let str_: *mut c_char = BuildParamLogString(
            params,
            std::ptr::null_mut(),
            log_parameter_max_length,
        );
        if !str_.is_null() && *str_ != 0 {
            errdetail!("Parameters: {}", CStr::from_ptr(str_).to_string_lossy());
        }
    }

    0
}

// ---------------------------------------------------------------------------
// errdetail_abort
//
// Add an errdetail() line showing abort reason.
// ---------------------------------------------------------------------------
unsafe fn errdetail_abort() -> c_int {
    if (*(MyProc as *mut ProcStub)).recoveryConflictPending {
        errdetail!("Abort reason: recovery conflict");
    }

    0
}

// ---------------------------------------------------------------------------
// errdetail_recovery_conflict
//
// Add an errdetail() line showing conflict source.
// ---------------------------------------------------------------------------
unsafe fn errdetail_recovery_conflict(reason: ProcSignalReason) -> c_int {
    match reason {
        PROCSIG_RECOVERY_CONFLICT_BUFFERPIN => {
            errdetail!("User was holding shared buffer pin for too long.");
        }
        PROCSIG_RECOVERY_CONFLICT_LOCK => {
            errdetail!("User was holding a relation lock for too long.");
        }
        PROCSIG_RECOVERY_CONFLICT_TABLESPACE => {
            errdetail!("User was or might have been using tablespace that must be dropped.");
        }
        PROCSIG_RECOVERY_CONFLICT_SNAPSHOT => {
            errdetail!("User query might have needed to see row versions that must be removed.");
        }
        PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT => {
            errdetail!("User was using a logical replication slot that must be invalidated.");
        }
        PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK => {
            errdetail!("User transaction caused buffer deadlock with recovery.");
        }
        PROCSIG_RECOVERY_CONFLICT_DATABASE => {
            errdetail!("User was connected to a database that must be dropped.");
        }
        _ => {
            /* no errdetail */
        }
    }

    0
}


// ---------------------------------------------------------------------------
// bind_param_error_callback
//
// Error context callback used while parsing parameters in a Bind message.
// ---------------------------------------------------------------------------
unsafe fn bind_param_error_callback(arg: *mut c_void) {
    let data: *mut BindParamCbDataInner = arg as *mut BindParamCbDataInner;
    let mut buf: StringInfoData = std::mem::zeroed();
    let quotedval: *mut c_char;

    if (*data).paramno < 0 {
        return;
    }

    /* If we have a textual value, quote it, and trim if necessary */
    if !(*data).paramval.is_null() {
        initStringInfo(&mut buf);
        appendStringInfoStringQuoted(&mut buf, (*data).paramval, log_parameter_max_length_on_error);
        quotedval = buf.data;
    } else {
        quotedval = std::ptr::null_mut();
    }

    if !(*data).portalName.is_null() && *(*data).portalName != 0 {
        if !quotedval.is_null() {
            errcontext(
                b"portal \"..\" parameter $%d = %s\0".as_ptr() as *const c_char,
            );
            /* C also: data->portalName, data->paramno+1, quotedval */
        } else {
            errcontext(
                b"portal \"..\" parameter $%d\0".as_ptr() as *const c_char,
            );
            /* C also: data->portalName, data->paramno+1 */
        }
    } else {
        if !quotedval.is_null() {
            errcontext(
                b"unnamed portal parameter $%d = %s\0".as_ptr() as *const c_char,
            );
            /* C also: data->paramno+1, quotedval */
        } else {
            errcontext(
                b"unnamed portal parameter $%d\0".as_ptr() as *const c_char,
            );
            /* C also: data->paramno+1 */
        }
    }

    if !quotedval.is_null() {
        pfree(quotedval as *mut c_void);
    }
}

// ---------------------------------------------------------------------------
// exec_describe_statement_message
//
// Process a "Describe" message for a prepared statement.
// ---------------------------------------------------------------------------
unsafe fn exec_describe_statement_message(stmt_name: *const c_char) {
    let psrc: *mut CachedPlanSource;

    /*
     * Start up a transaction command.
     */
    start_xact_command();

    /* Switch back to message context */
    MemoryContextSwitchTo(MessageContext);

    /* Find prepared statement */
    if *stmt_name != 0 {
        let pstmt: *mut PreparedStatementStub =
            FetchPreparedStatement(stmt_name, true) as *mut PreparedStatementStub;
        psrc = (*pstmt).plansource;
    } else {
        /* special-case the unnamed statement */
        psrc = unnamed_stmt_psrc;
        if psrc.is_null() {
            ereport!(ERROR, errmsg!("unnamed prepared statement does not exist"));
            /* C also: errcode(ERRCODE_UNDEFINED_PSTATEMENT) */
        }
    }

    /* Prepared statements shouldn't have changeable result descs */
    Assert!((*(psrc as *mut CachedPlanSourceStub)).fixed_result);

    /*
     * If we are in aborted transaction state, we can't run
     * SendRowDescriptionMessage().
     */
    if IsAbortedTransactionBlockState()
        && !(*(psrc as *mut CachedPlanSourceStub)).resultDesc.is_null()
    {
        ereport!(ERROR, errmsg!("current transaction is aborted, commands ignored until end of transaction block"));
        /* C also: errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION), errdetail_abort() */
    }

    if whereToSendOutput != DestRemote {
        return; /* can't actually do anything... */
    }

    /*
     * First describe the parameters...
     */
    pq_beginmessage_reuse(&mut row_description_buf, PqMsg_ParameterDescription);
    pq_sendint16(&mut row_description_buf, (*(psrc as *mut CachedPlanSourceStub)).num_params as i16);

    for i in 0..(*(psrc as *mut CachedPlanSourceStub)).num_params {
        let ptype: Oid = *(*(psrc as *mut CachedPlanSourceStub))
            .param_types
            .offset(i as isize);
        pq_sendint32(&mut row_description_buf, ptype as i32);
    }
    pq_endmessage_reuse(&mut row_description_buf);

    /*
     * Next send RowDescription or NoData to describe the result...
     */
    if !(*(psrc as *mut CachedPlanSourceStub)).resultDesc.is_null() {
        let tlist: *mut List = CachedPlanGetTargetList(psrc, std::ptr::null_mut());

        SendRowDescriptionMessage(
            &mut row_description_buf,
            (*(psrc as *mut CachedPlanSourceStub)).resultDesc,
            tlist,
            std::ptr::null_mut(),
        );
    } else {
        pq_putemptymessage(PqMsg_NoData);
    }
}

// ---------------------------------------------------------------------------
// exec_describe_portal_message
//
// Process a "Describe" message for a portal.
// ---------------------------------------------------------------------------
unsafe fn exec_describe_portal_message(portal_name: *const c_char) {
    let portal: Portal;

    /*
     * Start up a transaction command.
     */
    start_xact_command();

    /* Switch back to message context */
    MemoryContextSwitchTo(MessageContext);

    portal = GetPortalByName(portal_name);
    if !PortalIsValid(portal) {
        ereport!(ERROR, errmsg!("portal \"{}\" does not exist", CStr::from_ptr(portal_name).to_string_lossy()));
        /* C also: errcode(ERRCODE_UNDEFINED_CURSOR) */
    }

    /*
     * If we are in aborted transaction state, we can't run
     * SendRowDescriptionMessage().
     */
    if IsAbortedTransactionBlockState()
        && !(*(portal as *mut PortalStub)).tupDesc.is_null()
    {
        ereport!(ERROR, errmsg!("current transaction is aborted, commands ignored until end of transaction block"));
        /* C also: errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION), errdetail_abort() */
    }

    if whereToSendOutput != DestRemote {
        return; /* can't actually do anything... */
    }

    if !(*(portal as *mut PortalStub)).tupDesc.is_null() {
        SendRowDescriptionMessage(
            &mut row_description_buf,
            (*(portal as *mut PortalStub)).tupDesc,
            FetchPortalTargetList(portal),
            (*(portal as *mut PortalStub)).formats,
        );
    } else {
        pq_putemptymessage(PqMsg_NoData);
    }
}

// ---------------------------------------------------------------------------
// start_xact_command / finish_xact_command
//
// Convenience routines for starting/committing a single command.
// ---------------------------------------------------------------------------
unsafe fn start_xact_command() {
    if !xact_started {
        StartTransactionCommand();

        xact_started = true;
    } else if (MyXactFlags & XACT_FLAGS_PIPELINING) != 0 {
        /*
         * When the first Execute message is completed, following commands
         * will be done in an implicit transaction block created via pipelining.
         */
        BeginImplicitTransactionBlock();
    }

    /*
     * Start statement timeout if necessary.
     */
    enable_statement_timeout();

    /* Start timeout for checking if the client has gone away if necessary. */
    if client_connection_check_interval > 0
        && IsUnderPostmaster
        && !MyProcPort.is_null()
        && !get_timeout_active(CLIENT_CONNECTION_CHECK_TIMEOUT)
    {
        enable_timeout_after(CLIENT_CONNECTION_CHECK_TIMEOUT, client_connection_check_interval as i64);
    }
}

unsafe fn finish_xact_command() {
    /* cancel active statement timeout after each command */
    disable_statement_timeout();

    if xact_started {
        CommitTransactionCommand();

        // MEMORY_CONTEXT_CHECKING block omitted (debug-only path)
        // SHOW_MEMORY_STATS block omitted (debug-only path)

        xact_started = false;
    }
}

// ---------------------------------------------------------------------------
// IsTransactionExitStmt / IsTransactionExitStmtList / IsTransactionStmtList
//
// Convenience routines for checking whether a statement is one of the
// ones that we allow in transaction-aborted state.
// ---------------------------------------------------------------------------

/* Test a bare parsetree */
unsafe fn IsTransactionExitStmt(parsetree: *mut Node) -> bool {
    if !parsetree.is_null() && IsA!(parsetree, T_TransactionStmt) {
        let stmt: *mut TransactionStmtStub = parsetree as *mut TransactionStmtStub;

        if (*stmt).kind == TRANS_STMT_COMMIT
            || (*stmt).kind == TRANS_STMT_PREPARE
            || (*stmt).kind == TRANS_STMT_ROLLBACK
            || (*stmt).kind == TRANS_STMT_ROLLBACK_TO
        {
            return true;
        }
    }
    false
}

/* Test a list that contains PlannedStmt nodes */
unsafe fn IsTransactionExitStmtList(pstmts: *mut List) -> bool {
    if list_length(pstmts) == 1 {
        let pstmt: *mut PlannedStmtStub =
            linitial_node!(PlannedStmtStub, T_PlannedStmt, pstmts);

        if (*pstmt).commandType == CMD_UTILITY
            && IsTransactionExitStmt((*pstmt).utilityStmt as *mut Node)
        {
            return true;
        }
    }
    false
}

/* Test a list that contains PlannedStmt nodes */
unsafe fn IsTransactionStmtList(pstmts: *mut List) -> bool {
    if list_length(pstmts) == 1 {
        let pstmt: *mut PlannedStmtStub =
            linitial_node!(PlannedStmtStub, T_PlannedStmt, pstmts);

        if (*pstmt).commandType == CMD_UTILITY
            && IsA!((*pstmt).utilityStmt as *mut Node, T_TransactionStmt)
        {
            return true;
        }
    }
    false
}

// ---------------------------------------------------------------------------
// drop_unnamed_stmt
//
// Release any existing unnamed prepared statement.
// ---------------------------------------------------------------------------
unsafe fn drop_unnamed_stmt() {
    /* paranoia to avoid a dangling pointer in case of error */
    if !unnamed_stmt_psrc.is_null() {
        let psrc = unnamed_stmt_psrc;
        unnamed_stmt_psrc = std::ptr::null_mut();
        DropCachedPlan(psrc);
    }
}


// ---------------------------------------------------------------------------
// Signal handler routines used in PostgresMain()
// ---------------------------------------------------------------------------

/*
 * quickdie() occurs when signaled SIGQUIT by the postmaster.
 *
 * Either some backend has bought the farm, or we've been told to shut down
 * "immediately"; so we need to stop what we're doing and exit.
 */
pub unsafe fn quickdie(_sig: c_int) {
    sigaddset(
        &mut BlockSig as *mut SigSet as *mut c_void,
        SIGQUIT,
    ); /* prevent nested calls */
    sigprocmask(1 /* SIG_SETMASK */, &mut BlockSig as *mut SigSet as *mut c_void, std::ptr::null_mut());

    /*
     * Prevent interrupts while exiting.
     */
    InterruptHoldoffCount += 1; /* HOLD_INTERRUPTS */

    /*
     * If we're aborting out of client auth, don't risk trying to send
     * anything to the client.
     */
    if ClientAuthInProgress && whereToSendOutput == DestRemote {
        whereToSendOutput = DestNone;
    }

    /*
     * Notify the client before exiting.
     *
     * It's dubious to call ereport() from a signal handler.  But it seems
     * better to try than to disconnect abruptly.
     */
    error_context_stack = std::ptr::null_mut();

    /*
     * When responding to a postmaster-issued signal, we send the message only
     * to the client.
     */
    match GetQuitSignalReason() {
        r if r == 0 /* PMQUIT_NOT_SENT */ => {
            /* Hmm, SIGQUIT arrived out of the blue */
            ereport!(WARNING, errmsg!("terminating connection because of unexpected SIGQUIT signal"));
            /* C also: errcode(ERRCODE_ADMIN_SHUTDOWN) */
        }
        r if r == 1 /* PMQUIT_FOR_CRASH */ => {
            /* A crash-and-restart cycle is in progress */
            ereport!(WARNING_CLIENT_ONLY, errmsg!("terminating connection because of crash of another server process"));
            /* C also: errcode(ERRCODE_CRASH_SHUTDOWN), errdetail("The postmaster..."), errhint(...) */
        }
        r if r == 2 /* PMQUIT_FOR_STOP */ => {
            /* Immediate-mode stop */
            ereport!(WARNING_CLIENT_ONLY, errmsg!("terminating connection due to immediate shutdown command"));
            /* C also: errcode(ERRCODE_ADMIN_SHUTDOWN) */
        }
        _ => {}
    }

    /*
     * We DO NOT want to run proc_exit() or atexit() callbacks.
     * Note we do _exit(2) not _exit(0).
     */
    std::process::exit(2);
}

/*
 * Shutdown signal from postmaster: abort transaction and exit
 * at soonest convenient time.
 */
pub unsafe fn die(_sig: c_int) {
    /* Don't joggle the elbow of proc_exit */
    if !{
        // proc_exit_inprogress placeholder
        false
    } {
        InterruptPending = true;
        ProcDiePending = true;
    }

    /* for the cumulative stats system */
    pgStatSessionEndCause = DISCONNECT_KILLED;

    /* If we're still here, waken anything waiting on the process latch */
    SetLatch(std::ptr::null_mut() /* MyLatch */);

    /*
     * If we're in single user mode, we want to quit immediately.
     */
    if DoingCommandRead && whereToSendOutput != DestRemote {
        ProcessInterrupts();
    }
}

/*
 * Query-cancel signal from postmaster: abort current transaction
 * at soonest convenient time.
 */
pub unsafe fn StatementCancelHandler(_sig: c_int) {
    /*
     * Don't joggle the elbow of proc_exit.
     */
    if !{
        // proc_exit_inprogress placeholder
        false
    } {
        InterruptPending = true;
        QueryCancelPending = true;
    }

    /* If we're still here, waken anything waiting on the process latch */
    SetLatch(std::ptr::null_mut() /* MyLatch */);
}

/* signal handler for floating point exception */
pub unsafe fn FloatExceptionHandler(_sig: c_int) {
    /* We're not returning, so no need to save errno */
    ereport!(ERROR, errmsg!("floating-point exception"));
    /* C also: errcode(ERRCODE_FLOATING_POINT_EXCEPTION), errdetail("An invalid floating-point operation was signaled...") */
}

/*
 * Tell the next CHECK_FOR_INTERRUPTS() to check for a particular type of
 * recovery conflict.  Runs in a SIGUSR1 handler.
 */
pub unsafe fn HandleRecoveryConflictInterrupt(reason: ProcSignalReason) {
    RecoveryConflictPendingReasons[reason as usize] = true;
    RecoveryConflictPending = true;
    InterruptPending = true;
    /* latch will be set by procsignal_sigusr1_handler */
}

/*
 * Check one individual conflict reason.
 */
unsafe fn ProcessRecoveryConflictInterrupt(reason: ProcSignalReason) {
    match reason {
        PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK => {
            /*
             * If we aren't waiting for a lock we can never deadlock.
             */
            if GetAwaitedLock().is_null() {
                return;
            }
            /* Intentional fall through to check wait for pin */
            if !HoldingBufferPinThatDelaysRecovery() {
                if reason == PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK
                    && GetStartupBufferPinWaitBufId() < 0
                {
                    CheckDeadLockAlert();
                }
                return;
            }

            (*(MyProc as *mut ProcStub)).recoveryConflictPending = true;

            /* Intentional fall through to error handling */
            'conflict_error: {
                if !IsTransactionOrTransactionBlock() {
                    return;
                }

                /* PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT handled in arm below */

                if !IsSubTransaction() {
                    if IsAbortedTransactionBlockState() {
                        return;
                    }

                    if !DoingCommandRead {
                        if QueryCancelHoldoffCount != 0 {
                            RecoveryConflictPendingReasons[reason as usize] = true;
                            RecoveryConflictPending = true;
                            InterruptPending = true;
                            return;
                        }

                        LockErrorCleanup();
                        pgstat_report_recovery_conflict(reason);
                        ereport!(ERROR, errmsg!("canceling statement due to conflict with recovery"));
                        /* C also: errcode(ERRCODE_T_R_SERIALIZATION_FAILURE), errdetail_recovery_conflict(reason) */
                        break 'conflict_error;
                    }
                }

                pgstat_report_recovery_conflict(reason);
                ereport!(FATAL, errmsg!("terminating connection due to conflict with recovery"));
                /* C also: errcode(ERRCODE_T_R_SERIALIZATION_FAILURE), errdetail_recovery_conflict(reason), errhint(...) */
            }
        }

        PROCSIG_RECOVERY_CONFLICT_BUFFERPIN => {
            if !HoldingBufferPinThatDelaysRecovery() {
                return;
            }

            (*(MyProc as *mut ProcStub)).recoveryConflictPending = true;

            if !IsTransactionOrTransactionBlock() {
                return;
            }

            if !IsSubTransaction() {
                if IsAbortedTransactionBlockState() {
                    return;
                }

                if !DoingCommandRead {
                    if QueryCancelHoldoffCount != 0 {
                        RecoveryConflictPendingReasons[reason as usize] = true;
                        RecoveryConflictPending = true;
                        InterruptPending = true;
                        return;
                    }

                    LockErrorCleanup();
                    pgstat_report_recovery_conflict(reason);
                    ereport!(ERROR, errmsg!("canceling statement due to conflict with recovery"));
                    return;
                }
            }

            pgstat_report_recovery_conflict(reason);
            ereport!(FATAL, errmsg!("terminating connection due to conflict with recovery"));
        }

        PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT => {
            /* always throws ERROR, never FATAL */
            if IsAbortedTransactionBlockState() {
                return;
            }

            if !DoingCommandRead {
                if QueryCancelHoldoffCount != 0 {
                    RecoveryConflictPendingReasons[reason as usize] = true;
                    RecoveryConflictPending = true;
                    InterruptPending = true;
                    return;
                }

                LockErrorCleanup();
                pgstat_report_recovery_conflict(reason);
                ereport!(ERROR, errmsg!("canceling statement due to conflict with recovery"));
            }
        }

        PROCSIG_RECOVERY_CONFLICT_LOCK
        | PROCSIG_RECOVERY_CONFLICT_TABLESPACE
        | PROCSIG_RECOVERY_CONFLICT_SNAPSHOT => {
            if !IsTransactionOrTransactionBlock() {
                return;
            }

            if !IsSubTransaction() {
                if IsAbortedTransactionBlockState() {
                    return;
                }

                if !DoingCommandRead {
                    if QueryCancelHoldoffCount != 0 {
                        RecoveryConflictPendingReasons[reason as usize] = true;
                        RecoveryConflictPending = true;
                        InterruptPending = true;
                        return;
                    }

                    LockErrorCleanup();
                    pgstat_report_recovery_conflict(reason);
                    ereport!(ERROR, errmsg!("canceling statement due to conflict with recovery"));
                    return;
                }
            }

            pgstat_report_recovery_conflict(reason);
            ereport!(FATAL, errmsg!("terminating connection due to conflict with recovery"));
        }

        PROCSIG_RECOVERY_CONFLICT_DATABASE => {
            pgstat_report_recovery_conflict(reason);
            ereport!(FATAL, errmsg!("terminating connection due to conflict with recovery"));
            /* C also: errcode(ERRCODE_DATABASE_DROPPED), errdetail_recovery_conflict(reason), errhint(...) */
        }

        _ => {
            elog!(FATAL, "unrecognized conflict mode: {}", reason);
        }
    }
}

/*
 * Check each possible recovery conflict reason.
 */
unsafe fn ProcessRecoveryConflictInterrupts() {
    /*
     * We don't need to worry about joggling the elbow of proc_exit.
     */
    Assert!(!false /* proc_exit_inprogress */);
    Assert!(InterruptHoldoffCount == 0);
    Assert!(RecoveryConflictPending);

    RecoveryConflictPending = false;

    let mut reason = PROCSIG_RECOVERY_CONFLICT_FIRST;
    while reason <= PROCSIG_RECOVERY_CONFLICT_LAST {
        if RecoveryConflictPendingReasons[reason as usize] {
            RecoveryConflictPendingReasons[reason as usize] = false;
            ProcessRecoveryConflictInterrupt(reason);
        }
        reason += 1;
    }
}


// ---------------------------------------------------------------------------
// ProcessInterrupts
//
// Out-of-line portion of CHECK_FOR_INTERRUPTS() macro.
// ---------------------------------------------------------------------------
pub unsafe fn ProcessInterrupts() {
    /* OK to accept any interrupts now? */
    if InterruptHoldoffCount != 0 || CritSectionCount != 0 {
        return;
    }
    InterruptPending = false;

    if ProcDiePending {
        ProcDiePending = false;
        QueryCancelPending = false; /* ProcDie trumps QueryCancel */
        LockErrorCleanup();
        /* As in quickdie, don't risk sending to client during auth */
        if ClientAuthInProgress && whereToSendOutput == DestRemote {
            whereToSendOutput = DestNone;
        }
        if ClientAuthInProgress {
            ereport!(FATAL, errmsg!("canceling authentication due to timeout"));
            /* C also: errcode(ERRCODE_QUERY_CANCELED) */
        } else if AmAutoVacuumWorkerProcess() {
            ereport!(FATAL, errmsg!("terminating autovacuum process due to administrator command"));
            /* C also: errcode(ERRCODE_ADMIN_SHUTDOWN) */
        } else if IsLogicalWorker() {
            ereport!(FATAL, errmsg!("terminating logical replication worker due to administrator command"));
            /* C also: errcode(ERRCODE_ADMIN_SHUTDOWN) */
        } else if IsLogicalLauncher() {
            ereport!(DEBUG1, errmsg!("logical replication launcher shutting down"));

            /*
             * The logical replication launcher can be stopped at any time.
             * Use exit status 1 so the background worker is restarted.
             */
            proc_exit(1);
        } else if AmWalReceiverProcess() {
            ereport!(FATAL, errmsg!("terminating walreceiver process due to administrator command"));
            /* C also: errcode(ERRCODE_ADMIN_SHUTDOWN) */
        } else if AmBackgroundWorkerProcess() {
            ereport!(FATAL, errmsg!("terminating background worker due to administrator command"));
            /* C also: errcode(ERRCODE_ADMIN_SHUTDOWN) */
        } else if AmIoWorkerProcess() {
            ereport!(DEBUG1, errmsg!("io worker shutting down due to administrator command"));

            proc_exit(0);
        } else {
            ereport!(FATAL, errmsg!("terminating connection due to administrator command"));
            /* C also: errcode(ERRCODE_ADMIN_SHUTDOWN) */
        }
    }

    if CheckClientConnectionPending {
        CheckClientConnectionPending = false;

        /*
         * Check for lost connection and re-arm, if still configured, but not
         * if we've arrived back at DoingCommandRead state.
         */
        if !DoingCommandRead && client_connection_check_interval > 0 {
            if !pq_check_connection() {
                ClientConnectionLost = true;
            } else {
                enable_timeout_after(CLIENT_CONNECTION_CHECK_TIMEOUT, client_connection_check_interval as i64);
            }
        }
    }

    if ClientConnectionLost {
        QueryCancelPending = false; /* lost connection trumps QueryCancel */
        LockErrorCleanup();
        /* don't send to client, we already know the connection to be dead. */
        whereToSendOutput = DestNone;
        ereport!(FATAL, errmsg!("connection to client lost"));
        /* C also: errcode(ERRCODE_CONNECTION_FAILURE) */
    }

    /*
     * Don't allow query cancel interrupts while reading input from the
     * client, because we might lose sync in the FE/BE protocol.
     */
    if QueryCancelPending && QueryCancelHoldoffCount != 0 {
        /*
         * Re-arm InterruptPending so that we process the cancel request as
         * soon as we're done reading the message.
         */
        InterruptPending = true;
    } else if QueryCancelPending {
        let lock_timeout_occurred: bool;
        let stmt_timeout_occurred: bool;

        QueryCancelPending = false;

        /*
         * If LOCK_TIMEOUT and STATEMENT_TIMEOUT indicators are both set, we
         * need to clear both.
         */
        lock_timeout_occurred = get_timeout_indicator(LOCK_TIMEOUT, true);
        stmt_timeout_occurred = get_timeout_indicator(STATEMENT_TIMEOUT, true);

        /*
         * If both were set, we want to report whichever timeout completed
         * earlier; a tie is broken in favor of lock timeout.
         */
        let mut lock_timeout_occurred = lock_timeout_occurred;
        if lock_timeout_occurred
            && stmt_timeout_occurred
            && get_timeout_finish_time(STATEMENT_TIMEOUT) < get_timeout_finish_time(LOCK_TIMEOUT)
        {
            lock_timeout_occurred = false; /* report stmt timeout */
        }

        if lock_timeout_occurred {
            LockErrorCleanup();
            ereport!(ERROR, errmsg!("canceling statement due to lock timeout"));
            /* C also: errcode(ERRCODE_LOCK_NOT_AVAILABLE) */
        }
        if stmt_timeout_occurred {
            LockErrorCleanup();
            ereport!(ERROR, errmsg!("canceling statement due to statement timeout"));
            /* C also: errcode(ERRCODE_QUERY_CANCELED) */
        }
        if AmAutoVacuumWorkerProcess() {
            LockErrorCleanup();
            ereport!(ERROR, errmsg!("canceling autovacuum task"));
            /* C also: errcode(ERRCODE_QUERY_CANCELED) */
        }

        /*
         * If we are reading a command from the client, just ignore the cancel
         * request.
         */
        if !DoingCommandRead {
            LockErrorCleanup();
            ereport!(ERROR, errmsg!("canceling statement due to user request"));
            /* C also: errcode(ERRCODE_QUERY_CANCELED) */
        }
    }

    if RecoveryConflictPending {
        ProcessRecoveryConflictInterrupts();
    }

    if IdleInTransactionSessionTimeoutPending {
        /*
         * If the GUC has been reset to zero, ignore the signal.
         */
        IdleInTransactionSessionTimeoutPending = false;
        if IdleInTransactionSessionTimeout > 0 {
            INJECTION_POINT(
                b"idle-in-transaction-session-timeout\0".as_ptr() as *const c_char,
                std::ptr::null_mut(),
            );
            ereport!(FATAL, errmsg!("terminating connection due to idle-in-transaction timeout"));
            /* C also: errcode(ERRCODE_IDLE_IN_TRANSACTION_SESSION_TIMEOUT) */
        }
    }

    if TransactionTimeoutPending {
        /* As above, ignore the signal if the GUC has been reset to zero. */
        TransactionTimeoutPending = false;
        if TransactionTimeout > 0 {
            INJECTION_POINT(
                b"transaction-timeout\0".as_ptr() as *const c_char,
                std::ptr::null_mut(),
            );
            ereport!(FATAL, errmsg!("terminating connection due to transaction timeout"));
            /* C also: errcode(ERRCODE_TRANSACTION_TIMEOUT) */
        }
    }

    if IdleSessionTimeoutPending {
        /* As above, ignore the signal if the GUC has been reset to zero. */
        IdleSessionTimeoutPending = false;
        if IdleSessionTimeout > 0 {
            INJECTION_POINT(
                b"idle-session-timeout\0".as_ptr() as *const c_char,
                std::ptr::null_mut(),
            );
            ereport!(FATAL, errmsg!("terminating connection due to idle-session timeout"));
            /* C also: errcode(ERRCODE_IDLE_SESSION_TIMEOUT) */
        }
    }

    /*
     * If there are pending stats updates and we currently are truly idle,
     * report stats now.
     */
    if IdleStatsUpdateTimeoutPending
        && DoingCommandRead
        && !IsTransactionOrTransactionBlock()
    {
        IdleStatsUpdateTimeoutPending = false;
        pgstat_report_stat(true);
    }

    if ProcSignalBarrierPending {
        ProcessProcSignalBarrier();
    }

    if ParallelMessagePending {
        ProcessParallelMessages();
    }

    if LogMemoryContextPending {
        ProcessLogMemoryContextInterrupt();
    }

    if ParallelApplyMessagePending {
        ProcessParallelApplyMessages();
    }
}


// ---------------------------------------------------------------------------
// GUC check/assign hooks
// ---------------------------------------------------------------------------

/*
 * GUC check_hook for client_connection_check_interval.
 */
pub unsafe fn check_client_connection_check_interval(
    newval: *mut c_int,
    _extra: *mut *mut c_void,
    _source: GucSource,
) -> bool {
    if !WaitEventSetCanReportClosed() && *newval != 0 {
        GUC_check_errdetail(
            b"\"client_connection_check_interval\" must be set to 0 on this platform.\0"
                .as_ptr() as *const c_char,
        );
        return false;
    }
    true
}

/*
 * GUC check_hook for log_parser_stats, log_planner_stats, log_executor_stats.
 */
pub unsafe fn check_stage_log_stats(
    newval: *mut bool,
    _extra: *mut *mut c_void,
    _source: GucSource,
) -> bool {
    if *newval && log_statement_stats {
        GUC_check_errdetail(
            b"Cannot enable parameter when \"log_statement_stats\" is true.\0".as_ptr()
                as *const c_char,
        );
        return false;
    }
    true
}

/*
 * GUC check_hook for log_statement_stats.
 */
pub unsafe fn check_log_stats(
    newval: *mut bool,
    _extra: *mut *mut c_void,
    _source: GucSource,
) -> bool {
    if *newval && (log_parser_stats || log_planner_stats || log_executor_stats) {
        GUC_check_errdetail(
            b"Cannot enable \"log_statement_stats\" when \"log_parser_stats\", \"log_planner_stats\", or \"log_executor_stats\" is true.\0"
                .as_ptr() as *const c_char,
        );
        return false;
    }
    true
}

/* GUC assign hook for transaction_timeout */
pub unsafe fn assign_transaction_timeout(newval: c_int, _extra: *mut c_void) {
    if IsTransactionState() {
        /*
         * If transaction_timeout GUC has changed within the transaction block
         * enable or disable the timer correspondingly.
         */
        if newval > 0 && !get_timeout_active(TRANSACTION_TIMEOUT) {
            enable_timeout_after(TRANSACTION_TIMEOUT, newval as i64);
        } else if newval <= 0 && get_timeout_active(TRANSACTION_TIMEOUT) {
            disable_timeout(TRANSACTION_TIMEOUT, false);
        }
    }
}

/*
 * GUC check_hook for restrict_nonsystem_relation_kind.
 */
pub unsafe fn check_restrict_nonsystem_relation_kind(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    _source: GucSource,
) -> bool {
    let rawstring: *mut c_char;
    let mut elemlist: *mut List = std::ptr::null_mut();
    let l: *mut crate::nodes::pg_list::ListCell = std::ptr::null_mut();
    let mut flags: c_int = 0;

    /* Need a modifiable copy of string */
    rawstring = pstrdup(*newval);

    if !SplitIdentifierString(rawstring, b',' as c_char, &mut elemlist) {
        /* syntax error in list */
        GUC_check_errdetail(b"List syntax is invalid.\0".as_ptr() as *const c_char);
        pfree(rawstring as *mut c_void);
        list_free(elemlist);
        return false;
    }

    foreach!(l, elemlist, {
        let tok: *const c_char = lfirst(crate::current_cell!(l)) as *const c_char;

        if pg_strcasecmp(tok, b"view\0".as_ptr() as *const c_char) == 0 {
            flags |= RESTRICT_RELKIND_VIEW;
        } else if pg_strcasecmp(tok, b"foreign-table\0".as_ptr() as *const c_char) == 0 {
            flags |= RESTRICT_RELKIND_FOREIGN_TABLE;
        } else {
            GUC_check_errdetail(b"Unrecognized key word.\0".as_ptr() as *const c_char);
            pfree(rawstring as *mut c_void);
            list_free(elemlist);
            return false;
        }
    });

    pfree(rawstring as *mut c_void);
    list_free(elemlist);

    /* Save the flags in *extra, for use by the assign function */
    *extra = guc_malloc(LOG, std::mem::size_of::<c_int>());
    if (*extra).is_null() {
        return false;
    }
    *(*extra as *mut c_int) = flags;

    true
}

/*
 * GUC assign_hook for restrict_nonsystem_relation_kind.
 */
pub unsafe fn assign_restrict_nonsystem_relation_kind(
    _newval: *const c_char,
    extra: *mut c_void,
) {
    let flags: *const c_int = extra as *const c_int;

    restrict_nonsystem_relation_kind = *flags;
}

// ---------------------------------------------------------------------------
// set_debug_options
//
// Apply "-d N" command line option.
// ---------------------------------------------------------------------------
pub unsafe fn set_debug_options(debug_flag: c_int, context: GucContext, source: GucSource) {
    if debug_flag > 0 {
        let mut debugstr: [c_char; 64] = [0; 64];
        let s = format!("debug{}", debug_flag);
        let bytes = s.as_bytes();
        let len = bytes.len().min(63);
        for i in 0..len {
            debugstr[i] = bytes[i] as c_char;
        }
        SetConfigOption(
            b"log_min_messages\0".as_ptr() as *const c_char,
            debugstr.as_ptr(),
            context,
            source,
        );
    } else {
        SetConfigOption(
            b"log_min_messages\0".as_ptr() as *const c_char,
            b"notice\0".as_ptr() as *const c_char,
            context,
            source,
        );
    }

    if debug_flag >= 1 && context == PGC_POSTMASTER {
        SetConfigOption(
            b"log_connections\0".as_ptr() as *const c_char,
            b"all\0".as_ptr() as *const c_char,
            context,
            source,
        );
        SetConfigOption(
            b"log_disconnections\0".as_ptr() as *const c_char,
            b"true\0".as_ptr() as *const c_char,
            context,
            source,
        );
    }
    if debug_flag >= 2 {
        SetConfigOption(
            b"log_statement\0".as_ptr() as *const c_char,
            b"all\0".as_ptr() as *const c_char,
            context,
            source,
        );
    }
    if debug_flag >= 3 {
        SetConfigOption(
            b"debug_print_parse\0".as_ptr() as *const c_char,
            b"true\0".as_ptr() as *const c_char,
            context,
            source,
        );
    }
    if debug_flag >= 4 {
        SetConfigOption(
            b"debug_print_plan\0".as_ptr() as *const c_char,
            b"true\0".as_ptr() as *const c_char,
            context,
            source,
        );
    }
    if debug_flag >= 5 {
        SetConfigOption(
            b"debug_print_rewritten\0".as_ptr() as *const c_char,
            b"true\0".as_ptr() as *const c_char,
            context,
            source,
        );
    }
}

pub unsafe fn set_plan_disabling_options(
    arg: *const c_char,
    context: GucContext,
    source: GucSource,
) -> bool {
    let tmp: *const c_char;

    match *arg as u8 as char {
        's' => tmp = b"enable_seqscan\0".as_ptr() as *const c_char,
        'i' => tmp = b"enable_indexscan\0".as_ptr() as *const c_char,
        'o' => tmp = b"enable_indexonlyscan\0".as_ptr() as *const c_char,
        'b' => tmp = b"enable_bitmapscan\0".as_ptr() as *const c_char,
        't' => tmp = b"enable_tidscan\0".as_ptr() as *const c_char,
        'n' => tmp = b"enable_nestloop\0".as_ptr() as *const c_char,
        'm' => tmp = b"enable_mergejoin\0".as_ptr() as *const c_char,
        'h' => tmp = b"enable_hashjoin\0".as_ptr() as *const c_char,
        _ => tmp = std::ptr::null(),
    }

    if !tmp.is_null() {
        SetConfigOption(tmp, b"false\0".as_ptr() as *const c_char, context, source);
        true
    } else {
        false
    }
}

pub unsafe fn get_stats_option_name(arg: *const c_char) -> *const c_char {
    match *arg as u8 as char {
        'p' => {
            if *arg.offset(1) as u8 as char == 'a' {
                /* "parser" */
                return b"log_parser_stats\0".as_ptr() as *const c_char;
            } else if *arg.offset(1) as u8 as char == 'l' {
                /* "planner" */
                return b"log_planner_stats\0".as_ptr() as *const c_char;
            }
        }
        'e' => {
            /* "executor" */
            return b"log_executor_stats\0".as_ptr() as *const c_char;
        }
        _ => {}
    }

    std::ptr::null()
}


// ---------------------------------------------------------------------------
// process_postgres_switches
//
// Parse command line arguments for backends.
// ---------------------------------------------------------------------------
pub unsafe fn process_postgres_switches(
    argc: c_int,
    argv: *mut *mut c_char,
    ctx: GucContext,
    dbname: *mut *const c_char,
) {
    let secure: bool = ctx == PGC_POSTMASTER;
    let mut errs: c_int = 0;
    let gucsource: GucSource;
    let mut flag: c_int;
    let mut argc = argc;
    let mut argv = argv;

    if secure {
        gucsource = PGC_S_ARGV; /* switches came from command line */

        /* Ignore the initial --single argument, if present */
        if argc > 1
            && *(*argv.offset(1)) == b'-' as c_char
            && *((*argv.offset(1)).offset(1)) == b'-' as c_char
        {
            /* check for "--single" */
            let s = CStr::from_ptr(*argv.offset(1)).to_string_lossy();
            if s == "--single" {
                argv = argv.offset(1);
                argc -= 1;
            }
        }
    } else {
        gucsource = PGC_S_CLIENT; /* switches came from client */
    }

    /* opterr = 0; (HAVE_INT_OPTERR) */
    opterr = 0;

    /*
     * Parse command-line options.
     */
    loop {
        flag = getopt(argc, argv, b"B:bC:c:D:d:EeFf:h:ijk:lN:nOPp:r:S:sTt:v:W:-:\0".as_ptr() as *const c_char);
        if flag == -1 {
            break;
        }

        match flag as u8 as char {
            'B' => {
                SetConfigOption(b"shared_buffers\0".as_ptr() as *const c_char, optarg, ctx, gucsource);
            }
            'b' => {
                /* Undocumented flag used for binary upgrades */
                if secure {
                    IsBinaryUpgrade = true;
                }
            }
            'C' => {
                /* ignored for consistency with the postmaster */
            }
            '-' => {
                /*
                 * Error if the user misplaced a special must-be-first option.
                 */
                if parse_dispatch_option(optarg) != DISPATCH_POSTMASTER {
                    ereport!(ERROR, errmsg!("--{} must be first argument", CStr::from_ptr(optarg).to_string_lossy()));
                    /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
                }
                /* FALLTHROUGH */
                let mut name: *mut c_char = std::ptr::null_mut();
                let mut value: *mut c_char = std::ptr::null_mut();
                ParseLongOption(optarg, &mut name, &mut value);
                if value.is_null() {
                    ereport!(ERROR, errmsg!("--{} requires a value", CStr::from_ptr(optarg).to_string_lossy()));
                    /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
                }
                SetConfigOption(name, value, ctx, gucsource);
                pfree(name as *mut c_void);
                pfree(value as *mut c_void);
            }
            'c' => {
                let mut name: *mut c_char = std::ptr::null_mut();
                let mut value: *mut c_char = std::ptr::null_mut();
                ParseLongOption(optarg, &mut name, &mut value);
                if value.is_null() {
                    ereport!(ERROR, errmsg!("-c {} requires a value", CStr::from_ptr(optarg).to_string_lossy()));
                    /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
                }
                SetConfigOption(name, value, ctx, gucsource);
                pfree(name as *mut c_void);
                pfree(value as *mut c_void);
            }
            'D' => {
                if secure {
                    userDoption = optarg;
                }
            }
            'd' => {
                let s = CStr::from_ptr(optarg).to_string_lossy();
                let n: c_int = s.parse().unwrap_or(0);
                set_debug_options(n, ctx, gucsource);
            }
            'E' => {
                if secure {
                    EchoQuery = true;
                }
            }
            'e' => {
                SetConfigOption(b"datestyle\0".as_ptr() as *const c_char, b"euro\0".as_ptr() as *const c_char, ctx, gucsource);
            }
            'F' => {
                SetConfigOption(b"fsync\0".as_ptr() as *const c_char, b"false\0".as_ptr() as *const c_char, ctx, gucsource);
            }
            'f' => {
                if !set_plan_disabling_options(optarg, ctx, gucsource) {
                    errs += 1;
                }
            }
            'h' => {
                SetConfigOption(b"listen_addresses\0".as_ptr() as *const c_char, optarg, ctx, gucsource);
            }
            'i' => {
                SetConfigOption(b"listen_addresses\0".as_ptr() as *const c_char, b"*\0".as_ptr() as *const c_char, ctx, gucsource);
            }
            'j' => {
                if secure {
                    UseSemiNewlineNewline = true;
                }
            }
            'k' => {
                SetConfigOption(b"unix_socket_directories\0".as_ptr() as *const c_char, optarg, ctx, gucsource);
            }
            'l' => {
                SetConfigOption(b"ssl\0".as_ptr() as *const c_char, b"true\0".as_ptr() as *const c_char, ctx, gucsource);
            }
            'N' => {
                SetConfigOption(b"max_connections\0".as_ptr() as *const c_char, optarg, ctx, gucsource);
            }
            'n' => {
                /* ignored for consistency with postmaster */
            }
            'O' => {
                SetConfigOption(b"allow_system_table_mods\0".as_ptr() as *const c_char, b"true\0".as_ptr() as *const c_char, ctx, gucsource);
            }
            'P' => {
                SetConfigOption(b"ignore_system_indexes\0".as_ptr() as *const c_char, b"true\0".as_ptr() as *const c_char, ctx, gucsource);
            }
            'p' => {
                SetConfigOption(b"port\0".as_ptr() as *const c_char, optarg, ctx, gucsource);
            }
            'r' => {
                /* send output (stdout and stderr) to the given file */
                if secure {
                    strlcpy(OutputFileName.as_mut_ptr(), optarg, MAXPGPATH);
                }
            }
            'S' => {
                SetConfigOption(b"work_mem\0".as_ptr() as *const c_char, optarg, ctx, gucsource);
            }
            's' => {
                SetConfigOption(b"log_statement_stats\0".as_ptr() as *const c_char, b"true\0".as_ptr() as *const c_char, ctx, gucsource);
            }
            'T' => {
                /* ignored for consistency with the postmaster */
            }
            't' => {
                let tmp = get_stats_option_name(optarg);
                if !tmp.is_null() {
                    SetConfigOption(tmp, b"true\0".as_ptr() as *const c_char, ctx, gucsource);
                } else {
                    errs += 1;
                }
            }
            'v' => {
                /*
                 * -v is no longer used in normal operation.
                 */
                if secure {
                    let s = CStr::from_ptr(optarg).to_string_lossy();
                    FrontendProtocol = s.parse().unwrap_or(0);
                }
            }
            'W' => {
                SetConfigOption(b"post_auth_delay\0".as_ptr() as *const c_char, optarg, ctx, gucsource);
            }
            _ => {
                errs += 1;
            }
        }

        if errs != 0 {
            break;
        }
    }

    /*
     * Optional database name should be there only if *dbname is NULL.
     */
    if errs == 0 && !dbname.is_null() && (*dbname).is_null() && argc - optind >= 1 {
        *dbname = *argv.offset(optind as isize);
        optind += 1;
    }

    if errs != 0 || argc != optind {
        if errs != 0 {
            optind -= 1; /* complain about the previous argument */
        }

        /* spell the error message a bit differently depending on context */
        if IsUnderPostmaster {
            ereport!(FATAL, errmsg!("invalid command-line argument for server process: {}", CStr::from_ptr(*argv.offset(optind as isize)).to_string_lossy()));
            /* C also: errcode(ERRCODE_SYNTAX_ERROR), errhint("Try ... --help") */
        } else {
            ereport!(FATAL, errmsg!("invalid command-line argument: {}", CStr::from_ptr(*argv.offset(optind as isize)).to_string_lossy()));
            /* C also: errcode(ERRCODE_SYNTAX_ERROR), errhint("Try ... --help") */
        }
    }

    /*
     * Reset getopt(3) library so that it will work correctly in subprocesses.
     */
    optind = 1;
    /* optreset = 1; (HAVE_INT_OPTRESET) */
    optreset = 1;
}


// ---------------------------------------------------------------------------
// PostgresSingleUserMain
//
// Entry point for single user mode.
// ---------------------------------------------------------------------------
pub unsafe fn PostgresSingleUserMain(
    argc: c_int,
    argv: *mut *mut c_char,
    username: *const c_char,
) {
    let mut dbname: *const c_char = std::ptr::null();

    Assert!(!IsUnderPostmaster);

    /* Initialize startup process environment. */
    InitStandaloneProcess(*argv);

    /*
     * Set default values for command-line options.
     */
    InitializeGUCOptions();

    /*
     * Parse command-line options.
     */
    process_postgres_switches(argc, argv, PGC_POSTMASTER, &mut dbname);

    /* Must have gotten a database name, or have a default (the username) */
    if dbname.is_null() {
        dbname = username;
        if dbname.is_null() {
            ereport!(FATAL, errmsg!("{}: no database nor user name specified", CStr::from_ptr(progname).to_string_lossy()));
            /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        }
    }

    /* Acquire configuration parameters */
    if !SelectConfigFiles(userDoption, progname) {
        proc_exit(1);
    }

    /*
     * Validate we have been given a reasonable-looking DataDir and change
     * into it.
     */
    checkDataDir();
    ChangeToDataDir();

    /*
     * Create lockfile for data directory.
     */
    CreateDataDirLockFile(false);

    /* read control file (error checking and contains config) */
    LocalProcessControlFile(false);

    /*
     * process any libraries that should be preloaded at postmaster start
     */
    process_shared_preload_libraries();

    /* Initialize MaxBackends */
    InitializeMaxBackends();

    /*
     * We don't need postmaster child slots in single-user mode, but
     * initialize them anyway.
     */
    InitPostmasterChildSlots();

    /* Initialize size of fast-path lock cache. */
    InitializeFastPathLocks();

    /*
     * Give preloaded libraries a chance to request additional shared memory.
     */
    process_shmem_requests();

    /*
     * Now that loadable modules have had their chance to request additional
     * shared memory, determine the value of any runtime-computed GUCs.
     */
    InitializeShmemGUCs();

    /*
     * Now that modules have been loaded, we can process any custom resource
     * managers.
     */
    InitializeWalConsistencyChecking();

    /*
     * Create shared memory etc.
     */
    CreateSharedMemoryAndSemaphores();

    /*
     * Estimate number of openable files.
     */
    set_max_safe_fds();

    /*
     * Remember stand-alone backend startup time.
     */
    PgStartTime = GetCurrentTimestamp();

    /*
     * Create a per-backend PGPROC struct in shared memory.
     */
    InitProcess();

    /*
     * Now that sufficient infrastructure has been initialized, PostgresMain()
     * can do the rest.
     */
    PostgresMain(dbname, username);
}


// Additional stub needed for progname
pub static mut progname: *const c_char = b"postgres\0".as_ptr() as *const c_char;

// ---------------------------------------------------------------------------
// PostgresMain
//
// postgres main loop -- all backends, interactive or otherwise loop here.
// ---------------------------------------------------------------------------
pub unsafe fn PostgresMain(dbname: *const c_char, username: *const c_char) {
    // These locals must be "volatile" in C; in Rust we handle via normal mutability
    let mut send_ready_for_query: bool = true;
    let mut idle_in_transaction_timeout_enabled: bool = false;
    let mut idle_session_timeout_enabled: bool = false;

    Assert!(!dbname.is_null());
    Assert!(!username.is_null());

    Assert!(GetProcessingMode() == 0 /* InitProcessing */);

    /*
     * Set up signal handlers.
     */
    if am_walsender {
        WalSndSignals();
    } else {
        pqsignal(SIGHUP, SignalHandlerForConfigReload as *mut c_void);
        pqsignal(SIGINT, StatementCancelHandler as *mut c_void);
        pqsignal(SIGTERM, die as *mut c_void);

        if IsUnderPostmaster {
            pqsignal(SIGQUIT, quickdie as *mut c_void); /* hard crash time */
        } else {
            pqsignal(SIGQUIT, die as *mut c_void);
        }
        InitializeTimeouts(); /* establishes SIGALRM handler */

        pqsignal(SIGPIPE, SIG_IGN as *mut c_void);
        pqsignal(SIGUSR1, procsignal_sigusr1_handler as *mut c_void);
        pqsignal(SIGUSR2, SIG_IGN as *mut c_void);
        pqsignal(SIGFPE, FloatExceptionHandler as *mut c_void);

        /*
         * Reset some signals that are accepted by postmaster but not by backend.
         */
        pqsignal(SIGCHLD, SIG_DFL as *mut c_void);
    }

    /* Early initialization */
    BaseInit();

    /* We need to allow SIGINT, etc during the initial transaction */
    sigprocmask(2 /* SIG_SETMASK */, &UnBlockSig as *const SigSet as *const c_void as *mut c_void, std::ptr::null_mut());

    /*
     * Generate a random cancel key, if this is a backend serving a connection.
     */
    Assert!(MyCancelKeyLength == 0);
    if whereToSendOutput == DestRemote {
        let len: c_int = if MyProcPort.is_null()
            || /* proto < PG_PROTOCOL(3,2) */ false
        {
            4
        } else {
            MAX_CANCEL_KEY_LENGTH as c_int
        };
        if !pg_strong_random(MyCancelKey.as_mut_ptr() as *mut c_void, len as usize) {
            ereport!(ERROR, errmsg!("could not generate random cancel key"));
            /* C also: errcode(ERRCODE_INTERNAL_ERROR) */
        }
        MyCancelKeyLength = len;
    }

    /*
     * General initialization.
     */
    InitPostgres(
        dbname,
        InvalidOid,  /* database to connect to */
        username,
        InvalidOid,  /* role to connect as */
        if !am_walsender { INIT_PG_LOAD_SESSION_LIBS } else { 0 },
        std::ptr::null_mut(), /* no out_dbname */
    );

    /*
     * If the PostmasterContext is still around, recycle the space.
     */
    if !PostmasterContext.is_null() {
        MemoryContextDelete(PostmasterContext);
        PostmasterContext = std::ptr::null_mut();
    }

    SetProcessingMode(2 /* NormalProcessing */);

    /*
     * Now all GUC states are fully set up.  Report them to client.
     */
    BeginReportingGUCOptions();

    /*
     * Also set up handler to log session end.
     */
    if IsUnderPostmaster && Log_disconnections {
        on_proc_exit(log_disconnections, 0);
    }

    pgstat_report_connect(MyDatabaseId);

    /* Perform initialization specific to a WAL sender process. */
    if am_walsender {
        InitWalSender();
    }

    /*
     * Send this backend's cancellation info to the frontend.
     */
    if whereToSendOutput == DestRemote {
        let mut buf: StringInfoData = std::mem::zeroed();

        Assert!(MyCancelKeyLength > 0);
        pq_beginmessage(&mut buf, PqMsg_BackendKeyData);
        pq_sendint32(&mut buf, /* MyProcPid */ 0i32);
        pq_sendbytes(&mut buf, MyCancelKey.as_ptr() as *const c_char, MyCancelKeyLength as usize);
        pq_endmessage(&mut buf);
        /* Need not flush since ReadyForQuery will do it. */
    }

    /* Welcome banner for standalone case */
    if whereToSendOutput == DestDebug {
        /* printf("\nPostgreSQL stand-alone backend %s\n", PG_VERSION); */
    }

    /*
     * Create the memory context we will use in the main loop.
     *
     * MessageContext is reset once per iteration of the main loop.
     */
    MessageContext = AllocSetContextCreate(
        TopMemoryContext,
        b"MessageContext\0".as_ptr() as *const c_char,
        0, /* ALLOCSET_DEFAULT_MINSIZE */
        8 * 1024, /* ALLOCSET_DEFAULT_INITSIZE */
        8 * 1024 * 1024 /* ALLOCSET_DEFAULT_MAXSIZE */
    );

    /*
     * Create memory context and buffer used for RowDescription messages.
     */
    row_description_context = AllocSetContextCreate(
        TopMemoryContext,
        b"RowDescriptionContext\0".as_ptr() as *const c_char,
        0, /* ALLOCSET_DEFAULT_MINSIZE */
        8 * 1024, /* ALLOCSET_DEFAULT_INITSIZE */
        8 * 1024 * 1024 /* ALLOCSET_DEFAULT_MAXSIZE */
    );
    MemoryContextSwitchTo(row_description_context);
    initStringInfo(&mut row_description_buf);
    MemoryContextSwitchTo(TopMemoryContext);

    /* Fire any defined login event triggers, if appropriate */
    EventTriggerOnLogin();

    /*
     * POSTGRES main processing loop begins here.
     *
     * If an exception is encountered, processing resumes here so we abort
     * the current transaction and start a new one.
     *
     * Note: we use sigsetjmp with savemask=1 so that UnBlockSig is restored.
     */
    // setjmp/longjmp are not representable in safe Rust; we approximate here.
    // The actual error recovery path is TODO(pg-port): wiring up the panic handler.
    // For now the loop runs as if no errors occur.
    // TODO(pg-port): Replace with proper error recovery mechanism.

    // Error recovery (normally reached via longjmp):
    // {
    //     error_context_stack = null;
    //     HOLD_INTERRUPTS();
    //     disable_all_timeouts(false);
    //     QueryCancelPending = false;
    //     idle_in_transaction_timeout_enabled = false;
    //     idle_session_timeout_enabled = false;
    //     DoingCommandRead = false;
    //     pq_comm_reset();
    //     EmitErrorReport();
    //     valgrind_report_error_query(debug_query_string);
    //     debug_query_string = null;
    //     AbortCurrentTransaction();
    //     if am_walsender { WalSndErrorCleanup(); }
    //     PortalErrorCleanup();
    //     if !MyReplicationSlot.is_null() { ReplicationSlotRelease(); }
    //     ReplicationSlotCleanup(false);
    //     jit_reset_after_error();
    //     MemoryContextSwitchTo(MessageContext);
    //     FlushErrorState();
    //     if doing_extended_query_message { ignore_till_sync = true; }
    //     xact_started = false;
    //     if pq_is_reading_msg() { ereport!(FATAL, ...) }
    //     RESUME_INTERRUPTS();
    // }

    // PG_exception_stack = &local_sigjmp_buf;

    if !ignore_till_sync {
        send_ready_for_query = true; /* initially, or after error */
    }

    /*
     * Non-error queries loop here.
     */
    loop {
        let firstchar: c_int;
        let mut input_message: StringInfoData = std::mem::zeroed();

        /*
         * At top of loop, reset extended-query-message flag.
         */
        doing_extended_query_message = false;

        /*
         * Release storage left over from prior query cycle, and create a new
         * query input buffer in the cleared MessageContext.
         */
        MemoryContextSwitchTo(MessageContext);
        MemoryContextReset(MessageContext);

        initStringInfo(&mut input_message);

        /*
         * Also consider releasing our catalog snapshot if any.
         */
        InvalidateCatalogSnapshotConditionally();

        /*
         * (1) If we've reached idle state, tell the frontend we're ready for
         * a new query.
         */
        if send_ready_for_query {
            if IsAbortedTransactionBlockState() {
                set_ps_display(b"idle in transaction (aborted)\0".as_ptr() as *const c_char);
                pgstat_report_activity(STATE_IDLEINTRANSACTION_ABORTED, std::ptr::null());

                /* Start the idle-in-transaction timer */
                if IdleInTransactionSessionTimeout > 0
                    && (IdleInTransactionSessionTimeout < TransactionTimeout
                        || TransactionTimeout == 0)
                {
                    idle_in_transaction_timeout_enabled = true;
                    enable_timeout_after(
                        IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
                        IdleInTransactionSessionTimeout,
                    );
                }
            } else if IsTransactionOrTransactionBlock() {
                set_ps_display(b"idle in transaction\0".as_ptr() as *const c_char);
                pgstat_report_activity(STATE_IDLEINTRANSACTION, std::ptr::null());

                /* Start the idle-in-transaction timer */
                if IdleInTransactionSessionTimeout > 0
                    && (IdleInTransactionSessionTimeout < TransactionTimeout
                        || TransactionTimeout == 0)
                {
                    idle_in_transaction_timeout_enabled = true;
                    enable_timeout_after(
                        IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
                        IdleInTransactionSessionTimeout,
                    );
                }
            } else {
                let stats_timeout: i64;

                /*
                 * Process incoming notifies, if any.
                 */
                if notifyInterruptPending {
                    ProcessNotifyInterrupt(false);
                }

                /*
                 * Check if we need to report stats.
                 */
                stats_timeout = pgstat_report_stat(false);
                if stats_timeout > 0 {
                    if !get_timeout_active(IDLE_STATS_UPDATE_TIMEOUT) {
                        enable_timeout_after(IDLE_STATS_UPDATE_TIMEOUT, stats_timeout);
                    }
                } else {
                    /* all stats flushed, no need for the timeout */
                    if get_timeout_active(IDLE_STATS_UPDATE_TIMEOUT) {
                        disable_timeout(IDLE_STATS_UPDATE_TIMEOUT, false);
                    }
                }

                set_ps_display(b"idle\0".as_ptr() as *const c_char);
                pgstat_report_activity(STATE_IDLE, std::ptr::null());

                /* Start the idle-session timer */
                if IdleSessionTimeout > 0 {
                    idle_session_timeout_enabled = true;
                    enable_timeout_after(IDLE_SESSION_TIMEOUT, IdleSessionTimeout);
                }
            }

            /* Report any recently-changed GUC options */
            ReportChangedGUCOptions();

            /*
             * The first time this backend is ready for query, log the
             * durations of the different components of connection
             * establishment and setup.
             */
            if conn_timing.ready_for_use == i64::MIN
                && (log_connections & LOG_CONNECTION_SETUP_DURATIONS) != 0
                && IsExternalConnectionBackend(MyBackendType)
            {
                let total_duration: u64;
                let fork_duration: u64;
                let auth_duration: u64;

                conn_timing.ready_for_use = GetCurrentTimestamp();

                total_duration = TimestampDifferenceMicroseconds(
                    conn_timing.socket_create,
                    conn_timing.ready_for_use,
                );
                fork_duration = TimestampDifferenceMicroseconds(
                    conn_timing.fork_start,
                    conn_timing.fork_end,
                );
                auth_duration = TimestampDifferenceMicroseconds(
                    conn_timing.auth_start,
                    conn_timing.auth_end,
                );

                ereport!(LOG, errmsg!("connection ready: setup total={:.3} ms, fork={:.3} ms, authentication={:.3} ms", total_duration as f64 / NS_PER_US as f64, fork_duration as f64 / NS_PER_US as f64, auth_duration as f64 / NS_PER_US as f64));
            }

            ReadyForQuery(whereToSendOutput);
            send_ready_for_query = false;
        }

        /*
         * (2) Allow asynchronous signals to be executed immediately if they
         * come in while we are waiting for client input.
         */
        DoingCommandRead = true;

        /*
         * (3) read a command (loop blocks here)
         */
        firstchar = ReadCommand(&mut input_message);

        /*
         * (4) turn off the idle-in-transaction and idle-session timeouts.
         */
        if idle_in_transaction_timeout_enabled {
            disable_timeout(IDLE_IN_TRANSACTION_SESSION_TIMEOUT, false);
            idle_in_transaction_timeout_enabled = false;
        }
        if idle_session_timeout_enabled {
            disable_timeout(IDLE_SESSION_TIMEOUT, false);
            idle_session_timeout_enabled = false;
        }

        /*
         * (5) disable async signal conditions again.
         */
        if InterruptPending {
            ProcessInterrupts();
        }
        DoingCommandRead = false;

        /*
         * (6) check for any other interesting events that happened while we
         * slept.
         */
        if ConfigReloadPending {
            ConfigReloadPending = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        /*
         * (7) process the command.  But ignore it if we're skipping till Sync.
         */
        if ignore_till_sync && firstchar != -1 /* EOF */ {
            continue;
        }

        match firstchar as u8 as char {
            'Q' /* PqMsg_Query */ => {
                let query_string: *const c_char;

                /* Set statement_timestamp() */
                SetCurrentStatementStartTimestamp();

                query_string = pq_getmsgstring(&mut input_message);
                pq_getmsgend(&mut input_message);

                if am_walsender {
                    if !exec_replication_command(query_string) {
                        exec_simple_query(query_string);
                    }
                } else {
                    exec_simple_query(query_string);
                }

                valgrind_report_error_query(query_string);

                send_ready_for_query = true;
            }
            'P' /* PqMsg_Parse */ => {
                let stmt_name: *const c_char;
                let query_string: *const c_char;
                let numParams: c_int;
                let mut paramTypes: *mut Oid = std::ptr::null_mut();

                forbidden_in_wal_sender(firstchar as c_char as c_int);

                /* Set statement_timestamp() */
                SetCurrentStatementStartTimestamp();

                stmt_name = pq_getmsgstring(&mut input_message);
                query_string = pq_getmsgstring(&mut input_message);
                numParams = pq_getmsgint(&mut input_message, 2);
                if numParams > 0 {
                    paramTypes = palloc_array(
                        std::mem::size_of::<Oid>(),
                        numParams as usize,
                    ) as *mut Oid;
                    for i in 0..numParams {
                        *paramTypes.offset(i as isize) = pq_getmsgint(&mut input_message, 4) as Oid;
                    }
                }
                pq_getmsgend(&mut input_message);

                exec_parse_message(query_string, stmt_name, paramTypes, numParams);

                valgrind_report_error_query(query_string);
            }
            'B' /* PqMsg_Bind */ => {
                forbidden_in_wal_sender(firstchar as c_char as c_int);

                /* Set statement_timestamp() */
                SetCurrentStatementStartTimestamp();

                /*
                 * this message is complex enough that it seems best to put
                 * the field extraction out-of-line
                 */
                exec_bind_message(&mut input_message);

                /* exec_bind_message does valgrind_report_error_query */
            }
            'E' /* PqMsg_Execute */ => {
                let portal_name: *const c_char;
                let max_rows: c_int;

                forbidden_in_wal_sender(firstchar as c_char as c_int);

                /* Set statement_timestamp() */
                SetCurrentStatementStartTimestamp();

                portal_name = pq_getmsgstring(&mut input_message);
                max_rows = pq_getmsgint(&mut input_message, 4);
                pq_getmsgend(&mut input_message);

                exec_execute_message(portal_name, max_rows as i64);

                /* exec_execute_message does valgrind_report_error_query */
            }
            'F' /* PqMsg_FunctionCall */ => {
                forbidden_in_wal_sender(firstchar as c_char as c_int);

                /* Set statement_timestamp() */
                SetCurrentStatementStartTimestamp();

                /* Report query to various monitoring facilities. */
                pgstat_report_activity(STATE_FASTPATH, std::ptr::null());
                set_ps_display(b"<FASTPATH>\0".as_ptr() as *const c_char);

                /* start an xact for this function invocation */
                start_xact_command();

                /*
                 * Note: we may at this point be inside an aborted transaction.
                 * HandleFunctionRequest() must check for it after doing so.
                 */

                /* switch back to message context */
                MemoryContextSwitchTo(MessageContext);

                HandleFunctionRequest(&mut input_message);

                /* commit the function-invocation transaction */
                finish_xact_command();

                valgrind_report_error_query(b"fastpath function call\0".as_ptr() as *const c_char);

                send_ready_for_query = true;
            }
            'C' /* PqMsg_Close */ => {
                let close_type: c_int;
                let close_target: *const c_char;

                forbidden_in_wal_sender(firstchar as c_char as c_int);

                close_type = pq_getmsgbyte(&mut input_message);
                close_target = pq_getmsgstring(&mut input_message);
                pq_getmsgend(&mut input_message);

                match close_type as u8 as char {
                    'S' => {
                        if *close_target != 0 {
                            DropPreparedStatement(close_target, false);
                        } else {
                            /* special-case the unnamed statement */
                            drop_unnamed_stmt();
                        }
                    }
                    'P' => {
                        let portal: Portal = GetPortalByName(close_target);
                        if PortalIsValid(portal) {
                            PortalDrop(portal, false);
                        }
                    }
                    _ => {
                        ereport!(ERROR, errmsg!("invalid CLOSE message subtype {}", close_type));
                        /* C also: errcode(ERRCODE_PROTOCOL_VIOLATION) */
                    }
                }

                if whereToSendOutput == DestRemote {
                    pq_putemptymessage(PqMsg_CloseComplete);
                }

                valgrind_report_error_query(b"CLOSE message\0".as_ptr() as *const c_char);
            }
            'D' /* PqMsg_Describe */ => {
                let describe_type: c_int;
                let describe_target: *const c_char;

                forbidden_in_wal_sender(firstchar as c_char as c_int);

                /* Set statement_timestamp() (needed for xact) */
                SetCurrentStatementStartTimestamp();

                describe_type = pq_getmsgbyte(&mut input_message);
                describe_target = pq_getmsgstring(&mut input_message);
                pq_getmsgend(&mut input_message);

                match describe_type as u8 as char {
                    'S' => {
                        exec_describe_statement_message(describe_target);
                    }
                    'P' => {
                        exec_describe_portal_message(describe_target);
                    }
                    _ => {
                        ereport!(ERROR, errmsg!("invalid DESCRIBE message subtype {}", describe_type));
                        /* C also: errcode(ERRCODE_PROTOCOL_VIOLATION) */
                    }
                }

                valgrind_report_error_query(b"DESCRIBE message\0".as_ptr() as *const c_char);
            }
            'H' /* PqMsg_Flush */ => {
                pq_getmsgend(&mut input_message);
                if whereToSendOutput == DestRemote {
                    pq_flush();
                }
            }
            'S' /* PqMsg_Sync */ => {
                pq_getmsgend(&mut input_message);

                /*
                 * If pipelining was used, we may be in an implicit transaction block.
                 */
                EndImplicitTransactionBlock();
                finish_xact_command();
                valgrind_report_error_query(b"SYNC message\0".as_ptr() as *const c_char);
                send_ready_for_query = true;
            }
            /*
             * PqMsg_Terminate means the frontend is closing down the socket.
             * EOF means unexpected loss of frontend connection.
             * Either way, perform normal shutdown.
             */
            '\0' /* EOF = -1, handled by the outer i32 */ => {
                /* for the cumulative statistics system */
                pgStatSessionEndCause = DISCONNECT_CLIENT_EOF;

                /* fall through */
                if whereToSendOutput == DestRemote {
                    whereToSendOutput = DestNone;
                }
                proc_exit(0);
            }
            'X' /* PqMsg_Terminate */ => {
                if whereToSendOutput == DestRemote {
                    whereToSendOutput = DestNone;
                }
                proc_exit(0);
            }
            'd' /* PqMsg_CopyData */
            | 'c' /* PqMsg_CopyDone */
            | 'f' /* PqMsg_CopyFail */ => {
                /*
                 * Accept but ignore these messages, per protocol spec.
                 */
            }
            _ => {
                ereport!(FATAL, errmsg!("invalid frontend message type {}", firstchar));
                /* C also: errcode(ERRCODE_PROTOCOL_VIOLATION) */
            }
        }

        // Handle EOF (-1) specially
        if firstchar == -1 {
            pgStatSessionEndCause = DISCONNECT_CLIENT_EOF;
            if whereToSendOutput == DestRemote {
                whereToSendOutput = DestNone;
            }
            proc_exit(0);
        }
    } /* end of input-reading loop */
}


// ---------------------------------------------------------------------------
// forbidden_in_wal_sender
//
// Throw an error if we're a WAL sender process.
// ---------------------------------------------------------------------------
unsafe fn forbidden_in_wal_sender(firstchar: c_int) {
    if am_walsender {
        if firstchar == b'F' as c_int {
            ereport!(ERROR, errmsg!("fastpath function calls not supported in a replication connection"));
            /* C also: errcode(ERRCODE_PROTOCOL_VIOLATION) */
        } else {
            ereport!(ERROR, errmsg!("extended query protocol not supported in a replication connection"));
            /* C also: errcode(ERRCODE_PROTOCOL_VIOLATION) */
        }
    }
}

// ---------------------------------------------------------------------------
// ResetUsage / ShowUsage
// ---------------------------------------------------------------------------

static mut Save_r: [u8; 256] = [0u8; 256]; /* struct rusage placeholder */
static mut Save_t: [u8; 16] = [0u8; 16];  /* struct timeval placeholder */

pub unsafe fn ResetUsage() {
    getrusage(RUSAGE_SELF, Save_r.as_mut_ptr() as *mut c_void);
    gettimeofday(Save_t.as_mut_ptr() as *mut c_void, std::ptr::null_mut());
}

pub unsafe fn ShowUsage(title: *const c_char) {
    let mut str_: StringInfoData = std::mem::zeroed();
    // struct timeval and rusage manipulation omitted -- TODO(pg-port): libc structs
    // The key logic:
    //   getrusage -> compute diffs from Save_r
    //   gettimeofday -> compute elapsed from Save_t
    //   build appendStringInfo messages
    //   ereport(LOG, ...)

    initStringInfo(&mut str_);

    appendStringInfoString(&mut str_, b"! system usage stats:\n\0".as_ptr() as *const c_char);

    /* TODO(pg-port): fill in rusage/timeval differences via libc bindings */

    /* remove trailing newline */
    if str_.len > 0 && *str_.data.offset((str_.len - 1) as isize) == b'\n' as c_char {
        str_.len -= 1;
        *str_.data.offset(str_.len as isize) = 0;
    }

    ereport!(LOG, errmsg!("{}: {}", CStr::from_ptr(title).to_string_lossy(), CStr::from_ptr(str_.data).to_string_lossy()));
    /* C also: errmsg_internal(title), errdetail_internal(str.data) */

    pfree(str_.data as *mut c_void);
}

// ---------------------------------------------------------------------------
// log_disconnections
//
// on_proc_exit handler to log end of session.
// ---------------------------------------------------------------------------
unsafe extern "C" fn log_disconnections(code: c_int, _arg: Datum) {
    let port: *mut Port = MyProcPort;
    let mut secs: i64 = 0;
    let mut usecs: c_int = 0;
    let msecs: i32;
    let hours: i64;
    let minutes: i64;
    let seconds: i64;

    TimestampDifference(MyStartTimestamp, GetCurrentTimestamp(), &mut secs, &mut usecs);
    msecs = usecs / 1000;

    let hours_v = secs / SECS_PER_HOUR;
    let secs_rem = secs % SECS_PER_HOUR;
    let minutes_v = secs_rem / SECS_PER_MINUTE;
    let seconds_v = secs_rem % SECS_PER_MINUTE;
    let _ = (hours_v, minutes_v, seconds_v, code);

    /* TODO(pg-port): access port->user_name, port->database_name, port->remote_host, port->remote_port via Port struct */
    ereport!(LOG, errmsg!("disconnection: session time: {}:{:02}:{:02}.{:03} user=.. database=.. host=..", secs / SECS_PER_HOUR, (secs % SECS_PER_HOUR) / SECS_PER_MINUTE, secs % SECS_PER_MINUTE, msecs));
    let _ = port; /* suppress unused warning */
}

// ---------------------------------------------------------------------------
// enable_statement_timeout / disable_statement_timeout
// ---------------------------------------------------------------------------
unsafe fn enable_statement_timeout() {
    /* must be within an xact */
    Assert!(xact_started);

    if StatementTimeout > 0
        && (StatementTimeout < TransactionTimeout || TransactionTimeout == 0)
    {
        if !get_timeout_active(STATEMENT_TIMEOUT) {
            enable_timeout_after(STATEMENT_TIMEOUT, StatementTimeout);
        }
    } else {
        if get_timeout_active(STATEMENT_TIMEOUT) {
            disable_timeout(STATEMENT_TIMEOUT, false);
        }
    }
}

unsafe fn disable_statement_timeout() {
    if get_timeout_active(STATEMENT_TIMEOUT) {
        disable_timeout(STATEMENT_TIMEOUT, false);
    }
}

