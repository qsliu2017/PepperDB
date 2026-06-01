//! tcop/tcopprot.h - prototypes for postgres.c.

use std::ffi::{c_char, c_int, c_void};

use crate::nodes::params::{ParamListInfo, ParserSetupHook};
use crate::nodes::parsenodes::{Query, RawStmt};
use crate::nodes::pg_list::List;
use crate::nodes::plannodes::PlannedStmt;
use crate::postgres_ext::Oid;
use crate::tcop::dest::CommandDest;
use crate::utils::misc::queryenvironment::QueryEnvironment;

// GucContext / GucSource come from utils/guc.h, not yet ported.  Modeled as the
// C-enum projection (pub type + c_int), matching project convention.
// TODO: dedup when utils/guc.h lands.
pub type GucContext = c_int;
// GucSource is also declared by utils/guc.h; a c_int alias already exists in
// other modules (e.g. stack_depth.rs, tableamapi.rs).  Re-stated locally here.
// TODO: dedup when utils/guc.h lands.
pub type GucSource = c_int;

// ProcSignalReason comes from storage/procsignal.h, not yet ported.
// TODO: dedup when storage/procsignal.h lands.
pub type ProcSignalReason = c_int;

// SIGNAL_ARGS expands to `int postgres_signal_arg`; signal handlers keep the C
// ABI so they can be installed as OS signal handlers.

extern "C" {
    pub static mut whereToSendOutput: CommandDest;
    pub static mut debug_query_string: *const c_char;
    pub static mut PostAuthDelay: c_int;
    pub static mut client_connection_check_interval: c_int;
}

/* GUC-configurable parameters */

pub type LogStmtLevel = c_int;
pub const LOGSTMT_NONE: LogStmtLevel = 0; /* log no statements */
pub const LOGSTMT_DDL: LogStmtLevel = 1; /* log data definition statements */
pub const LOGSTMT_MOD: LogStmtLevel = 2; /* log modification statements, plus DDL */
pub const LOGSTMT_ALL: LogStmtLevel = 3; /* log all statements */

extern "C" {
    pub static mut Log_disconnections: bool;
    pub static mut log_statement: c_int;
}

/* Flags for restrict_nonsystem_relation_kind value */
pub const RESTRICT_RELKIND_VIEW: c_int = 0x01;
pub const RESTRICT_RELKIND_FOREIGN_TABLE: c_int = 0x02;

extern "C" {
    pub static mut restrict_nonsystem_relation_kind: c_int;
}

pub unsafe fn pg_parse_query(_query_string: *const c_char) -> *mut List {
    unimplemented!()
}

pub unsafe fn pg_rewrite_query(_query: *mut Query) -> *mut List {
    unimplemented!()
}

pub unsafe fn pg_analyze_and_rewrite_fixedparams(
    _parsetree: *mut RawStmt,
    _query_string: *const c_char,
    _paramTypes: *const Oid,
    _numParams: c_int,
    _queryEnv: *mut QueryEnvironment,
) -> *mut List {
    unimplemented!()
}

pub unsafe fn pg_analyze_and_rewrite_varparams(
    _parsetree: *mut RawStmt,
    _query_string: *const c_char,
    _paramTypes: *mut *mut Oid,
    _numParams: *mut c_int,
    _queryEnv: *mut QueryEnvironment,
) -> *mut List {
    unimplemented!()
}

pub unsafe fn pg_analyze_and_rewrite_withcb(
    _parsetree: *mut RawStmt,
    _query_string: *const c_char,
    _parserSetup: ParserSetupHook,
    _parserSetupArg: *mut c_void,
    _queryEnv: *mut QueryEnvironment,
) -> *mut List {
    unimplemented!()
}

pub unsafe fn pg_plan_query(
    _querytree: *mut Query,
    _query_string: *const c_char,
    _cursorOptions: c_int,
    _boundParams: ParamListInfo,
) -> *mut PlannedStmt {
    unimplemented!()
}

pub unsafe fn pg_plan_queries(
    _querytrees: *mut List,
    _query_string: *const c_char,
    _cursorOptions: c_int,
    _boundParams: ParamListInfo,
) -> *mut List {
    unimplemented!()
}

pub unsafe extern "C" fn die(_postgres_signal_arg: c_int) {
    unimplemented!()
}

/// pg_noreturn in C; modeled with `-> !`.
pub unsafe extern "C" fn quickdie(_postgres_signal_arg: c_int) -> ! {
    unimplemented!()
}

pub unsafe extern "C" fn StatementCancelHandler(_postgres_signal_arg: c_int) {
    unimplemented!()
}

/// pg_noreturn in C; modeled with `-> !`.
pub unsafe extern "C" fn FloatExceptionHandler(_postgres_signal_arg: c_int) -> ! {
    unimplemented!()
}

pub unsafe fn HandleRecoveryConflictInterrupt(_reason: ProcSignalReason) {
    unimplemented!()
}

pub unsafe fn ProcessClientReadInterrupt(_blocked: bool) {
    unimplemented!()
}

pub unsafe fn ProcessClientWriteInterrupt(_blocked: bool) {
    unimplemented!()
}

pub unsafe fn process_postgres_switches(
    _argc: c_int,
    _argv: *mut *mut c_char,
    _ctx: GucContext,
    _dbname: *mut *const c_char,
) {
    unimplemented!()
}

/// pg_noreturn in C; modeled with `-> !`.
pub unsafe fn PostgresSingleUserMain(
    _argc: c_int,
    _argv: *mut *mut c_char,
    _username: *const c_char,
) -> ! {
    unimplemented!()
}

/// pg_noreturn in C; modeled with `-> !`.
pub unsafe fn PostgresMain(_dbname: *const c_char, _username: *const c_char) -> ! {
    unimplemented!()
}

pub unsafe fn ResetUsage() {
    unimplemented!()
}

pub unsafe fn ShowUsage(_title: *const c_char) {
    unimplemented!()
}

pub unsafe fn check_log_duration(_msec_str: *mut c_char, _was_logged: bool) -> c_int {
    unimplemented!()
}

pub unsafe fn set_debug_options(_debug_flag: c_int, _context: GucContext, _source: GucSource) {
    unimplemented!()
}

pub unsafe fn set_plan_disabling_options(
    _arg: *const c_char,
    _context: GucContext,
    _source: GucSource,
) -> bool {
    unimplemented!()
}

pub unsafe fn get_stats_option_name(_arg: *const c_char) -> *const c_char {
    unimplemented!()
}
