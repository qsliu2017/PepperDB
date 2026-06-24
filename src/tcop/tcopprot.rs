//! Translated from PostgreSQL src/include/tcop/tcopprot.h

use crate::postgres_ext::Oid;
use bitflags::bitflags;

// Process-global backend state (-> session/task-local later).
pub static mut whereToSendOutput: i32 = 0; // CommandDest
pub static mut debug_query_string: Option<String> = None;
pub static mut PostAuthDelay: i32 = 0;
pub static mut client_connection_check_interval: i32 = 0;

// GUC-configurable: statement-logging verbosity. Sequential ordinal -> enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogStmtLevel {
    LogstmtNone, // log no statements
    LogstmtDdl,  // log data definition statements
    LogstmtMod,  // log modification statements, plus DDL
    LogstmtAll,  // log all statements
}

pub static mut Log_disconnections: bool = false;
pub static mut log_statement: i32 = 0; // LogStmtLevel

bitflags! {
    // Flags for restrict_nonsystem_relation_kind value.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct RestrictRelkind: i32 {
        const VIEW          = 0x01;
        const FOREIGN_TABLE = 0x02;
    }
}

pub static mut restrict_nonsystem_relation_kind: i32 = 0;

// Forward refs for the function stubs; repointed in Phase 2.
#[deprecated(note = "TODO(struct-forward): repoint to crate::nodes::pg_list as Vec<T> in Phase 2")]
pub struct List; // TODO(struct-forward)
#[deprecated(note = "TODO(struct-forward): repoint to crate::nodes::parsenodes::Query in Phase 2")]
pub struct Query; // TODO(struct-forward)
#[deprecated(note = "TODO(struct-forward): repoint to crate::nodes::parsenodes::RawStmt in Phase 2")]
pub struct RawStmt; // TODO(struct-forward)
#[deprecated(note = "TODO(struct-forward): repoint to crate::nodes::plannodes::PlannedStmt in Phase 2")]
pub struct PlannedStmt; // TODO(struct-forward)
#[deprecated(note = "TODO(struct-forward): repoint to crate::nodes::params::ParamListInfo in Phase 2")]
pub struct ParamListInfo; // TODO(struct-forward)
#[deprecated(note = "TODO(struct-forward): repoint to crate::utils::queryenvironment::QueryEnvironment in Phase 2")]
pub struct QueryEnvironment; // TODO(struct-forward)
#[deprecated(note = "TODO(struct-forward): repoint to crate::nodes::params::ParserSetupHook in Phase 2")]
pub struct ParserSetupHook; // TODO(struct-forward)
#[deprecated(note = "TODO(struct-forward): repoint to crate::storage::procsignal::ProcSignalReason in Phase 2")]
pub struct ProcSignalReason; // TODO(struct-forward)
#[deprecated(note = "TODO(struct-forward): repoint to crate::utils::guc GucContext in Phase 2")]
pub struct GucContext; // TODO(struct-forward)
#[deprecated(note = "TODO(struct-forward): repoint to crate::utils::guc GucSource in Phase 2")]
pub struct GucSource; // TODO(struct-forward)

#[allow(deprecated)]
pub fn pg_parse_query(_query_string: &str) -> List {
    unimplemented!()
}

#[allow(deprecated)]
pub fn pg_rewrite_query(_query: &Query) -> List {
    unimplemented!()
}

#[allow(deprecated)]
pub fn pg_analyze_and_rewrite_fixedparams(
    _parsetree: &RawStmt,
    _query_string: &str,
    _param_types: &[Oid],
    _query_env: &QueryEnvironment,
) -> List {
    unimplemented!()
}

#[allow(deprecated)]
pub fn pg_analyze_and_rewrite_varparams(
    _parsetree: &RawStmt,
    _query_string: &str,
    _param_types: &mut Vec<Oid>,
    _query_env: &QueryEnvironment,
) -> List {
    unimplemented!()
}

#[allow(deprecated)]
pub fn pg_analyze_and_rewrite_withcb(
    _parsetree: &RawStmt,
    _query_string: &str,
    _parser_setup: ParserSetupHook,
    _query_env: &QueryEnvironment,
) -> List {
    unimplemented!()
}

#[allow(deprecated)]
pub fn pg_plan_query(
    _querytree: &Query,
    _query_string: &str,
    _cursor_options: i32,
    _bound_params: ParamListInfo,
) -> PlannedStmt {
    unimplemented!()
}

#[allow(deprecated)]
pub fn pg_plan_queries(
    _querytrees: &List,
    _query_string: &str,
    _cursor_options: i32,
    _bound_params: ParamListInfo,
) -> List {
    unimplemented!()
}

// Signal handlers; SIGNAL_ARGS -> i32 signo. Async coloring deferred.
pub fn die(_signo: i32) {
    unimplemented!()
}

pub fn quickdie(_signo: i32) -> ! {
    unimplemented!()
}

pub fn StatementCancelHandler(_signo: i32) {
    unimplemented!()
}

pub fn FloatExceptionHandler(_signo: i32) -> ! {
    unimplemented!()
}

#[allow(deprecated)]
pub fn HandleRecoveryConflictInterrupt(_reason: ProcSignalReason) {
    unimplemented!()
}

pub fn ProcessClientReadInterrupt(_blocked: bool) {
    unimplemented!()
}

pub fn ProcessClientWriteInterrupt(_blocked: bool) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn process_postgres_switches(_argv: &[String], _ctx: GucContext) -> Option<String> {
    unimplemented!()
}

pub fn PostgresSingleUserMain(_argv: &[String], _username: &str) -> ! {
    unimplemented!()
}

// Main backend loop entry; never returns.
pub fn PostgresMain(_dbname: &str, _username: &str) -> ! {
    unimplemented!()
}

pub fn ResetUsage() {
    unimplemented!()
}

pub fn ShowUsage(_title: &str) {
    unimplemented!()
}

// returns true if the statement should be logged; msec_str out-param folds into tuple.
pub fn check_log_duration(_was_logged: bool) -> (bool, String) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn set_debug_options(_debug_flag: i32, _context: GucContext, _source: GucSource) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn set_plan_disabling_options(_arg: &str, _context: GucContext, _source: GucSource) -> bool {
    unimplemented!()
}

pub fn get_stats_option_name(_arg: &str) -> Option<&'static str> {
    unimplemented!()
}
