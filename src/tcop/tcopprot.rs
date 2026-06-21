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

pub unsafe fn pg_parse_query(query_string: *const c_char) -> *mut List {
    crate::tcop::postgres::pg_parse_query(query_string as _) as _
}

pub unsafe fn pg_rewrite_query(query: *mut Query) -> *mut List {
    crate::tcop::postgres::pg_rewrite_query(query as _) as _
}

pub unsafe fn pg_analyze_and_rewrite_fixedparams(
    parsetree: *mut RawStmt,
    query_string: *const c_char,
    paramTypes: *const Oid,
    numParams: c_int,
    queryEnv: *mut QueryEnvironment,
) -> *mut List {
    crate::tcop::postgres::pg_analyze_and_rewrite_fixedparams(
        parsetree as _,
        query_string as _,
        paramTypes as _,
        numParams as _,
        queryEnv as _,
    ) as _
}

pub unsafe fn pg_analyze_and_rewrite_varparams(
    parsetree: *mut RawStmt,
    query_string: *const c_char,
    paramTypes: *mut *mut Oid,
    numParams: *mut c_int,
    queryEnv: *mut QueryEnvironment,
) -> *mut List {
    crate::tcop::postgres::pg_analyze_and_rewrite_varparams(
        parsetree as _,
        query_string as _,
        paramTypes as _,
        numParams as _,
        queryEnv as _,
    ) as _
}

pub unsafe fn pg_analyze_and_rewrite_withcb(
    parsetree: *mut RawStmt,
    query_string: *const c_char,
    parserSetup: ParserSetupHook,
    parserSetupArg: *mut c_void,
    queryEnv: *mut QueryEnvironment,
) -> *mut List {
    crate::tcop::postgres::pg_analyze_and_rewrite_withcb(
        parsetree as _,
        query_string as _,
        core::mem::transmute(parserSetup),
        parserSetupArg as _,
        queryEnv as _,
    ) as _
}

pub unsafe fn pg_plan_query(
    querytree: *mut Query,
    query_string: *const c_char,
    cursorOptions: c_int,
    boundParams: ParamListInfo,
) -> *mut PlannedStmt {
    crate::tcop::postgres::pg_plan_query(
        querytree as _,
        query_string as _,
        cursorOptions as _,
        boundParams as _,
    ) as _
}

pub unsafe fn pg_plan_queries(
    querytrees: *mut List,
    query_string: *const c_char,
    cursorOptions: c_int,
    boundParams: ParamListInfo,
) -> *mut List {
    crate::tcop::postgres::pg_plan_queries(
        querytrees as _,
        query_string as _,
        cursorOptions as _,
        boundParams as _,
    ) as _
}

#[no_mangle]
pub unsafe extern "C" fn die(postgres_signal_arg: c_int) {
    crate::tcop::postgres::die(postgres_signal_arg as _)
}

/// pg_noreturn in C; modeled with `-> !`.
pub unsafe extern "C" fn quickdie(postgres_signal_arg: c_int) -> ! {
    crate::tcop::postgres::quickdie(postgres_signal_arg as _);
    unreachable!()
}

pub unsafe extern "C" fn StatementCancelHandler(postgres_signal_arg: c_int) {
    crate::tcop::postgres::StatementCancelHandler(postgres_signal_arg as _)
}

/// pg_noreturn in C; modeled with `-> !`.
pub unsafe extern "C" fn FloatExceptionHandler(postgres_signal_arg: c_int) -> ! {
    crate::tcop::postgres::FloatExceptionHandler(postgres_signal_arg as _);
    unreachable!()
}

pub unsafe fn HandleRecoveryConflictInterrupt(reason: ProcSignalReason) {
    crate::tcop::postgres::HandleRecoveryConflictInterrupt(reason as _)
}

pub unsafe fn ProcessClientReadInterrupt(blocked: bool) {
    crate::tcop::postgres::ProcessClientReadInterrupt(blocked as _)
}

pub unsafe fn ProcessClientWriteInterrupt(blocked: bool) {
    crate::tcop::postgres::ProcessClientWriteInterrupt(blocked as _)
}

pub unsafe fn process_postgres_switches(
    argc: c_int,
    argv: *mut *mut c_char,
    ctx: GucContext,
    dbname: *mut *const c_char,
) {
    crate::tcop::postgres::process_postgres_switches(
        argc as _,
        argv as _,
        ctx as _,
        dbname as _,
    )
}

/// pg_noreturn in C; modeled with `-> !`.
pub unsafe fn PostgresSingleUserMain(
    argc: c_int,
    argv: *mut *mut c_char,
    username: *const c_char,
) -> ! {
    crate::tcop::postgres::PostgresSingleUserMain(argc as _, argv as _, username as _);
    unreachable!()
}

/// pg_noreturn in C; modeled with `-> !`.
pub unsafe fn PostgresMain(dbname: *const c_char, username: *const c_char) -> ! {
    crate::tcop::postgres::PostgresMain(dbname as _, username as _);
    unreachable!()
}

pub unsafe fn ResetUsage() {
    crate::tcop::postgres::ResetUsage()
}

pub unsafe fn ShowUsage(title: *const c_char) {
    crate::tcop::postgres::ShowUsage(title as _)
}

pub unsafe fn check_log_duration(msec_str: *mut c_char, was_logged: bool) -> c_int {
    crate::tcop::postgres::check_log_duration(msec_str as _, was_logged as _) as _
}

pub unsafe fn set_debug_options(debug_flag: c_int, context: GucContext, source: GucSource) {
    crate::tcop::postgres::set_debug_options(debug_flag as _, context as _, source as _)
}

pub unsafe fn set_plan_disabling_options(
    arg: *const c_char,
    context: GucContext,
    source: GucSource,
) -> bool {
    crate::tcop::postgres::set_plan_disabling_options(arg as _, context as _, source as _) as _
}

pub unsafe fn get_stats_option_name(arg: *const c_char) -> *const c_char {
    crate::tcop::postgres::get_stats_option_name(arg as _) as _
}
