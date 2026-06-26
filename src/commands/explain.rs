//! Translated from PostgreSQL src/include/commands/explain.h

#![allow(
    clippy::boxed_local,
    reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1; real impl consumes params"
)]

use crate::access::tupdesc::TupleDesc;
use crate::commands::explain_state::ExplainState;
use crate::executor::execdesc::QueryDesc;
use crate::executor::instrument::BufferUsage;
use crate::nodes::memnodes::MemoryContextCounters;
use crate::nodes::nodes::Node;
use crate::nodes::params::ParamListInfo;
use crate::nodes::parsenodes::{ExplainStmt, Query};
use crate::nodes::plannodes::PlannedStmt;
use crate::nodes::primnodes::IntoClause;
use crate::parser::parse_node::ParseState;
use crate::portability::instr_time::InstrTime;
use crate::postgres_ext::Oid;
use crate::tcop::dest::DestReceiver;
use crate::utils::queryenvironment::QueryEnvironment;

// Plugin hooks (C function pointers, NULL by default) -> Option<fn ...>. As
// global hook state they become session/task state in the single-process port;
// the type aliases are the porting target. TODO(ptr): hook registration.

/// Hook for plugins to get control in ExplainOneQuery().
pub type ExplainOneQueryHook = fn(
    query: &mut Query,
    cursorOptions: i32,
    into: Option<&mut IntoClause>,
    es: &mut ExplainState,
    queryString: &str,
    params: ParamListInfo,
    queryEnv: Option<&mut QueryEnvironment>,
);
pub static mut ExplainOneQuery_hook: Option<ExplainOneQueryHook> = None;

/// Hook for EXPLAIN plugins to print extra information for each plan.
pub type ExplainPerPlanHook = fn(
    plannedstmt: &mut PlannedStmt,
    into: Option<&mut IntoClause>,
    es: &mut ExplainState,
    queryString: &str,
    params: ParamListInfo,
    queryEnv: Option<&mut QueryEnvironment>,
);
pub static mut explain_per_plan_hook: Option<ExplainPerPlanHook> = None;

/// Hook for EXPLAIN plugins to print extra fields on individual plan nodes.
/// `PlanState` lives in nodes::execnodes (level 5); use Node placeholder.
pub type ExplainPerNodeHook = fn(
    planstate: &mut Node,
    ancestors: &[Box<Node>],
    relationship: &str,
    plan_name: &str,
    es: &mut ExplainState,
);
pub static mut explain_per_node_hook: Option<ExplainPerNodeHook> = None;

/// Hook for plugins to get control in explain_get_index_name().
pub type ExplainGetIndexNameHook = fn(indexId: Oid) -> Option<String>;
pub static mut explain_get_index_name_hook: Option<ExplainGetIndexNameHook> = None;

pub fn ExplainQuery(
    _pstate: &mut ParseState,
    _stmt: &mut ExplainStmt,
    _params: ParamListInfo,
    _dest: &mut dyn DestReceiver,
) {
    unimplemented!()
}

pub fn standard_ExplainOneQuery(
    _query: &mut Query,
    _cursorOptions: i32,
    _into: Option<&mut IntoClause>,
    _es: &mut ExplainState,
    _queryString: &str,
    _params: ParamListInfo,
    _queryEnv: Option<&mut QueryEnvironment>,
) {
    unimplemented!()
}

pub fn ExplainResultDesc(_stmt: &mut ExplainStmt) -> TupleDesc {
    unimplemented!()
}

pub fn ExplainOneUtility(
    _utilityStmt: &mut Node,
    _into: Option<&mut IntoClause>,
    _es: &mut ExplainState,
    _pstate: &mut ParseState,
    _params: ParamListInfo,
) {
    unimplemented!()
}

pub fn ExplainOnePlan(
    _plannedstmt: &mut PlannedStmt,
    _into: Option<&mut IntoClause>,
    _es: &mut ExplainState,
    _queryString: &str,
    _params: ParamListInfo,
    _queryEnv: Option<&mut QueryEnvironment>,
    _planduration: Option<&InstrTime>,
    _bufusage: Option<&BufferUsage>,
    _mem_counters: Option<&MemoryContextCounters>,
) {
    unimplemented!()
}

pub fn ExplainPrintPlan(_es: &mut ExplainState, _queryDesc: &mut QueryDesc) {
    unimplemented!()
}

pub fn ExplainPrintTriggers(_es: &mut ExplainState, _queryDesc: &mut QueryDesc) {
    unimplemented!()
}

pub fn ExplainPrintJITSummary(_es: &mut ExplainState, _queryDesc: &mut QueryDesc) {
    unimplemented!()
}

pub fn ExplainQueryText(_es: &mut ExplainState, _queryDesc: &mut QueryDesc) {
    unimplemented!()
}

pub fn ExplainQueryParameters(_es: &mut ExplainState, _params: ParamListInfo, _maxlen: i32) {
    unimplemented!()
}
