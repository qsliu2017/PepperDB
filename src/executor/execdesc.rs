//! executor/execdesc.h - plan and query descriptor for the executor.

use std::ffi::{c_char, c_int};

use crate::nodes::execnodes::{EState, PlanState, Snapshot};
use crate::nodes::nodes::CmdType;
use crate::nodes::params::ParamListInfo;
use crate::nodes::plannodes::PlannedStmt;
use crate::access::common::tupdesc::TupleDesc;
use crate::tcop::dest::DestReceiver;
use crate::utils::misc::queryenvironment::QueryEnvironment;
use crate::executor::instrument::Instrumentation;

/* ----------------
 *		query descriptor:
 *
 *	a QueryDesc encapsulates everything that the executor
 *	needs to execute the query.
 *
 *	For the convenience of SQL-language functions, we also support QueryDescs
 *	containing utility statements; these must not be passed to the executor
 *	however.
 * ---------------------
 */
#[repr(C)]
pub struct QueryDesc {
    /* These fields are provided by CreateQueryDesc */
    pub operation: CmdType,                  /* CMD_SELECT, CMD_UPDATE, etc. */
    pub plannedstmt: *mut PlannedStmt,       /* planner's output (could be utility, too) */
    pub sourceText: *const c_char,           /* source text of the query */
    pub snapshot: Snapshot,                  /* snapshot to use for query */
    pub crosscheck_snapshot: Snapshot,       /* crosscheck for RI update/delete */
    pub dest: *mut DestReceiver,             /* the destination for tuple output */
    pub params: ParamListInfo,               /* param values being passed in */
    pub queryEnv: *mut QueryEnvironment,     /* query environment passed in */
    pub instrument_options: c_int,           /* OR of InstrumentOption flags */

    /* These fields are set by ExecutorStart */
    pub tupDesc: TupleDesc,                  /* descriptor for result tuples */
    pub estate: *mut EState,                 /* executor's query-wide state */
    pub planstate: *mut PlanState,           /* tree of per-plan-node state */

    /* This field is set by ExecutePlan */
    pub already_executed: bool,              /* true if previously executed */

    /* This is always set NULL by the core system, but plugins can change it */
    pub totaltime: *mut Instrumentation,     /* total time spent in ExecutorRun */
}

/* in pquery.c */
pub unsafe fn CreateQueryDesc(
    plannedstmt: *mut PlannedStmt,
    sourceText: *const c_char,
    snapshot: Snapshot,
    crosscheck_snapshot: Snapshot,
    dest: *mut DestReceiver,
    params: ParamListInfo,
    queryEnv: *mut QueryEnvironment,
    instrument_options: c_int,
) -> *mut QueryDesc {
    crate::tcop::pquery::CreateQueryDesc(plannedstmt as _, sourceText as _, snapshot as _, crosscheck_snapshot as _, dest as _, params as _, queryEnv as _, instrument_options as _) as _
}

pub unsafe fn FreeQueryDesc(qdesc: *mut QueryDesc) {
    crate::tcop::pquery::FreeQueryDesc(qdesc as _)
}
