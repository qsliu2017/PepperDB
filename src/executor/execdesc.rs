//! Translated from PostgreSQL src/include/executor/execdesc.h

use crate::executor::instrument::InstrumentOption;
use crate::nodes::execnodes::{
    EState, Instrumentation, ParamListInfo, PlanState, PlannedStmt, QueryEnvironment, Snapshot,
    TupleDesc,
};
use crate::nodes::nodes::CmdType;
use crate::tcop::dest::DestReceiver;

/// A QueryDesc encapsulates everything the executor needs to execute the query.
/// In-memory executor state (not a Node).
#[allow(deprecated)]
pub struct QueryDesc {
    /* These fields are provided by CreateQueryDesc */
    pub operation: CmdType,
    pub plannedstmt: Option<Box<PlannedStmt>>,
    pub sourceText: String,
    pub snapshot: Option<Box<Snapshot>>,
    pub crosscheck_snapshot: Option<Box<Snapshot>>,
    pub dest: Option<Box<dyn DestReceiver>>,
    pub params: Option<Box<ParamListInfo>>,
    pub queryEnv: Option<Box<QueryEnvironment>>,
    pub instrument_options: InstrumentOption,

    /* These fields are set by ExecutorStart */
    pub tupDesc: Option<Box<TupleDesc>>,
    pub estate: Option<Box<EState>>,
    pub planstate: Option<Box<PlanState>>,

    /* This field is set by ExecutePlan */
    pub already_executed: bool,

    /* NULL by core; plugins can change it */
    pub totaltime: Option<Box<Instrumentation>>,
}

#[allow(deprecated)]
pub fn CreateQueryDesc(
    _plannedstmt: Option<Box<PlannedStmt>>,
    _sourceText: String,
    _snapshot: Option<Box<Snapshot>>,
    _crosscheck_snapshot: Option<Box<Snapshot>>,
    _dest: Option<Box<dyn DestReceiver>>,
    _params: Option<Box<ParamListInfo>>,
    _queryEnv: Option<Box<QueryEnvironment>>,
    _instrument_options: InstrumentOption,
) -> QueryDesc {
    unimplemented!()
}

pub fn FreeQueryDesc(_qdesc: QueryDesc) {
    unimplemented!()
}
