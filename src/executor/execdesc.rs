//! Translated from PostgreSQL src/include/executor/execdesc.h

use std::sync::Arc;
use crate::utils::rel::RelationData;

use crate::executor::instrument::InstrumentOption;
use crate::nodes::execnodes::{
    EState, Instrumentation, ParamListInfo, PlannedStmt, QueryEnvironment, Snapshot, TupleDesc,
};
use crate::nodes::nodes::CmdType;
use crate::tcop::dest::DestReceiver;

/// The live plan-state tree (PG `PlanState *`). In this port the concrete node
/// states are held in an enum (downcast-free dispatch); see execProcnode.rs.
pub use crate::backend::executor::execProcnode::PlanStateNode;

/// A QueryDesc encapsulates everything the executor needs to execute the query.
/// In-memory executor state (not a Node).
///
/// `'rel` is the lifetime of the open range-table relations the contained `EState`
/// borrows (relation-ownership-plan §1.2): the `Arc<RelationData>` owners live in
/// the command frame and outlive the QueryDesc's executor run.
#[allow(deprecated)]
pub struct QueryDesc<'rel> {
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
    pub tupDesc: Option<TupleDesc>,
    pub estate: Option<Box<EState<'rel>>>,
    pub planstate: Option<Box<PlanStateNode<'rel>>>,

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
) -> QueryDesc<'static> {
    unimplemented!()
}

pub fn FreeQueryDesc(_qdesc: QueryDesc<'_>) {
    unimplemented!()
}
