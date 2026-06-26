//! Translated from PostgreSQL src/include/commands/prepare.h

#![allow(
    clippy::boxed_local,
    reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1; real impl consumes params"
)]

use crate::access::tupdesc::TupleDesc;
use crate::commands::explain_state::ExplainState;
use crate::datatype::timestamp::TimestampTz;
use crate::nodes::nodes::Node;
use crate::nodes::params::ParamListInfo;
use crate::nodes::parsenodes::{DeallocateStmt, ExecuteStmt, PrepareStmt};
use crate::nodes::primnodes::IntoClause;
use crate::parser::parse_node::ParseState;
use crate::tcop::cmdtag::QueryCompletion;
use crate::tcop::dest::DestReceiver;
use crate::utils::plancache::CachedPlanSource;

/// A prepared statement: a thin veneer over a plancache entry plus a name.
/// In-memory. C kept `stmt_name` first for dynahash; the Rust hashtable keys on
/// the name separately, so it is an owned String here.
pub struct PreparedStatement {
    pub stmt_name: String,
    pub plansource: Box<CachedPlanSource>, // the actual cached plan
    pub from_sql: bool,                    // prepared via SQL, not FE/BE protocol?
    pub prepare_time: TimestampTz,         // when the stmt was prepared
}

pub fn PrepareQuery(
    _pstate: &mut ParseState,
    _stmt: &PrepareStmt,
    _stmt_location: i32,
    _stmt_len: i32,
) {
    unimplemented!()
}

pub fn ExecuteQuery(
    _pstate: &mut ParseState,
    _stmt: &ExecuteStmt,
    _intoClause: Option<&IntoClause>,
    _params: ParamListInfo,
    _dest: &mut dyn DestReceiver,
    _qc: &mut QueryCompletion,
) {
    unimplemented!()
}

pub fn DeallocateQuery(_stmt: &DeallocateStmt) {
    unimplemented!()
}

pub fn ExplainExecuteQuery(
    _execstmt: &ExecuteStmt,
    _into: Option<&IntoClause>,
    _es: &mut ExplainState,
    _pstate: &mut ParseState,
    _params: ParamListInfo,
) {
    unimplemented!()
}

pub fn StorePreparedStatement(
    _stmt_name: &str,
    _plansource: Box<CachedPlanSource>,
    _from_sql: bool,
) {
    unimplemented!()
}

// throwError flag dropped: not-found is Option (caller decides via expect/?).
pub fn FetchPreparedStatement(_stmt_name: &str) -> Option<&'static PreparedStatement> {
    unimplemented!()
}

pub fn DropPreparedStatement(_stmt_name: &str, _showError: bool) {
    unimplemented!()
}

pub fn FetchPreparedStatementResultDesc(_stmt: &PreparedStatement) -> TupleDesc {
    unimplemented!()
}

pub fn FetchPreparedStatementTargetList(_stmt: &PreparedStatement) -> Vec<Box<Node>> {
    unimplemented!()
}

pub fn DropAllPreparedStatements() {
    unimplemented!()
}
