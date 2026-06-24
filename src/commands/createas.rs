//! Translated from PostgreSQL src/include/commands/createas.h

use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::params::ParamListInfo;
use crate::nodes::parsenodes::CreateTableAsStmt;
use crate::nodes::primnodes::IntoClause;
use crate::parser::parse_node::ParseState;
use crate::tcop::cmdtag::QueryCompletion;
use crate::tcop::dest::DestReceiver;
use crate::utils::queryenvironment::QueryEnvironment;

pub fn ExecCreateTableAs(
    _pstate: &mut ParseState,
    _stmt: &CreateTableAsStmt,
    _params: ParamListInfo,
    _queryEnv: &mut QueryEnvironment,
    _qc: &mut QueryCompletion,
) -> ObjectAddress {
    unimplemented!()
}

pub fn GetIntoRelEFlags(_intoClause: &IntoClause) -> i32 {
    unimplemented!()
}

// TODO(ptr): the concrete receiver type is chosen at runtime; revisit ownership.
pub fn CreateIntoRelDestReceiver(_intoClause: &IntoClause) -> Box<dyn DestReceiver> {
    unimplemented!()
}

pub fn CreateTableAsRelExists(_ctas: &CreateTableAsStmt) -> bool {
    unimplemented!()
}
