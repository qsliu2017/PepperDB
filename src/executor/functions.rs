//! Translated from PostgreSQL src/include/executor/functions.h

use crate::fmgr::FunctionCallInfo;
use crate::nodes::execnodes::{HeapTuple, TupleDesc};
use crate::nodes::nodes::Node;
use crate::parser::parse_node::ParseState;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::tcop::dest::DestReceiver;

/// Parser-callback state for resolving parameter references while parsing a SQL
/// function body. Separate from SQLFunctionCache. In-memory.
pub struct SQLFunctionParseInfo {
    pub fname: String,
    pub nargs: i32,
    pub argtypes: Vec<Oid>,
    /// names of input args; entries may be None for unnamed args.
    pub argnames: Option<Vec<Option<String>>>,
    pub collation: Oid,
}

pub type SQLFunctionParseInfoPtr = Box<SQLFunctionParseInfo>;

pub fn fmgr_sql(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!()
}

#[allow(deprecated)]
pub fn prepare_sql_fn_parse_info(
    _procedureTuple: HeapTuple,
    _call_expr: Option<Node>,
    _inputCollation: Oid,
) -> SQLFunctionParseInfoPtr {
    unimplemented!()
}

pub fn sql_fn_parser_setup(_pstate: &mut ParseState, _pinfo: &SQLFunctionParseInfo) {
    unimplemented!()
}

pub fn check_sql_fn_statements(_queryTreeLists: Vec<Node>) {
    unimplemented!()
}

pub fn check_sql_fn_retval(
    _queryTreeLists: Vec<Node>,
    _rettype: Oid,
    _rettupdesc: Option<Box<TupleDesc>>,
    _prokind: u8,
    _insertDroppedCols: bool,
) -> bool {
    unimplemented!()
}

pub fn CreateSQLFunctionDestReceiver() -> Box<dyn DestReceiver> {
    unimplemented!()
}
