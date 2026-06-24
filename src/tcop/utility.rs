//! Translated from PostgreSQL src/include/tcop/utility.h
//! Prototypes for utility.c.

use bitflags::bitflags;

use crate::nodes::nodes::Node;
use crate::nodes::params::ParamListInfo;
use crate::nodes::parsenodes::Query;
use crate::nodes::plannodes::PlannedStmt;
use crate::tcop::cmdtag::{GetCommandTagName, QueryCompletion};
use crate::tcop::cmdtaglist::CommandTag;
use crate::tcop::dest::DestReceiver;
use crate::tcop::tcopprot::LogStmtLevel;
use crate::access::tupdesc::TupleDesc;
use crate::postgres_ext::Oid;
use crate::utils::queryenvironment::QueryEnvironment;

/// Context in which a utility statement is being processed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessUtilityContext {
    Toplevel,         // toplevel interactive command
    Query,            // a complete query, but not toplevel
    QueryNonatomic,   // a complete query, nonatomic execution context
    Subcommand,       // a portion of a query
}

/// Info needed when recursing from ALTER TABLE. In-memory.
pub struct AlterTableUtilityContext {
    pub pstmt: Box<PlannedStmt>,       // PlannedStmt for outer ALTER TABLE command
    pub query_string: String,          // its query string
    pub relid: Oid,                    // OID of ALTER's target table
    pub params: ParamListInfo,         // any parameters available to ALTER TABLE
    pub query_env: Box<QueryEnvironment>, // execution environment for ALTER TABLE
}

bitflags! {
    /// Describes the extent to which a command is read-only. Composite
    /// `STRICTLY_READ_ONLY`. Per bitflags-port.md appendix A (GOOD: composite).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct CommandReadOnly: i32 {
        const OK_IN_READ_ONLY_TXN = 0x0001;
        const OK_IN_PARALLEL_MODE = 0x0002;
        const OK_IN_RECOVERY = 0x0004;
    }
}

impl CommandReadOnly {
    /// `COMMAND_IS_STRICTLY_READ_ONLY` = OK_IN_READ_ONLY_TXN | OK_IN_RECOVERY |
    /// OK_IN_PARALLEL_MODE.
    pub const IS_STRICTLY_READ_ONLY: Self = Self::all();
    /// `COMMAND_IS_NOT_READ_ONLY` = 0.
    pub const IS_NOT_READ_ONLY: Self = Self::empty();
}

/// Hook for plugins to get control in ProcessUtility(). C: a fn pointer; per
/// function-mapping a runtime-pluggable hook -> fn-ptr (not `dyn`).
pub type ProcessUtilityHookType = fn(
    pstmt: &PlannedStmt,
    query_string: &str,
    read_only_tree: bool,
    context: ProcessUtilityContext,
    params: ParamListInfo,
    query_env: Option<&mut QueryEnvironment>,
    dest: &mut dyn DestReceiver,
    qc: Option<&mut QueryCompletion>,
);

pub static mut ProcessUtility_hook: Option<ProcessUtilityHookType> = None;

pub fn ProcessUtility(
    pstmt: &PlannedStmt,
    query_string: &str,
    read_only_tree: bool,
    context: ProcessUtilityContext,
    params: ParamListInfo,
    query_env: Option<&mut QueryEnvironment>,
    dest: &mut dyn DestReceiver,
    qc: Option<&mut QueryCompletion>,
) {
    unimplemented!()
}

pub fn standard_ProcessUtility(
    pstmt: &PlannedStmt,
    query_string: &str,
    read_only_tree: bool,
    context: ProcessUtilityContext,
    params: ParamListInfo,
    query_env: Option<&mut QueryEnvironment>,
    dest: &mut dyn DestReceiver,
    qc: Option<&mut QueryCompletion>,
) {
    unimplemented!()
}

pub fn ProcessUtilityForAlterTable(stmt: &Node, context: &mut AlterTableUtilityContext) {
    unimplemented!()
}

pub fn UtilityReturnsTuples(parsetree: &Node) -> bool {
    unimplemented!()
}

pub fn UtilityTupleDescriptor(parsetree: &Node) -> TupleDesc {
    unimplemented!()
}

/// C returns NULL when the parsetree contains no query; folds to `Option`.
pub fn UtilityContainsQuery(parsetree: &Node) -> Option<Box<Query>> {
    unimplemented!()
}

pub fn CreateCommandTag(parsetree: &Node) -> CommandTag {
    unimplemented!()
}

/// Inline in the header: name for the command tag of a parse tree.
pub fn CreateCommandName(parsetree: &Node) -> &'static str {
    GetCommandTagName(CreateCommandTag(parsetree))
}

pub fn GetCommandLogLevel(parsetree: &Node) -> LogStmtLevel {
    unimplemented!()
}

pub fn CommandIsReadOnly(pstmt: &PlannedStmt) -> bool {
    unimplemented!()
}
