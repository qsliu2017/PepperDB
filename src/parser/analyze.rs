//! Translated from PostgreSQL src/include/parser/analyze.h

#![allow(clippy::boxed_local, reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1; real impl consumes params")]

use crate::nodes::lockoptions::{LockClauseStrength, LockWaitPolicy};
use crate::nodes::nodes::Node;
use crate::nodes::params::ParserSetupHook;
use crate::nodes::parsenodes::{CommonTableExpr, Query, RawStmt, ReturningClause, SortGroupClause};
use crate::nodes::queryjumble::JumbleState;
use crate::parser::parse_node::{ParseExprKind, ParseState};
use crate::postgres_ext::Oid;
use crate::utils::queryenvironment::QueryEnvironment;
use crate::utils::relcache::Relation;

/// Hook for plugins to get control at end of parse analysis.
pub type PostParseAnalyzeHookType =
    fn(pstate: &mut ParseState, query: &mut Query, jstate: Option<&mut JumbleState>);

pub static mut post_parse_analyze_hook: Option<PostParseAnalyzeHookType> = None;

pub fn parse_analyze_fixedparams(
    _parse_tree: &RawStmt,
    _source_text: &str,
    _param_types: &[Oid],
    _num_params: i32,
    _query_env: Option<&mut QueryEnvironment>,
) -> Box<Query> {
    unimplemented!()
}

pub fn parse_analyze_varparams(
    _parse_tree: &RawStmt,
    _source_text: &str,
    _param_types: &mut Vec<Oid>,
    _query_env: Option<&mut QueryEnvironment>,
) -> Box<Query> {
    unimplemented!()
}

pub fn parse_analyze_withcb(
    _parse_tree: &RawStmt,
    _source_text: &str,
    _parser_setup: ParserSetupHook,
    _query_env: Option<&mut QueryEnvironment>,
) -> Box<Query> {
    unimplemented!()
}

pub fn parse_sub_analyze(
    _parse_tree: Box<Node>,
    _parent_parse_state: &mut ParseState,
    _parent_cte: Option<&CommonTableExpr>,
    _locked_from_parent: bool,
    _resolve_unknowns: bool,
) -> Box<Query> {
    unimplemented!()
}

pub fn transformInsertRow(
    _pstate: &mut ParseState,
    _exprlist: Vec<Box<Node>>,
    _stmtcols: Vec<Box<Node>>,
    _icolumns: Vec<Box<Node>>,
    _attrnos: Vec<i32>,
    _strip_indirection: bool,
) -> Vec<Box<Node>> {
    unimplemented!()
}

pub fn transformUpdateTargetList(
    _pstate: &mut ParseState,
    _orig_tlist: Vec<Box<Node>>,
) -> Vec<Box<Node>> {
    unimplemented!()
}

pub fn transformReturningClause(
    _pstate: &mut ParseState,
    _qry: &mut Query,
    _returning_clause: &mut ReturningClause,
    _expr_kind: ParseExprKind,
) {
    unimplemented!()
}

pub fn transformTopLevelStmt(_pstate: &mut ParseState, _parse_tree: &RawStmt) -> Box<Query> {
    unimplemented!()
}

pub fn transformStmt(_pstate: &mut ParseState, _parse_tree: Box<Node>) -> Box<Query> {
    unimplemented!()
}

pub fn stmt_requires_parse_analysis(_parse_tree: &RawStmt) -> bool {
    unimplemented!()
}

pub fn analyze_requires_snapshot(_parse_tree: &RawStmt) -> bool {
    unimplemented!()
}

pub fn query_requires_rewrite_plan(_query: &Query) -> bool {
    unimplemented!()
}

pub fn LCS_asString(_strength: LockClauseStrength) -> &'static str {
    unimplemented!()
}

pub fn CheckSelectLocking(_qry: &Query, _strength: LockClauseStrength) {
    unimplemented!()
}

pub fn applyLockingClause(
    _qry: &mut Query,
    _rtindex: usize,
    _strength: LockClauseStrength,
    _wait_policy: LockWaitPolicy,
    _pushed_down: bool,
) {
    unimplemented!()
}

pub fn BuildOnConflictExcludedTargetlist(
    _target_rel: Relation,
    _excl_rel_index: usize,
) -> Vec<Box<Node>> {
    unimplemented!()
}

pub fn makeSortGroupClauseForSetOp(_rescoltype: Oid, _require_hash: bool) -> Box<SortGroupClause> {
    unimplemented!()
}
