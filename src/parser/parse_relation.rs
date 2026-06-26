//! Translated from PostgreSQL src/include/parser/parse_relation.h

#![allow(clippy::boxed_local, reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1; real impl consumes params")]

use crate::c::NameData;
use crate::nodes::nodes::{JoinType, Node};
use crate::nodes::parsenodes::{
    CommonTableExpr, Query, RTEPermissionInfo, RangeFunction, RangeTblEntry,
};
use crate::nodes::primnodes::{
    Alias, RangeVar, TableFunc, Var, VarReturningType,
};
use crate::parser::parse_node::{ParseNamespaceColumn, ParseNamespaceItem, ParseState};
use crate::postgres_ext::Oid;
use crate::utils::relcache::Relation;

/// `sublevels_up` out-param folded in -> returns `(item, sublevels_up)`.
pub fn refnameNamespaceItem(
    _pstate: &mut ParseState,
    _schemaname: Option<&str>,
    _refname: &str,
    _location: i32,
) -> (Option<Box<ParseNamespaceItem>>, i32) {
    unimplemented!()
}

/// `ctelevelsup` out-param folded in -> returns `(cte, ctelevelsup)`.
pub fn scanNameSpaceForCTE(
    _pstate: &mut ParseState,
    _refname: &str,
) -> (Option<Box<CommonTableExpr>>, usize) {
    unimplemented!()
}

pub fn scanNameSpaceForENR(_pstate: &mut ParseState, _refname: &str) -> bool {
    unimplemented!()
}

pub fn checkNameSpaceConflicts(
    _pstate: &mut ParseState,
    _namespace1: &[Box<Node>],
    _namespace2: &[Box<Node>],
) {
    unimplemented!()
}

pub fn GetNSItemByRangeTablePosn(
    _pstate: &mut ParseState,
    _varno: i32,
    _sublevels_up: i32,
) -> Box<ParseNamespaceItem> {
    unimplemented!()
}

pub fn GetRTEByRangeTablePosn(
    _pstate: &mut ParseState,
    _varno: i32,
    _sublevels_up: i32,
) -> Box<RangeTblEntry> {
    unimplemented!()
}

pub fn GetCTEForRTE(
    _pstate: &mut ParseState,
    _rte: &RangeTblEntry,
    _rtelevelsup: i32,
) -> Box<CommonTableExpr> {
    unimplemented!()
}

pub fn scanNSItemForColumn(
    _pstate: &mut ParseState,
    _nsitem: &ParseNamespaceItem,
    _sublevels_up: i32,
    _colname: &str,
    _location: i32,
) -> Option<Box<Node>> {
    unimplemented!()
}

pub fn colNameToVar(
    _pstate: &mut ParseState,
    _colname: &str,
    _localonly: bool,
    _location: i32,
) -> Option<Box<Node>> {
    unimplemented!()
}

pub fn markNullableIfNeeded(_pstate: &mut ParseState, _var: &mut Var) {
    unimplemented!()
}

pub fn markVarForSelectPriv(_pstate: &mut ParseState, _var: &mut Var) {
    unimplemented!()
}

pub fn parserOpenTable(
    _pstate: &mut ParseState,
    _relation: &RangeVar,
    _lockmode: i32,
) -> Relation {
    unimplemented!()
}

pub fn addRangeTableEntry(
    _pstate: &mut ParseState,
    _relation: &mut RangeVar,
    _alias: Option<&Alias>,
    _inh: bool,
    _in_from_cl: bool,
) -> Box<ParseNamespaceItem> {
    unimplemented!()
}

pub fn addRangeTableEntryForRelation(
    _pstate: &mut ParseState,
    _rel: Relation,
    _lockmode: i32,
    _alias: Option<&Alias>,
    _inh: bool,
    _in_from_cl: bool,
) -> Box<ParseNamespaceItem> {
    unimplemented!()
}

pub fn addRangeTableEntryForSubquery(
    _pstate: &mut ParseState,
    _subquery: Box<Query>,
    _alias: Option<&Alias>,
    _lateral: bool,
    _in_from_cl: bool,
) -> Box<ParseNamespaceItem> {
    unimplemented!()
}

pub fn addRangeTableEntryForFunction(
    _pstate: &mut ParseState,
    _funcnames: Vec<Box<Node>>,
    _funcexprs: Vec<Box<Node>>,
    _coldeflists: Vec<Box<Node>>,
    _rangefunc: &RangeFunction,
    _lateral: bool,
    _in_from_cl: bool,
) -> Box<ParseNamespaceItem> {
    unimplemented!()
}

pub fn addRangeTableEntryForValues(
    _pstate: &mut ParseState,
    _exprs: Vec<Box<Node>>,
    _coltypes: Vec<Oid>,
    _coltypmods: Vec<i32>,
    _colcollations: Vec<Oid>,
    _alias: Option<&Alias>,
    _lateral: bool,
    _in_from_cl: bool,
) -> Box<ParseNamespaceItem> {
    unimplemented!()
}

pub fn addRangeTableEntryForTableFunc(
    _pstate: &mut ParseState,
    _tf: &mut TableFunc,
    _alias: Option<&Alias>,
    _lateral: bool,
    _in_from_cl: bool,
) -> Box<ParseNamespaceItem> {
    unimplemented!()
}

pub fn addRangeTableEntryForJoin(
    _pstate: &mut ParseState,
    _colnames: Vec<Box<Node>>,
    _nscolumns: &mut [ParseNamespaceColumn],
    _jointype: JoinType,
    _nummergedcols: i32,
    _aliasvars: Vec<Box<Node>>,
    _leftcols: Vec<i32>,
    _rightcols: Vec<i32>,
    _join_using_alias: Option<&Alias>,
    _alias: Option<&Alias>,
    _in_from_cl: bool,
) -> Box<ParseNamespaceItem> {
    unimplemented!()
}

pub fn addRangeTableEntryForCTE(
    _pstate: &mut ParseState,
    _cte: &mut CommonTableExpr,
    _levelsup: usize,
    _rv: &RangeVar,
    _in_from_cl: bool,
) -> Box<ParseNamespaceItem> {
    unimplemented!()
}

pub fn addRangeTableEntryForENR(
    _pstate: &mut ParseState,
    _rv: &RangeVar,
    _in_from_cl: bool,
) -> Box<ParseNamespaceItem> {
    unimplemented!()
}

pub fn addRangeTableEntryForGroup(
    _pstate: &mut ParseState,
    _group_clauses: Vec<Box<Node>>,
) -> Box<ParseNamespaceItem> {
    unimplemented!()
}

pub fn addRTEPermissionInfo(
    _rteperminfos: &mut Vec<Box<RTEPermissionInfo>>,
    _rte: &mut RangeTblEntry,
) -> Box<RTEPermissionInfo> {
    unimplemented!()
}

pub fn getRTEPermissionInfo(
    _rteperminfos: &[Box<RTEPermissionInfo>],
    _rte: &RangeTblEntry,
) -> Box<RTEPermissionInfo> {
    unimplemented!()
}

pub fn isLockedRefname(_pstate: &mut ParseState, _refname: &str) -> bool {
    unimplemented!()
}

pub fn addNSItemToQuery(
    _pstate: &mut ParseState,
    _nsitem: Box<ParseNamespaceItem>,
    _add_to_join_list: bool,
    _add_to_rel_name_space: bool,
    _add_to_var_name_space: bool,
) {
    unimplemented!()
}

#[deprecated(note = "TODO(panic): migrate to Result + ?")]
pub fn errorMissingRTE(_pstate: &mut ParseState, _relation: &RangeVar) -> ! {
    unimplemented!()
}

#[deprecated(note = "TODO(panic): migrate to Result + ?")]
pub fn errorMissingColumn(
    _pstate: &mut ParseState,
    _relname: Option<&str>,
    _colname: &str,
    _location: i32,
) -> ! {
    unimplemented!()
}

/// `colnames`/`colvars` out-params -> returned tuple of lists.
pub fn expandRTE(
    _rte: &RangeTblEntry,
    _rtindex: i32,
    _sublevels_up: i32,
    _returning_type: VarReturningType,
    _location: i32,
    _include_dropped: bool,
) -> (Vec<Box<Node>>, Vec<Box<Node>>) {
    unimplemented!()
}

/// `colnames` out-param folded into the return tuple.
pub fn expandNSItemVars(
    _pstate: &mut ParseState,
    _nsitem: &ParseNamespaceItem,
    _sublevels_up: i32,
    _location: i32,
) -> (Vec<Box<Node>>, Vec<Box<Node>>) {
    unimplemented!()
}

pub fn expandNSItemAttrs(
    _pstate: &mut ParseState,
    _nsitem: &ParseNamespaceItem,
    _sublevels_up: i32,
    _require_col_privs: bool,
    _location: i32,
) -> Vec<Box<Node>> {
    unimplemented!()
}

pub fn attnameAttNum(_rd: Relation, _attname: &str, _sys_col_ok: bool) -> i32 {
    unimplemented!()
}

pub fn attnumAttName(_rd: Relation, _attid: i32) -> Option<&'static NameData> {
    unimplemented!()
}

pub fn attnumTypeId(_rd: Relation, _attid: i32) -> Oid {
    unimplemented!()
}

pub fn attnumCollationId(_rd: Relation, _attid: i32) -> Oid {
    unimplemented!()
}

pub fn isQueryUsingTempRelation(_query: &Query) -> bool {
    unimplemented!()
}
