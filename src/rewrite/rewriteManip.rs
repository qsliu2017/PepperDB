//! Translated from PostgreSQL src/include/rewrite/rewriteManip.h
//!
//! Querytree manipulation subroutines for the query rewriter.

use crate::access::attmap::AttrMap;
use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{Query, RangeTblEntry};
use crate::nodes::pathnodes::Relids;
use crate::nodes::primnodes::Var;
use crate::postgres_ext::Oid;

/// C: `replace_rte_variables_context`. The C `void *callback_arg` is folded into
/// the callback closure (function-mapping rule 6.3); kept as a field here.
pub struct ReplaceRteVariablesContext<'a> {
    /// callback function: given a Var and this context, returns its replacement.
    pub callback: ReplaceRteVariablesCallback<'a>,
    pub target_varno: i32,
    pub sublevels_up: i32,
    /// have we inserted a SubLink?
    pub inserted_sublink: bool,
}

pub type ReplaceRteVariablesCallback<'a> =
    &'a mut dyn FnMut(&Var, &mut ReplaceRteVariablesContext) -> Node;

/// C: `ReplaceVarsNoMatchOption`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplaceVarsNoMatchOption {
    /// throw error if no match
    REPORT_ERROR,
    /// change the Var's varno, nothing else
    CHANGE_VARNO,
    /// replace with a NULL Const
    SUBSTITUTE_NULL,
}

/// C: `ChangeVarNodes_context`.
pub struct ChangeVarNodesContext<'a> {
    pub rt_index: i32,
    pub new_index: i32,
    pub sublevels_up: i32,
    pub callback: ChangeVarNodesCallback<'a>,
}

pub type ChangeVarNodesCallback<'a> = &'a mut dyn FnMut(&mut Node, &mut ChangeVarNodesContext) -> bool;

pub fn adjust_relid_set(_relids: Relids, _oldrelid: i32, _newrelid: i32) -> Relids {
    unimplemented!()
}

/// Combines src range tables into dst. Mutates `dst_rtable`/`dst_perminfos`.
pub fn combine_range_tables(
    _dst_rtable: &mut Vec<Node>,
    _dst_perminfos: &mut Vec<Node>,
    _src_rtable: Vec<Node>,
    _src_perminfos: Vec<Node>,
) {
    unimplemented!()
}

pub fn offset_var_nodes(_node: &mut Node, _offset: i32, _sublevels_up: i32) {
    unimplemented!()
}

pub fn change_var_nodes(_node: &mut Node, _rt_index: i32, _new_index: i32, _sublevels_up: i32) {
    unimplemented!()
}

pub fn change_var_nodes_extended(
    _node: &mut Node,
    _rt_index: i32,
    _new_index: i32,
    _sublevels_up: i32,
    _callback: ChangeVarNodesCallback,
) {
    unimplemented!()
}

pub fn change_var_nodes_walk_expression(_node: &mut Node, _context: &mut ChangeVarNodesContext) -> bool {
    unimplemented!()
}

pub fn increment_var_sublevels_up(_node: &mut Node, _delta_sublevels_up: i32, _min_sublevels_up: i32) {
    unimplemented!()
}

pub fn increment_var_sublevels_up_rtable(
    _rtable: &mut [Node],
    _delta_sublevels_up: i32,
    _min_sublevels_up: i32,
) {
    unimplemented!()
}

pub fn range_table_entry_used(_node: &Node, _rt_index: i32, _sublevels_up: i32) -> bool {
    unimplemented!()
}

/// Returns the INSERT...SELECT subquery, plus a handle to the slot holding it.
pub fn get_insert_select_query(_parsetree: &mut Query) -> Option<&mut Query> {
    unimplemented!()
}

pub fn add_qual(_parsetree: &mut Query, _qual: Option<Node>) {
    unimplemented!()
}
pub fn add_inverted_qual(_parsetree: &mut Query, _qual: Option<Node>) {
    unimplemented!()
}

pub fn contain_aggs_of_level(_node: &Node, _levelsup: i32) -> bool {
    unimplemented!()
}
pub fn locate_agg_of_level(_node: &Node, _levelsup: i32) -> i32 {
    unimplemented!()
}
pub fn contain_windowfuncs(_node: &Node) -> bool {
    unimplemented!()
}
pub fn locate_windowfunc(_node: &Node) -> i32 {
    unimplemented!()
}
pub fn check_expr_has_sublink(_node: &Node) -> bool {
    unimplemented!()
}

pub fn add_nulling_relids(
    _node: &Node,
    _target_relids: &Bitmapset,
    _added_relids: &Bitmapset,
) -> Node {
    unimplemented!()
}
pub fn remove_nulling_relids(
    _node: &Node,
    _removable_relids: &Bitmapset,
    _except_relids: &Bitmapset,
) -> Node {
    unimplemented!()
}

/// Returns the rewritten node, plus the `outer_hasSubLinks` out-param.
pub fn replace_rte_variables(
    _node: &Node,
    _target_varno: i32,
    _sublevels_up: i32,
    _callback: ReplaceRteVariablesCallback,
) -> (Node, bool) {
    unimplemented!()
}

pub fn replace_rte_variables_mutator(_node: &Node, _context: &mut ReplaceRteVariablesContext) -> Node {
    unimplemented!()
}

/// Returns the mapped node, plus the `found_whole_row` out-param.
pub fn map_variable_attnos(
    _node: &Node,
    _target_varno: i32,
    _sublevels_up: i32,
    _attno_map: &AttrMap,
    _to_rowtype: Oid,
) -> (Node, bool) {
    unimplemented!()
}

pub fn replace_var_from_target_list(
    _var: &Var,
    _target_rte: &RangeTblEntry,
    _targetlist: &[Node],
    _result_relation: i32,
    _nomatch_option: ReplaceVarsNoMatchOption,
    _nomatch_varno: i32,
) -> Node {
    unimplemented!()
}

/// Returns the rewritten node, plus the `outer_hasSubLinks` out-param.
pub fn replace_vars_from_target_list(
    _node: &Node,
    _target_varno: i32,
    _sublevels_up: i32,
    _target_rte: &RangeTblEntry,
    _targetlist: &[Node],
    _result_relation: i32,
    _nomatch_option: ReplaceVarsNoMatchOption,
    _nomatch_varno: i32,
) -> (Node, bool) {
    unimplemented!()
}
