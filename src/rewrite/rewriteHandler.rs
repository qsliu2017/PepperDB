//! Translated from PostgreSQL src/include/rewrite/rewriteHandler.h
//! External interface to query rewriter.

use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::nodes::{CmdType, Node};
use crate::nodes::parsenodes::Query;
use crate::postgres_ext::Oid;
use crate::utils::rel::RelationData;

/// PG `QueryRewrite`.
pub use crate::backend::rewrite::rewriteHandler::query_rewrite as QueryRewrite;

pub fn acquire_rewrite_locks(
    _parsetree: &mut Query,
    _for_execute: bool,
    _for_update_pushed_down: bool,
) {
    unimplemented!()
}

pub fn build_column_default(_rel: &RelationData, _attrno: i32) -> Option<Node> {
    unimplemented!()
}

pub fn get_view_query(_view: &RelationData) -> Box<Query> {
    unimplemented!()
}

pub fn view_has_instead_trigger(
    _view: &RelationData,
    _event: CmdType,
    _merge_action_list: Vec<Node>,
) -> bool {
    unimplemented!()
}

/// C returns a `const char *` reason, NULL if auto-updatable -> Option<String>.
pub fn view_query_is_auto_updatable(_viewquery: &Query, _check_cols: bool) -> Option<String> {
    unimplemented!()
}

pub fn relation_is_updatable(
    _reloid: Oid,
    _outer_reloids: Vec<Oid>,
    _include_triggers: bool,
    _include_cols: Option<Bitmapset>,
) -> i32 {
    unimplemented!()
}

pub fn error_view_not_updatable(
    _view: &RelationData,
    _command: CmdType,
    _merge_action_list: Vec<Node>,
    _detail: Option<&str>,
) {
    unimplemented!()
}

pub fn expand_generated_columns_in_expr(
    _node: Option<Node>,
    _rel: &RelationData,
    _rt_index: i32,
) -> Option<Node> {
    unimplemented!()
}

pub fn build_generation_expression(_rel: &RelationData, _attrno: i32) -> Option<Node> {
    unimplemented!()
}
