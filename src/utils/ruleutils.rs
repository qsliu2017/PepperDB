//! Translated from PostgreSQL src/include/utils/ruleutils.h
//! Declarations for ruleutils.c (deparse/pg_get_* functions).

use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::Query;
use crate::nodes::plannodes::{Plan, PlannedStmt};
use crate::postgres_ext::Oid;
use bitflags::bitflags;

bitflags! {
    /// Flags for `pg_get_indexdef_columns_extended()`. GOOD single-bit set (u16).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct RuleIndexdef: u16 {
        const PRETTY    = 0x01;
        const KEYS_ONLY = 0x02; // ignore included attributes
    }
}

pub fn pg_get_indexdef_string(_indexrelid: Oid) -> String {
    unimplemented!()
}
pub fn pg_get_indexdef_columns(_indexrelid: Oid, _pretty: bool) -> String {
    unimplemented!()
}
pub fn pg_get_indexdef_columns_extended(_indexrelid: Oid, _flags: RuleIndexdef) -> String {
    unimplemented!()
}
pub fn pg_get_querydef(_query: &Query, _pretty: bool) -> String {
    unimplemented!()
}

pub fn pg_get_partkeydef_columns(_relid: Oid, _pretty: bool) -> String {
    unimplemented!()
}
pub fn pg_get_partconstrdef_string(_partition_id: Oid, _aliasname: &str) -> String {
    unimplemented!()
}

pub fn pg_get_constraintdef_command(_constraint_id: Oid) -> String {
    unimplemented!()
}
pub fn deparse_expression(
    _expr: &Node,
    _dpcontext: &[Node],
    _force_prefix: bool,
    _show_implicit: bool,
) -> String {
    unimplemented!()
}
pub fn deparse_context_for(_aliasname: &str, _relid: Oid) -> Vec<Node> {
    unimplemented!()
}
pub fn deparse_context_for_plan_tree(_pstmt: &PlannedStmt, _rtable_names: &[Node]) -> Vec<Node> {
    unimplemented!()
}
pub fn set_deparse_context_plan(
    _dpcontext: &[Node],
    _plan: &Plan,
    _ancestors: &[Node],
) -> Vec<Node> {
    unimplemented!()
}
pub fn select_rtable_names_for_explain(_rtable: &[Node], _rels_used: &Bitmapset) -> Vec<Node> {
    unimplemented!()
}
pub fn get_window_frame_options_for_explain(
    _frame_options: i32,
    _start_offset: Option<&Node>,
    _end_offset: Option<&Node>,
    _dpcontext: &[Node],
    _force_prefix: bool,
) -> String {
    unimplemented!()
}
pub fn generate_collation_name(_collid: Oid) -> String {
    unimplemented!()
}
pub fn generate_opclass_name(_opclass: Oid) -> String {
    unimplemented!()
}
pub fn get_range_partbound_string(_bound_datums: &[Node]) -> String {
    unimplemented!()
}

pub fn pg_get_statisticsobjdef_string(_statextid: Oid) -> String {
    unimplemented!()
}
