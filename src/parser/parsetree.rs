//! Translated from PostgreSQL src/include/parser/parsetree.h
//! Routines to access various components and subcomponents of parse trees.

use crate::access::attnum::AttrNumber;
use crate::c::Index;
use crate::nodes::parsenodes::{Query, RangeTblEntry, RowMarkClause};
use crate::nodes::primnodes::TargetEntry;

// range table operations

/// rt_fetch: fetch the RTE at 1-based `rangetable_index` from the range table.
/// C macro used `list_nth(rangetable, index-1)`; the range table is a Vec here.
pub fn rt_fetch(rangetable_index: Index, rangetable: &[RangeTblEntry]) -> &RangeTblEntry {
    &rangetable[rangetable_index - 1]
}

/// Given an RTE and an attribute number, return the appropriate variable name
/// or alias for that attribute of that RTE.
pub fn get_rte_attribute_name(_rte: &RangeTblEntry, _attnum: AttrNumber) -> Option<String> {
    unimplemented!()
}

/// Check whether an attribute of an RTE has been dropped.
pub fn get_rte_attribute_is_dropped(_rte: &RangeTblEntry, _attnum: AttrNumber) -> bool {
    unimplemented!()
}

// target list operations

pub fn get_tle_by_resno(_tlist: &[TargetEntry], _resno: AttrNumber) -> Option<&TargetEntry> {
    unimplemented!()
}

// FOR UPDATE/SHARE info

pub fn get_parse_rowmark(_qry: &Query, _rtindex: Index) -> Option<Box<RowMarkClause>> {
    unimplemented!()
}
