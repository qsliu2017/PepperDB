//! parser/parsetree.h - routines to access components/subcomponents of parse trees.

use std::ffi::c_char;

use crate::access::attnum::AttrNumber;
use crate::c::Index;
use crate::nodes::pg_list::{list_nth, List};
use crate::nodes::parsenodes::{Query, RangeTblEntry, RowMarkClause};
use crate::nodes::primnodes::TargetEntry;

/* ----------------
 *		range table operations
 * ----------------
 */

/*
 *		rt_fetch
 *
 * NB: this will crash and burn if handed an out-of-range RT index
 */
#[inline]
pub unsafe fn rt_fetch(rangetable_index: Index, rangetable: *const List) -> *mut RangeTblEntry {
    list_nth(rangetable, (rangetable_index as i32) - 1) as *mut RangeTblEntry
}

/*
 * Given an RTE and an attribute number, return the appropriate
 * variable name or alias for that attribute of that RTE.
 */
pub unsafe fn get_rte_attribute_name(_rte: *mut RangeTblEntry, _attnum: AttrNumber) -> *mut c_char {
    unimplemented!()
}

/*
 * Check whether an attribute of an RTE has been dropped
 */
pub unsafe fn get_rte_attribute_is_dropped(_rte: *mut RangeTblEntry, _attnum: AttrNumber) -> bool {
    unimplemented!()
}

/* ----------------
 *		target list operations
 * ----------------
 */

pub unsafe fn get_tle_by_resno(_tlist: *mut List, _resno: AttrNumber) -> *mut TargetEntry {
    unimplemented!()
}

/* ----------------
 *		FOR UPDATE/SHARE info
 * ----------------
 */

pub unsafe fn get_parse_rowmark(_qry: *mut Query, _rtindex: Index) -> *mut RowMarkClause {
    unimplemented!()
}
