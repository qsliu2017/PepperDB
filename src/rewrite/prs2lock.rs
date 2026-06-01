//! rewrite/prs2lock.h - data structures for POSTGRES Rule System II (rewrite rules only)

use std::ffi::c_int;
use crate::nodes::nodes::{CmdType, Node};
use crate::nodes::pg_list::List;
use crate::postgres_ext::Oid;
use std::ffi::c_char;

/*
 * RewriteRule -
 *	  holds an info for a rewrite rule
 *
 */
#[repr(C)]
pub struct RewriteRule {
    pub ruleId: Oid,
    pub event: CmdType,
    pub qual: *mut Node,
    pub actions: *mut List,
    pub enabled: c_char,
    pub isInstead: bool,
}

/*
 * RuleLock -
 *	  all rules that apply to a particular relation. Even though we only
 *	  have the rewrite rule system left and these are not really "locks",
 *	  the name is kept for historical reasons.
 */
#[repr(C)]
pub struct RuleLock {
    pub numLocks: c_int,
    pub rules: *mut *mut RewriteRule,
}
