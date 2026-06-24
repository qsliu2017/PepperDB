//! Translated from PostgreSQL src/include/rewrite/prs2lock.h
//! Data structures for the POSTGRES Rule System II (rewrite rules only).

use crate::nodes::nodes::{CmdType, Node};
use crate::postgres_ext::Oid;

/// Info for a single rewrite rule.
pub struct RewriteRule {
    pub rule_id: Oid,
    pub event: CmdType,
    pub qual: Option<Box<Node>>,
    pub actions: Vec<Node>,
    pub enabled: u8, // char-coded enable state (rule firing setting)
    pub is_instead: bool,
}

/// All rules that apply to a particular relation. ("Lock" is historical; these
/// are not really locks.)
pub struct RuleLock {
    pub rules: Vec<RewriteRule>, // numLocks is implied by .len()
}
