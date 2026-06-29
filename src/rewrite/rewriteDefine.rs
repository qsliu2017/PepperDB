//! Translated from PostgreSQL src/include/rewrite/rewriteDefine.h

use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::nodes::{CmdType, Node};
use crate::nodes::parsenodes::RuleStmt;
use crate::nodes::primnodes::RangeVar;
use crate::postgres_ext::Oid;
use crate::utils::rel::RelationData;

pub const RULE_FIRES_ON_ORIGIN: u8 = b'O';
pub const RULE_FIRES_ALWAYS: u8 = b'A';
pub const RULE_FIRES_ON_REPLICA: u8 = b'R';
pub const RULE_DISABLED: u8 = b'D';

pub fn define_rule(_stmt: &RuleStmt, _query_string: &str) -> ObjectAddress {
    unimplemented!()
}

pub fn define_query_rewrite(
    _rulename: &str,
    _event_relid: Oid,
    _event_qual: Option<Node>,
    _event_type: CmdType,
    _is_instead: bool,
    _replace: bool,
    _action: Vec<Node>,
) -> ObjectAddress {
    unimplemented!()
}

pub fn rename_rewrite_rule(_relation: &RangeVar, _old_name: &str, _new_name: &str) -> ObjectAddress {
    unimplemented!()
}

pub fn set_rule_check_as_user(_node: &mut Node, _userid: Oid) {
    unimplemented!()
}

pub fn enable_disable_rule(_rel: &RelationData, _rulename: &str, _fires_when: u8) {
    unimplemented!()
}
