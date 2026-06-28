//! Translated from PostgreSQL src/include/commands/collationcmds.h

use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::AlterCollationStmt;
use crate::parser::parse_node::ParseState;
use crate::postgres_ext::Oid;

pub fn DefineCollation(
    _pstate: &mut ParseState,
    _names: &[Node],
    _parameters: &[Node],
    _if_not_exists: bool,
) -> ObjectAddress {
    unimplemented!()
}

pub fn IsThereCollationInNamespace(_collname: &str, _nsp_oid: Oid) {
    unimplemented!()
}

pub fn AlterCollation(_stmt: &AlterCollationStmt) -> ObjectAddress {
    unimplemented!()
}
