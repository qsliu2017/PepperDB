//! Translated from PostgreSQL src/include/commands/subscriptioncmds.h

use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::parsenodes::{
    AlterSubscriptionStmt, CreateSubscriptionStmt, DefElem, DropSubscriptionStmt,
};
use crate::parser::parse_node::ParseState;
use crate::postgres_ext::Oid;

pub fn CreateSubscription(
    pstate: &mut ParseState,
    stmt: &mut CreateSubscriptionStmt,
    isTopLevel: bool,
) -> ObjectAddress {
    unimplemented!()
}

pub fn AlterSubscription(
    pstate: &mut ParseState,
    stmt: &mut AlterSubscriptionStmt,
    isTopLevel: bool,
) -> ObjectAddress {
    unimplemented!()
}

pub fn DropSubscription(stmt: &mut DropSubscriptionStmt, isTopLevel: bool) {
    unimplemented!()
}

pub fn AlterSubscriptionOwner(name: &str, newOwnerId: Oid) -> ObjectAddress {
    unimplemented!()
}

pub fn AlterSubscriptionOwner_oid(subid: Oid, newOwnerId: Oid) {
    unimplemented!()
}

// char return = the streaming mode code.
pub fn defGetStreamingMode(def: &mut DefElem) -> i8 {
    unimplemented!()
}
