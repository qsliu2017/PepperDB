//! Translated from PostgreSQL src/include/commands/publicationcmds.h

use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::parsenodes::{AlterPublicationStmt, CreatePublicationStmt};
use crate::parser::parse_node::ParseState;
use crate::postgres_ext::Oid;

/// Same as MAXNUMMESSAGES in sinvaladt.c.
pub const MAX_RELCACHE_INVAL_MSGS: i32 = 4096;

pub fn CreatePublication(pstate: &mut ParseState, stmt: &mut CreatePublicationStmt) -> ObjectAddress {
    unimplemented!()
}

pub fn AlterPublication(pstate: &mut ParseState, stmt: &mut AlterPublicationStmt) {
    unimplemented!()
}

pub fn RemovePublicationById(pubid: Oid) {
    unimplemented!()
}

pub fn RemovePublicationRelById(proid: Oid) {
    unimplemented!()
}

pub fn RemovePublicationSchemaById(psoid: Oid) {
    unimplemented!()
}

pub fn AlterPublicationOwner(name: &str, newOwnerId: Oid) -> ObjectAddress {
    unimplemented!()
}

pub fn AlterPublicationOwner_oid(pubid: Oid, newOwnerId: Oid) {
    unimplemented!()
}

// List *relids (OidList) -> &[Oid].
pub fn InvalidatePublicationRels(relids: &[Oid]) {
    unimplemented!()
}

pub fn pub_rf_contains_invalid_column(
    pubid: Oid,
    relation: &crate::utils::rel::RelationData,
    ancestors: &[Oid],
    pubviaroot: bool,
) -> bool {
    unimplemented!()
}

// bool out-params (invalid_column_list, invalid_gen_col) fold into a result struct.
pub struct PubInvalidColumns {
    pub invalid: bool,
    pub invalid_column_list: bool,
    pub invalid_gen_col: bool,
}

pub fn pub_contains_invalid_column(
    pubid: Oid,
    relation: &crate::utils::rel::RelationData,
    ancestors: &[Oid],
    pubviaroot: bool,
    pubgencols_type: i8,
) -> PubInvalidColumns {
    unimplemented!()
}

pub fn InvalidatePubRelSyncCache(pubid: Oid, puballtables: bool) {
    unimplemented!()
}
