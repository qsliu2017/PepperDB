//! Translated from PostgreSQL src/include/commands/proclang.h

use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::parsenodes::CreatePLangStmt;
use crate::postgres_ext::Oid;

pub fn CreateProceduralLanguage(stmt: &mut CreatePLangStmt) -> ObjectAddress {
    unimplemented!()
}

// missing_ok sentinel (InvalidOid) -> Option.
pub fn get_language_oid(langname: &str, missing_ok: bool) -> Option<Oid> {
    unimplemented!()
}
