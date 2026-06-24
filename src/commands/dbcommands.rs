//! Translated from PostgreSQL src/include/commands/dbcommands.h

use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::parsenodes::{
    AlterDatabaseRefreshCollStmt, AlterDatabaseSetStmt, AlterDatabaseStmt, CreatedbStmt, DropdbStmt,
};
use crate::parser::parse_node::ParseState;
use crate::postgres_ext::Oid;

pub fn createdb(_pstate: &mut ParseState, _stmt: &CreatedbStmt) -> Oid {
    unimplemented!()
}

pub fn dropdb(_dbname: &str, _missing_ok: bool, _force: bool) {
    unimplemented!()
}

pub fn DropDatabase(_pstate: &mut ParseState, _stmt: &DropdbStmt) {
    unimplemented!()
}

pub fn RenameDatabase(_oldname: &str, _newname: &str) -> ObjectAddress {
    unimplemented!()
}

pub fn AlterDatabase(_pstate: &mut ParseState, _stmt: &AlterDatabaseStmt, _is_top_level: bool) -> Oid {
    unimplemented!()
}

pub fn AlterDatabaseRefreshColl(_stmt: &AlterDatabaseRefreshCollStmt) -> ObjectAddress {
    unimplemented!()
}

pub fn AlterDatabaseSet(_stmt: &AlterDatabaseSetStmt) -> Oid {
    unimplemented!()
}

pub fn AlterDatabaseOwner(_dbname: &str, _new_owner_id: Oid) -> ObjectAddress {
    unimplemented!()
}

/// InvalidOid sentinel (when missing_ok) -> None.
pub fn get_database_oid(_dbname: &str, _missing_ok: bool) -> Option<Oid> {
    unimplemented!()
}

pub fn get_database_name(_dbid: Oid) -> Option<String> {
    unimplemented!()
}

pub fn have_createdb_privilege() -> bool {
    unimplemented!()
}

pub fn check_encoding_locale_matches(_encoding: i32, _collate: &str, _ctype: &str) {
    unimplemented!()
}
