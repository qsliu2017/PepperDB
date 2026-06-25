//! Translated from PostgreSQL src/include/commands/user.h

use crate::catalog::objectaddress::ObjectAddress;
use crate::libpq::crypt::PasswordType;
use crate::nodes::parsenodes::{
    AlterRoleSetStmt, AlterRoleStmt, CreateRoleStmt, DropOwnedStmt, DropRoleStmt, GrantRoleStmt,
    ReassignOwnedStmt,
};
use crate::parser::parse_node::ParseState;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::utils::guc::GucSource;

// GUCs (process-global in C -> session/global state later).
pub static mut Password_encryption: i32 = 0; // values from enum PasswordType
pub static mut createrole_self_grant: Option<String> = None;

// Hook to check passwords in CreateRole()/AlterRole(); void *arg-style hook -> closure later.
pub type check_password_hook_type =
    fn(username: &str, shadow_pass: &str, password_type: PasswordType, validuntil_time: Datum, validuntil_null: bool);

pub static mut check_password_hook: Option<check_password_hook_type> = None;

pub fn CreateRole(_pstate: &ParseState, _stmt: &CreateRoleStmt) -> Oid {
    unimplemented!()
}

pub fn AlterRole(_pstate: &ParseState, _stmt: &AlterRoleStmt) -> Oid {
    unimplemented!()
}

pub fn AlterRoleSet(_stmt: &AlterRoleSetStmt) -> Oid {
    unimplemented!()
}

pub fn DropRole(_stmt: &DropRoleStmt) {
    unimplemented!()
}

pub fn GrantRole(_pstate: &ParseState, _stmt: &GrantRoleStmt) {
    unimplemented!()
}

pub fn RenameRole(_oldname: &str, _newname: &str) -> ObjectAddress {
    unimplemented!()
}

pub fn DropOwnedObjects(_stmt: &DropOwnedStmt) {
    unimplemented!()
}

pub fn ReassignOwnedObjects(_stmt: &ReassignOwnedStmt) {
    unimplemented!()
}

// roleSpecsToIds: List<RoleSpec> -> List<Oid>
pub fn roleSpecsToIds(_member_names: &[&str]) -> Vec<Oid> {
    unimplemented!()
}

// GUC check/assign hooks for createrole_self_grant. check returns bool success.
pub fn check_createrole_self_grant(_newval: &mut String, _source: GucSource) -> bool {
    unimplemented!()
}

pub fn assign_createrole_self_grant(_newval: &str) {
    unimplemented!()
}
