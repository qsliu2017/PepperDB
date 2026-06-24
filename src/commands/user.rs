//! Translated from PostgreSQL src/include/commands/user.h

use crate::libpq::crypt::PasswordType;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;

// GUCs (process-global in C -> session/global state later).
pub static mut Password_encryption: i32 = 0; // values from enum PasswordType
pub static mut createrole_self_grant: Option<String> = None;

// Hook to check passwords in CreateRole()/AlterRole(); void *arg-style hook -> closure later.
pub type check_password_hook_type =
    fn(username: &str, shadow_pass: &str, password_type: PasswordType, validuntil_time: Datum, validuntil_null: bool);

pub static mut check_password_hook: Option<check_password_hook_type> = None;

// Forward refs for the function stubs; repointed in Phase 2.
#[deprecated(note = "TODO(struct-forward): repoint to crate::parser::parse_node::ParseState in Phase 2")]
pub struct ParseState; // TODO(struct-forward)
#[deprecated(note = "TODO(struct-forward): repoint to crate::nodes::parsenodes::CreateRoleStmt in Phase 2")]
pub struct CreateRoleStmt; // TODO(struct-forward)
#[deprecated(note = "TODO(struct-forward): repoint to crate::nodes::parsenodes::AlterRoleStmt in Phase 2")]
pub struct AlterRoleStmt; // TODO(struct-forward)
#[deprecated(note = "TODO(struct-forward): repoint to crate::nodes::parsenodes::AlterRoleSetStmt in Phase 2")]
pub struct AlterRoleSetStmt; // TODO(struct-forward)
#[deprecated(note = "TODO(struct-forward): repoint to crate::nodes::parsenodes::DropRoleStmt in Phase 2")]
pub struct DropRoleStmt; // TODO(struct-forward)
#[deprecated(note = "TODO(struct-forward): repoint to crate::nodes::parsenodes::GrantRoleStmt in Phase 2")]
pub struct GrantRoleStmt; // TODO(struct-forward)
#[deprecated(note = "TODO(struct-forward): repoint to crate::nodes::parsenodes::DropOwnedStmt in Phase 2")]
pub struct DropOwnedStmt; // TODO(struct-forward)
#[deprecated(note = "TODO(struct-forward): repoint to crate::nodes::parsenodes::ReassignOwnedStmt in Phase 2")]
pub struct ReassignOwnedStmt; // TODO(struct-forward)
#[deprecated(note = "TODO(struct-forward): repoint to crate::catalog::objectaddress::ObjectAddress in Phase 2")]
pub struct ObjectAddress; // TODO(struct-forward)
#[deprecated(note = "TODO(struct-forward): repoint to crate::utils::guc GucSource in Phase 2")]
pub struct GucSource; // TODO(struct-forward)

#[allow(deprecated)]
pub fn CreateRole(_pstate: &ParseState, _stmt: &CreateRoleStmt) -> Oid {
    unimplemented!()
}

#[allow(deprecated)]
pub fn AlterRole(_pstate: &ParseState, _stmt: &AlterRoleStmt) -> Oid {
    unimplemented!()
}

#[allow(deprecated)]
pub fn AlterRoleSet(_stmt: &AlterRoleSetStmt) -> Oid {
    unimplemented!()
}

#[allow(deprecated)]
pub fn DropRole(_stmt: &DropRoleStmt) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn GrantRole(_pstate: &ParseState, _stmt: &GrantRoleStmt) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn RenameRole(_oldname: &str, _newname: &str) -> ObjectAddress {
    unimplemented!()
}

#[allow(deprecated)]
pub fn DropOwnedObjects(_stmt: &DropOwnedStmt) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn ReassignOwnedObjects(_stmt: &ReassignOwnedStmt) {
    unimplemented!()
}

// roleSpecsToIds: List<RoleSpec> -> List<Oid>
pub fn roleSpecsToIds(_member_names: &[&str]) -> Vec<Oid> {
    unimplemented!()
}

// GUC check/assign hooks for createrole_self_grant. check returns bool success.
#[allow(deprecated)]
pub fn check_createrole_self_grant(_newval: &mut String, _source: GucSource) -> bool {
    unimplemented!()
}

pub fn assign_createrole_self_grant(_newval: &str) {
    unimplemented!()
}
