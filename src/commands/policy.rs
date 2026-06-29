//! Translated from PostgreSQL src/include/commands/policy.h

use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::parsenodes::{AlterPolicyStmt, CreatePolicyStmt, RenameStmt};
use crate::postgres_ext::Oid;

pub fn RelationBuildRowSecurity(relation: &crate::utils::rel::RelationData) {
    unimplemented!()
}

pub fn RemovePolicyById(policy_id: Oid) {
    unimplemented!()
}

pub fn RemoveRoleFromObjectPolicy(roleid: Oid, classid: Oid, policy_id: Oid) -> bool {
    unimplemented!()
}

pub fn CreatePolicy(stmt: &mut CreatePolicyStmt) -> ObjectAddress {
    unimplemented!()
}

pub fn AlterPolicy(stmt: &mut AlterPolicyStmt) -> ObjectAddress {
    unimplemented!()
}

// missing_ok sentinel (InvalidOid) -> Option.
pub fn get_relation_policy_oid(relid: Oid, policy_name: &str, missing_ok: bool) -> Option<Oid> {
    unimplemented!()
}

pub fn rename_policy(stmt: &mut RenameStmt) -> ObjectAddress {
    unimplemented!()
}

pub fn relation_has_policies(rel: &crate::utils::rel::RelationData) -> bool {
    unimplemented!()
}
