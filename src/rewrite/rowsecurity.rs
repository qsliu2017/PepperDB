//! Translated from PostgreSQL src/include/rewrite/rowsecurity.h
//! Structures for managing row security policies and rewrite/rowsecurity.c protos.

use crate::nodes::nodes::{CmdType, Node};
use crate::nodes::parsenodes::{Query, RangeTblEntry};
use crate::utils::array::ArrayType;
use crate::utils::palloc::MemoryContext;
use crate::utils::relcache::Relation;

pub struct RowSecurityPolicy {
    /// Name of the policy
    pub policy_name: Option<String>,
    /// Type of command policy is for (C `char` command-type code)
    pub polcmd: i8,
    /// Array of roles policy is for
    pub roles: Option<Box<ArrayType>>,
    /// restrictive or permissive policy
    pub permissive: bool,
    /// Expression to filter rows
    pub qual: Option<Node>,
    /// Expression to limit rows allowed
    pub with_check_qual: Option<Node>,
    /// If either expression has sublinks
    pub hassublinks: bool,
}

pub struct RowSecurityDesc {
    /// row security memory context
    pub rscxt: MemoryContext,
    /// list of row security policies
    pub policies: Vec<RowSecurityPolicy>,
}

/// C: `List *(*row_security_policy_hook_type)(CmdType, Relation);`
pub type RowSecurityPolicyHookType = fn(cmdtype: CmdType, relation: Relation) -> Vec<RowSecurityPolicy>;

// TODO(global): migrate these hook pointers to session/extension registry.
pub static mut ROW_SECURITY_POLICY_HOOK_PERMISSIVE: Option<RowSecurityPolicyHookType> = None;
pub static mut ROW_SECURITY_POLICY_HOOK_RESTRICTIVE: Option<RowSecurityPolicyHookType> = None;

/// Output of `get_row_security_policies`.
///
/// C out-params `List **securityQuals, List **withCheckOptions, bool
/// *hasRowSecurity, bool *hasSubLinks` -> a named struct (4 mixed outputs).
pub struct RowSecurityPolicies {
    pub security_quals: Vec<Node>,
    pub with_check_options: Vec<Node>,
    pub has_row_security: bool,
    pub has_sub_links: bool,
}

pub fn get_row_security_policies(
    _root: &Query,
    _rte: &RangeTblEntry,
    _rt_index: i32,
) -> RowSecurityPolicies {
    unimplemented!()
}
