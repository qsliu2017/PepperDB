//! Translated from PostgreSQL src/include/utils/aclchk_internal.h

use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{AclMode, DropBehavior, ObjectType};
use crate::postgres_ext::Oid;

/// Information about one Grant/Revoke statement, in internal format: object and
/// grantee names turned into Oids, the privilege list an AclMode bitmask.
///
/// `all_privs`/`privileges` are object-level only; column-level privileges are
/// in `col_privs` (untransformed AccessPriv nodes; valid only for OBJECT_TABLE).
#[derive(Debug, Clone, PartialEq)]
pub struct InternalGrant {
    pub is_grant: bool,
    pub objtype: ObjectType,
    pub objects: Vec<Oid>,
    pub all_privs: bool,
    pub privileges: AclMode,
    pub col_privs: Vec<Node>,
    pub grantees: Vec<Oid>,
    pub grant_option: bool,
    pub behavior: DropBehavior,
}
