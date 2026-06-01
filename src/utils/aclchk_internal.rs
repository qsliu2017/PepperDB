//! utils/aclchk_internal.h - internal format for one GRANT/REVOKE statement.

use crate::nodes::parsenodes::{AclMode, DropBehavior, ObjectType};
use crate::nodes::pg_list::List;

/*
 * The information about one Grant/Revoke statement, in internal format: object
 * and grantees names have been turned into Oids, the privilege list is an
 * AclMode bitmask.  If 'privileges' is ACL_NO_RIGHTS (the 0 value) and
 * all_privs is true, 'privileges' will be internally set to the right kind of
 * ACL_ALL_RIGHTS_*, depending on the object type (NB - this will modify the
 * InternalGrant struct!)
 *
 * Note: 'all_privs' and 'privileges' represent object-level privileges only.
 * There might also be column-level privilege specifications, which are
 * represented in col_privs (this is a list of untransformed AccessPriv nodes).
 * Column privileges are only valid for objtype OBJECT_TABLE.
 */
#[repr(C)]
pub struct InternalGrant {
    pub is_grant: bool,
    pub objtype: ObjectType,
    pub objects: *mut List,
    pub all_privs: bool,
    pub privileges: AclMode,
    pub col_privs: *mut List,
    pub grantees: *mut List,
    pub grant_option: bool,
    pub behavior: DropBehavior,
}
