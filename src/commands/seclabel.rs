//! Translated from PostgreSQL src/include/commands/seclabel.h

use crate::catalog::objectaddress::ObjectAddress;
use crate::postgres_ext::Oid;

// char * return (NULL when no label) -> Option<String>.
pub fn GetSecurityLabel(object: &ObjectAddress, provider: &str) -> Option<String> {
    unimplemented!()
}

pub fn SetSecurityLabel(object: &ObjectAddress, provider: &str, label: &str) {
    unimplemented!()
}

pub fn DeleteSecurityLabel(object: &ObjectAddress) {
    unimplemented!()
}

pub fn DeleteSharedSecurityLabel(objectId: Oid, classId: Oid) {
    unimplemented!()
}

pub fn ExecSecLabelStmt(stmt: &mut crate::nodes::parsenodes::SecLabelStmt) -> ObjectAddress {
    unimplemented!()
}

/// C: `void (*check_object_relabel_type)(const ObjectAddress *object, const char *seclabel);`
pub type check_object_relabel_type = fn(object: &ObjectAddress, seclabel: &str);

pub fn register_label_provider(provider_name: &str, hook: check_object_relabel_type) {
    unimplemented!()
}
