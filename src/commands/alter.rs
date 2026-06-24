//! Translated from PostgreSQL src/include/commands/alter.h

use crate::catalog::dependency::ObjectAddresses;
use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::parsenodes::{
    AlterObjectDependsStmt, AlterObjectSchemaStmt, AlterOwnerStmt, RenameStmt,
};
use crate::postgres_ext::Oid;

pub fn ExecRenameStmt(_stmt: &RenameStmt) -> ObjectAddress {
    unimplemented!()
}

// out-param refAddress folded into the returned tuple.
pub fn ExecAlterObjectDependsStmt(
    _stmt: &AlterObjectDependsStmt,
) -> (ObjectAddress, ObjectAddress) {
    unimplemented!()
}

// out-param oldSchemaAddr folded into the returned tuple.
pub fn ExecAlterObjectSchemaStmt(
    _stmt: &AlterObjectSchemaStmt,
) -> (ObjectAddress, ObjectAddress) {
    unimplemented!()
}

pub fn AlterObjectNamespace_oid(
    _classId: Oid,
    _objid: Oid,
    _nspOid: Oid,
    _objsMoved: &mut ObjectAddresses,
) -> Oid {
    unimplemented!()
}

pub fn ExecAlterOwnerStmt(_stmt: &AlterOwnerStmt) -> ObjectAddress {
    unimplemented!()
}

pub fn AlterObjectOwner_internal(_classId: Oid, _objectId: Oid, _new_ownerId: Oid) {
    unimplemented!()
}
