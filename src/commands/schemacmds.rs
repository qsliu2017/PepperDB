//! Translated from PostgreSQL src/include/commands/schemacmds.h

use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::parsenodes::CreateSchemaStmt;
use crate::postgres_ext::Oid;

pub fn CreateSchemaCommand(
    stmt: &mut CreateSchemaStmt,
    queryString: &str,
    stmt_location: i32,
    stmt_len: i32,
) -> Oid {
    unimplemented!()
}

pub fn RenameSchema(oldname: &str, newname: &str) -> ObjectAddress {
    unimplemented!()
}

pub fn AlterSchemaOwner(name: &str, newOwnerId: Oid) -> ObjectAddress {
    unimplemented!()
}

pub fn AlterSchemaOwner_oid(schemaoid: Oid, newOwnerId: Oid) {
    unimplemented!()
}
