//! Translated from PostgreSQL src/include/commands/extension.h

use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{
    AlterExtensionContentsStmt, AlterExtensionStmt, CreateExtensionStmt,
};
use crate::parser::parse_node::ParseState;
use crate::postgres::Datum;
use crate::postgres_ext::{InvalidOid, Oid};

/// GUC
pub static mut Extension_control_path: Option<String> = None;

/// creating_extension is only true while running a CREATE EXTENSION or ALTER
/// EXTENSION UPDATE command. It instructs recordDependencyOnCurrentExtension()
/// to register a dependency on the current pg_extension object for each SQL
/// object created by an extension script.
pub static mut creating_extension: bool = false;
pub static mut CurrentExtensionObject: Oid = InvalidOid;

pub fn CreateExtension(_pstate: &mut ParseState, _stmt: &CreateExtensionStmt) -> ObjectAddress {
    unimplemented!()
}

pub fn RemoveExtensionById(_ext_id: Oid) {
    unimplemented!()
}

pub fn InsertExtensionTuple(
    _ext_name: &str,
    _ext_owner: Oid,
    _schema_oid: Oid,
    _relocatable: bool,
    _ext_version: &str,
    _ext_config: Datum,
    _ext_condition: Datum,
    _required_extensions: &[Oid],
) -> ObjectAddress {
    unimplemented!()
}

pub fn ExecAlterExtensionStmt(
    _pstate: &mut ParseState,
    _stmt: &AlterExtensionStmt,
) -> ObjectAddress {
    unimplemented!()
}

pub fn ExecAlterExtensionContentsStmt(
    _stmt: &AlterExtensionContentsStmt,
    _obj_addr: Option<&mut ObjectAddress>,
) -> ObjectAddress {
    unimplemented!()
}

/// InvalidOid sentinel (when missing_ok) -> None.
pub fn get_extension_oid(_extname: &str, _missing_ok: bool) -> Option<Oid> {
    unimplemented!()
}

pub fn get_extension_name(_ext_oid: Oid) -> Option<String> {
    unimplemented!()
}

pub fn get_extension_schema(_ext_oid: Oid) -> Oid {
    unimplemented!()
}

pub fn extension_file_exists(_extension_name: &str) -> bool {
    unimplemented!()
}

pub fn get_function_sibling_type(_funcoid: Oid, _typname: &str) -> Oid {
    unimplemented!()
}

/// C out-param `Oid *oldschema` folded into the return tuple.
pub fn AlterExtensionNamespace(
    _extension_name: &str,
    _newschema: &str,
) -> (ObjectAddress, Oid) {
    unimplemented!()
}
