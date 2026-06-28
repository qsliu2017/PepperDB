//! Translated from PostgreSQL src/include/commands/typecmds.h

#![allow(
    clippy::boxed_local,
    reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1; real impl consumes params"
)]

use crate::access::htup::HeapTuple;
use crate::catalog::dependency::ObjectAddresses;
use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{
    AlterEnumStmt, AlterTypeStmt, CreateDomainStmt, CreateEnumStmt, CreateRangeStmt, DropBehavior,
    ObjectType, RenameStmt,
};
use crate::nodes::primnodes::RangeVar;
use crate::parser::parse_node::ParseState;
use crate::postgres_ext::Oid;

pub const DEFAULT_TYPDELIM: u8 = b',';

pub fn DefineType(
    _pstate: &mut ParseState,
    _names: Vec<Node>,
    _parameters: Vec<Node>,
) -> ObjectAddress {
    unimplemented!()
}

pub fn RemoveTypeById(_typeOid: Oid) {
    unimplemented!()
}

pub fn DefineDomain(_pstate: &mut ParseState, _stmt: &CreateDomainStmt) -> ObjectAddress {
    unimplemented!()
}

pub fn DefineEnum(_stmt: &CreateEnumStmt) -> ObjectAddress {
    unimplemented!()
}

pub fn DefineRange(_pstate: &mut ParseState, _stmt: &CreateRangeStmt) -> ObjectAddress {
    unimplemented!()
}

pub fn AlterEnum(_stmt: &AlterEnumStmt) -> ObjectAddress {
    unimplemented!()
}

pub fn DefineCompositeType(_typevar: &RangeVar, _coldeflist: Vec<Node>) -> ObjectAddress {
    unimplemented!()
}

pub fn AssignTypeArrayOid() -> Oid {
    unimplemented!()
}

pub fn AssignTypeMultirangeOid() -> Oid {
    unimplemented!()
}

pub fn AssignTypeMultirangeArrayOid() -> Oid {
    unimplemented!()
}

pub fn AlterDomainDefault(_names: Vec<Node>, _defaultRaw: Node) -> ObjectAddress {
    unimplemented!()
}

pub fn AlterDomainNotNull(_names: Vec<Node>, _notNull: bool) -> ObjectAddress {
    unimplemented!()
}

// out-param constrAddr folded into the returned tuple.
pub fn AlterDomainAddConstraint(
    _names: Vec<Node>,
    _newConstraint: Node,
) -> (ObjectAddress, ObjectAddress) {
    unimplemented!()
}

pub fn AlterDomainValidateConstraint(_names: Vec<Node>, _constrName: &str) -> ObjectAddress {
    unimplemented!()
}

pub fn AlterDomainDropConstraint(
    _names: Vec<Node>,
    _constrName: &str,
    _behavior: DropBehavior,
    _missing_ok: bool,
) -> ObjectAddress {
    unimplemented!()
}

pub fn checkDomainOwner(_tup: HeapTuple) {
    unimplemented!()
}

pub fn RenameType(_stmt: &RenameStmt) -> ObjectAddress {
    unimplemented!()
}

pub fn AlterTypeOwner(
    _names: Vec<Node>,
    _newOwnerId: Oid,
    _objecttype: ObjectType,
) -> ObjectAddress {
    unimplemented!()
}

pub fn AlterTypeOwner_oid(_typeOid: Oid, _newOwnerId: Oid, _hasDependEntry: bool) {
    unimplemented!()
}

pub fn AlterTypeOwnerInternal(_typeOid: Oid, _newOwnerId: Oid) {
    unimplemented!()
}

// out-param oldschema folded into the returned tuple.
pub fn AlterTypeNamespace(
    _names: Vec<Node>,
    _newschema: &str,
    _objecttype: ObjectType,
) -> (ObjectAddress, Oid) {
    unimplemented!()
}

pub fn AlterTypeNamespace_oid(
    _typeOid: Oid,
    _nspOid: Oid,
    _ignoreDependent: bool,
    _objsMoved: &mut ObjectAddresses,
) -> Oid {
    unimplemented!()
}

pub fn AlterTypeNamespaceInternal(
    _typeOid: Oid,
    _nspOid: Oid,
    _isImplicitArray: bool,
    _ignoreDependent: bool,
    _errorOnTableType: bool,
    _objsMoved: &mut ObjectAddresses,
) -> Oid {
    unimplemented!()
}

pub fn AlterType(_stmt: &AlterTypeStmt) -> ObjectAddress {
    unimplemented!()
}
