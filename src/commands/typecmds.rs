//! Translated from PostgreSQL src/include/commands/typecmds.h

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
    _names: Vec<Box<Node>>,
    _parameters: Vec<Box<Node>>,
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

pub fn DefineCompositeType(_typevar: &RangeVar, _coldeflist: Vec<Box<Node>>) -> ObjectAddress {
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

pub fn AlterDomainDefault(_names: Vec<Box<Node>>, _defaultRaw: Box<Node>) -> ObjectAddress {
    unimplemented!()
}

pub fn AlterDomainNotNull(_names: Vec<Box<Node>>, _notNull: bool) -> ObjectAddress {
    unimplemented!()
}

// out-param constrAddr folded into the returned tuple.
pub fn AlterDomainAddConstraint(
    _names: Vec<Box<Node>>,
    _newConstraint: Box<Node>,
) -> (ObjectAddress, ObjectAddress) {
    unimplemented!()
}

pub fn AlterDomainValidateConstraint(_names: Vec<Box<Node>>, _constrName: &str) -> ObjectAddress {
    unimplemented!()
}

pub fn AlterDomainDropConstraint(
    _names: Vec<Box<Node>>,
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
    _names: Vec<Box<Node>>,
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
    _names: Vec<Box<Node>>,
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
