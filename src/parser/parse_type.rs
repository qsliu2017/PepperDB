//! Translated from PostgreSQL src/include/parser/parse_type.h

use crate::access::htup::HeapTuple;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{ColumnDef, TypeName};
use crate::parser::parse_node::ParseState;
use crate::postgres::Datum;
use crate::postgres_ext::{InvalidOid, Oid};

pub type Type = HeapTuple;

/// `typmod_p` out-param folded in; missing-tuple sentinel -> `Option`.
pub fn LookupTypeName(
    _pstate: &mut ParseState,
    _type_name: &TypeName,
    _typmod_p: &mut i32,
    _missing_ok: bool,
) -> Option<Type> {
    unimplemented!()
}

pub fn LookupTypeNameExtended(
    _pstate: &mut ParseState,
    _type_name: &TypeName,
    _typmod_p: &mut i32,
    _temp_ok: bool,
    _missing_ok: bool,
) -> Option<Type> {
    unimplemented!()
}

/// `InvalidOid` sentinel (when `missing_ok`) -> `Option`.
pub fn LookupTypeNameOid(
    _pstate: &mut ParseState,
    _type_name: &TypeName,
    _missing_ok: bool,
) -> Option<Oid> {
    unimplemented!()
}

/// `typmod_p` out-param folded in -> `(Type, typmod)`.
pub fn typenameType(
    _pstate: &mut ParseState,
    _type_name: &TypeName,
) -> (Type, i32) {
    unimplemented!()
}

pub fn typenameTypeId(_pstate: &mut ParseState, _type_name: &TypeName) -> Oid {
    unimplemented!()
}

/// `typeid_p`/`typmod_p` out-params -> returned tuple.
pub fn typenameTypeIdAndMod(_pstate: &mut ParseState, _type_name: &TypeName) -> (Oid, i32) {
    unimplemented!()
}

pub fn TypeNameToString(_type_name: &TypeName) -> String {
    unimplemented!()
}

pub fn TypeNameListToString(_typenames: &[Box<Node>]) -> String {
    unimplemented!()
}

pub fn LookupCollation(_pstate: &mut ParseState, _collnames: Vec<Box<Node>>, _location: i32) -> Oid {
    unimplemented!()
}

pub fn GetColumnDefCollation(
    _pstate: &mut ParseState,
    _coldef: &ColumnDef,
    _type_oid: Oid,
) -> Oid {
    unimplemented!()
}

pub fn typeidType(_id: Oid) -> Type {
    unimplemented!()
}

pub fn typeTypeId(_tp: Type) -> Oid {
    unimplemented!()
}

pub fn typeLen(_t: Type) -> i16 {
    unimplemented!()
}

pub fn typeByVal(_t: Type) -> bool {
    unimplemented!()
}

pub fn typeTypeName(_t: Type) -> String {
    unimplemented!()
}

pub fn typeTypeRelid(_typ: Type) -> Oid {
    unimplemented!()
}

pub fn typeTypeCollation(_typ: Type) -> Oid {
    unimplemented!()
}

pub fn stringTypeDatum(_tp: Type, _string: &str, _atttypmod: i32) -> Datum {
    unimplemented!()
}

pub fn typeidTypeRelid(_type_id: Oid) -> Oid {
    unimplemented!()
}

pub fn typeOrDomainTypeRelid(_type_id: Oid) -> Oid {
    unimplemented!()
}

pub fn typeStringToTypeName(_str: &str, _escontext: Option<&mut Node>) -> Box<TypeName> {
    unimplemented!()
}

/// `typeid_p`/`typmod_p` out-params -> returned tuple on success.
pub fn parseTypeString(
    _str: &str,
    _escontext: Option<&mut Node>,
) -> Option<(Oid, i32)> {
    unimplemented!()
}

/// true if typeid is composite, or domain over composite, but not RECORD
pub fn ISCOMPLEX(typeid: Oid) -> bool {
    typeOrDomainTypeRelid(typeid) != InvalidOid
}
