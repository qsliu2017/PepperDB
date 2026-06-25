//! Translated from PostgreSQL src/include/catalog/pg_attrdef.h

use crate::c::text;
use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::DropBehavior;
use crate::postgres_ext::Oid;
use crate::utils::rel::Relation;

pub const AttrDefaultRelationId: Oid = Oid(2604);

// pg_node_tree catalog field = varlena (serialized node tree); modeled as text.
pub type PgNodeTree = text;

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_attrdef {
    pub oid: Oid,
    pub adrelid: Oid, // BKI_LOOKUP(pg_class)
    pub adnum: i16,
    // CATALOG_VARLEN (not in fixed part):
    pub adbin: PgNodeTree, // BKI_FORCE_NOT_NULL
}

pub type Form_pg_attrdef = *mut FormData_pg_attrdef; // TODO(ptr)

// DECLARE_TOAST(pg_attrdef, 2830, 2831)
// DECLARE_UNIQUE_INDEX(pg_attrdef_adrelid_adnum_index, 2656, AttrDefaultIndexId, ...)
// DECLARE_UNIQUE_INDEX_PKEY(pg_attrdef_oid_index, 2657, AttrDefaultOidIndexId, ...)
// DECLARE_FOREIGN_KEY((adrelid, adnum), pg_attribute, (attrelid, attnum))

pub type AttrNumber = i16;

pub fn StoreAttrDefault(_rel: &Relation, _attnum: AttrNumber, _expr: &Node, _is_internal: bool) -> Oid {
    unimplemented!()
}

pub fn RemoveAttrDefault(
    _relid: Oid,
    _attnum: AttrNumber,
    _behavior: DropBehavior,
    _complain: bool,
    _internal: bool,
) {
    unimplemented!()
}

pub fn RemoveAttrDefaultById(_attrdef_id: Oid) {
    unimplemented!()
}

pub fn GetAttrDefaultOid(_relid: Oid, _attnum: AttrNumber) -> Oid {
    unimplemented!()
}

pub fn GetAttrDefaultColumnAddress(_attrdefoid: Oid) -> ObjectAddress {
    unimplemented!()
}
