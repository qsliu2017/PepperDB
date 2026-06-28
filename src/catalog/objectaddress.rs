//! Translated from PostgreSQL src/include/catalog/objectaddress.h

#![allow(
    clippy::needless_pass_by_value,
    reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1; real impl consumes params"
)]

use crate::access::attnum::AttrNumber;
use crate::access::htup::HeapTuple;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::ObjectType;
use crate::nodes::primnodes::RangeVar;
use crate::postgres_ext::Oid;
use crate::utils::array::ArrayType;
use crate::utils::relcache::Relation;

// C: typedef int LOCKMODE (storage/lockdefs.h).
pub type LOCKMODE = i32;

/// Represents a database object of any type. In-memory; small and Copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectAddress {
    pub classId: Oid,     // Class Id from pg_class
    pub objectId: Oid,    // OID of the object
    pub objectSubId: i32, // subitem within object (e.g. column), or 0
}

impl ObjectAddress {
    /// C macro ObjectAddressSubSet.
    pub fn set_sub(&mut self, class_id: Oid, object_id: Oid, object_sub_id: i32) {
        self.classId = class_id;
        self.objectId = object_id;
        self.objectSubId = object_sub_id;
    }

    /// C macro ObjectAddressSet (objectSubId = 0).
    pub fn set(&mut self, class_id: Oid, object_id: Oid) {
        self.set_sub(class_id, object_id, 0);
    }
}

pub const INVALID_OBJECT_ADDRESS: ObjectAddress = ObjectAddress {
    classId: crate::postgres_ext::InvalidOid,
    objectId: crate::postgres_ext::InvalidOid,
    objectSubId: 0,
};

// C out-param `Relation *relp` -> returned alongside the address; `missing_ok`
// with an InvalidOid result is the Option semantics callers want at the surface.
pub fn get_object_address(
    objtype: ObjectType,
    object: &Node,
    relp: &mut Relation,
    lockmode: LOCKMODE,
    missing_ok: bool,
) -> ObjectAddress {
    unimplemented!()
}

pub fn get_object_address_rv(
    objtype: ObjectType,
    rel: &RangeVar,
    object: Vec<Node>,
    relp: &mut Relation,
    lockmode: LOCKMODE,
    missing_ok: bool,
) -> ObjectAddress {
    unimplemented!()
}

pub fn check_object_ownership(
    roleid: Oid,
    objtype: ObjectType,
    address: ObjectAddress,
    object: &Node,
    relation: Relation,
) {
    unimplemented!()
}

pub fn get_object_namespace(address: &ObjectAddress) -> Oid {
    unimplemented!()
}

pub fn is_objectclass_supported(class_id: Oid) -> bool {
    unimplemented!()
}

pub fn get_object_class_descr(class_id: Oid) -> &'static str {
    unimplemented!()
}

pub fn get_object_oid_index(class_id: Oid) -> Oid {
    unimplemented!()
}

pub fn get_object_catcache_oid(class_id: Oid) -> i32 {
    unimplemented!()
}

pub fn get_object_catcache_name(class_id: Oid) -> i32 {
    unimplemented!()
}

pub fn get_object_attnum_oid(class_id: Oid) -> AttrNumber {
    unimplemented!()
}

pub fn get_object_attnum_name(class_id: Oid) -> AttrNumber {
    unimplemented!()
}

pub fn get_object_attnum_namespace(class_id: Oid) -> AttrNumber {
    unimplemented!()
}

pub fn get_object_attnum_owner(class_id: Oid) -> AttrNumber {
    unimplemented!()
}

pub fn get_object_attnum_acl(class_id: Oid) -> AttrNumber {
    unimplemented!()
}

pub fn get_object_type(class_id: Oid, object_id: Oid) -> ObjectType {
    unimplemented!()
}

pub fn get_object_namensp_unique(class_id: Oid) -> bool {
    unimplemented!()
}

/// Invalid HeapTuple sentinel -> None when not found.
pub fn get_catalog_object_by_oid(
    catalog: Relation,
    oidcol: AttrNumber,
    object_id: Oid,
) -> Option<HeapTuple> {
    unimplemented!()
}

pub fn get_catalog_object_by_oid_extended(
    catalog: Relation,
    oidcol: AttrNumber,
    object_id: Oid,
    locktup: bool,
) -> Option<HeapTuple> {
    unimplemented!()
}

pub fn getObjectDescription(object: &ObjectAddress, missing_ok: bool) -> String {
    unimplemented!()
}

pub fn getObjectDescriptionOids(classid: Oid, objid: Oid) -> String {
    unimplemented!()
}

pub fn read_objtype_from_string(objtype: &str) -> i32 {
    unimplemented!()
}

pub fn getObjectTypeDescription(object: &ObjectAddress, missing_ok: bool) -> String {
    unimplemented!()
}

pub fn getObjectIdentity(object: &ObjectAddress, missing_ok: bool) -> String {
    unimplemented!()
}

/// C: char* return + `List **objname`/`List **objargs` out-params -> struct.
pub struct ObjectIdentityParts {
    pub identity: String,
    pub objname: Vec<Node>,
    pub objargs: Vec<Node>,
}

pub fn getObjectIdentityParts(object: &ObjectAddress, missing_ok: bool) -> ObjectIdentityParts {
    unimplemented!()
}

pub fn strlist_to_textarray(list: Vec<Node>) -> *mut ArrayType {
    unimplemented!()
}

pub fn get_relkind_objtype(relkind: u8) -> ObjectType {
    unimplemented!()
}
