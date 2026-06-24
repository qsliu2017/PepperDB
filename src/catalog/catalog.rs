//! Translated from PostgreSQL src/include/catalog/catalog.h

use crate::access::attnum::AttrNumber;
use crate::catalog::pg_class::Form_pg_class;
use crate::common::relpath::RelFileNumber;
use crate::postgres_ext::Oid;
use crate::utils::relcache::Relation;

pub fn IsSystemRelation(relation: Relation) -> bool {
    unimplemented!()
}

pub fn IsToastRelation(relation: Relation) -> bool {
    unimplemented!()
}

pub fn IsCatalogRelation(relation: Relation) -> bool {
    unimplemented!()
}

pub fn IsInplaceUpdateRelation(relation: Relation) -> bool {
    unimplemented!()
}

pub fn IsSystemClass(relid: Oid, reltuple: Form_pg_class) -> bool {
    unimplemented!()
}

pub fn IsToastClass(reltuple: Form_pg_class) -> bool {
    unimplemented!()
}

pub fn IsCatalogRelationOid(relid: Oid) -> bool {
    unimplemented!()
}

pub fn IsCatalogTextUniqueIndexOid(relid: Oid) -> bool {
    unimplemented!()
}

pub fn IsInplaceUpdateOid(relid: Oid) -> bool {
    unimplemented!()
}

pub fn IsCatalogNamespace(namespace_id: Oid) -> bool {
    unimplemented!()
}

pub fn IsToastNamespace(namespace_id: Oid) -> bool {
    unimplemented!()
}

pub fn IsReservedName(name: &str) -> bool {
    unimplemented!()
}

pub fn IsSharedRelation(relation_id: Oid) -> bool {
    unimplemented!()
}

pub fn IsPinnedObject(class_id: Oid, object_id: Oid) -> bool {
    unimplemented!()
}

pub fn GetNewOidWithIndex(relation: Relation, index_id: Oid, oidcolumn: AttrNumber) -> Oid {
    unimplemented!()
}

pub fn GetNewRelFileNumber(
    reltablespace: Oid,
    pg_class: Relation,
    relpersistence: u8,
) -> RelFileNumber {
    unimplemented!()
}
