//! Translated from PostgreSQL src/include/catalog/catalog.h
//!
//! The bodies live in `crate::backend::catalog::catalog`; this header re-exports
//! the snake_case definitions under their C names (rules.md s3). The M2 path
//! implements the OID/relfilenumber allocators and the system/catalog/pinned
//! predicates; the remaining toast/inplace/text-unique helpers stay staged stubs.

use crate::catalog::pg_class::Form_pg_class;
use crate::postgres_ext::Oid;

pub use crate::backend::catalog::catalog::{
    get_new_oid_with_index as GetNewOidWithIndex, get_new_rel_file_number as GetNewRelFileNumber,
    is_catalog_namespace as IsCatalogNamespace, is_catalog_relation as IsCatalogRelation,
    is_catalog_relation_oid as IsCatalogRelationOid, is_pinned_object as IsPinnedObject,
    is_shared_relation as IsSharedRelation, is_system_class as IsSystemClass,
    is_system_relation as IsSystemRelation, is_toast_namespace as IsToastNamespace,
};

pub fn IsToastRelation(relation: &crate::utils::rel::RelationData) -> bool {
    let _ = relation;
    unimplemented!()
}

pub fn IsInplaceUpdateRelation(relation: &crate::utils::rel::RelationData) -> bool {
    let _ = relation;
    unimplemented!()
}

pub fn IsToastClass(reltuple: Form_pg_class) -> bool {
    let _ = reltuple;
    unimplemented!()
}

pub fn IsCatalogTextUniqueIndexOid(relid: Oid) -> bool {
    let _ = relid;
    unimplemented!()
}

pub fn IsInplaceUpdateOid(relid: Oid) -> bool {
    let _ = relid;
    unimplemented!()
}

pub fn IsReservedName(name: &str) -> bool {
    let _ = name;
    unimplemented!()
}
