//! Translated from PostgreSQL src/include/catalog/pg_foreign_data_wrapper.h

use crate::c::{text, NameData};
use crate::postgres_ext::Oid;
use crate::utils::acl::AclItem;

pub const ForeignDataWrapperRelationId: Oid = Oid(2328);

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_foreign_data_wrapper {
    pub oid: Oid,
    pub fdwname: NameData,
    pub fdwowner: Oid,     // BKI_LOOKUP(pg_authid)
    pub fdwhandler: Oid,   // BKI_LOOKUP_OPT(pg_proc)
    pub fdwvalidator: Oid, // BKI_LOOKUP_OPT(pg_proc)
    // CATALOG_VARLEN (not in fixed part):
    pub fdwacl: [AclItem; 1], // aclitem[1]
    pub fdwoptions: [text; 1],
}

pub type Form_pg_foreign_data_wrapper = *mut FormData_pg_foreign_data_wrapper; // TODO(ptr)

// DECLARE_TOAST(pg_foreign_data_wrapper, 4149, 4150)
// DECLARE_UNIQUE_INDEX_PKEY(pg_foreign_data_wrapper_oid_index, 112, ForeignDataWrapperOidIndexId)
// DECLARE_UNIQUE_INDEX(pg_foreign_data_wrapper_name_index, 548, ForeignDataWrapperNameIndexId)
// MAKE_SYSCACHE(FOREIGNDATAWRAPPEROID, pg_foreign_data_wrapper_oid_index, 2)
// MAKE_SYSCACHE(FOREIGNDATAWRAPPERNAME, pg_foreign_data_wrapper_name_index, 2)

