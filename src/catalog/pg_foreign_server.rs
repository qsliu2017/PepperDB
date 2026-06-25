//! Translated from PostgreSQL src/include/catalog/pg_foreign_server.h

use crate::c::{text, NameData};
use crate::postgres_ext::Oid;

pub const ForeignServerRelationId: Oid = Oid(1417);

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_foreign_server {
    pub oid: Oid,
    pub srvname: NameData,
    pub srvowner: Oid, // BKI_LOOKUP(pg_authid)
    pub srvfdw: Oid,   // BKI_LOOKUP(pg_foreign_data_wrapper)
    // CATALOG_VARLEN (not in fixed part):
    pub srvtype: text,
    pub srvversion: text,
    pub srvacl: [Aclitem; 1], // aclitem[1]; TODO(struct-forward)
    pub srvoptions: [text; 1],
}

// aclitem placeholder; real def lives in utils/acl.h.
#[deprecated(note = "TODO(struct-forward): repoint to crate::utils::acl::AclItem in Phase 2")]
#[repr(C)]
pub struct Aclitem {
    pub ai_grantee: Oid,
    pub ai_grantor: Oid,
    pub ai_privs: u64,
}

pub type Form_pg_foreign_server = *mut FormData_pg_foreign_server; // TODO(ptr)

// DECLARE_TOAST(pg_foreign_server, 4151, 4152)
// DECLARE_UNIQUE_INDEX_PKEY(pg_foreign_server_oid_index, 113, ForeignServerOidIndexId)
// DECLARE_UNIQUE_INDEX(pg_foreign_server_name_index, 549, ForeignServerNameIndexId)
// MAKE_SYSCACHE(FOREIGNSERVEROID, pg_foreign_server_oid_index, 2)
// MAKE_SYSCACHE(FOREIGNSERVERNAME, pg_foreign_server_name_index, 2)

