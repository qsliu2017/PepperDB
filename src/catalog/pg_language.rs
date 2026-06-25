//! Translated from PostgreSQL src/include/catalog/pg_language.h

use crate::c::NameData;
use crate::postgres_ext::Oid;
use crate::utils::acl::AclItem;

pub const LanguageRelationId: Oid = Oid(2612);

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_language {
    pub oid: Oid,
    pub lanname: NameData,
    pub lanowner: Oid, // BKI_LOOKUP(pg_authid)
    pub lanispl: bool,
    pub lanpltrusted: bool,
    pub lanplcallfoid: Oid, // BKI_LOOKUP_OPT(pg_proc)
    pub laninline: Oid,     // BKI_LOOKUP_OPT(pg_proc)
    pub lanvalidator: Oid,  // BKI_LOOKUP_OPT(pg_proc)
    // CATALOG_VARLEN (not in fixed part):
    pub lanacl: [AclItem; 1], // aclitem[1]
}

pub type Form_pg_language = *mut FormData_pg_language; // TODO(ptr)

// DECLARE_TOAST(pg_language, 4157, 4158)
// DECLARE_UNIQUE_INDEX(pg_language_name_index, 2681, LanguageNameIndexId)
// DECLARE_UNIQUE_INDEX_PKEY(pg_language_oid_index, 2682, LanguageOidIndexId)
// MAKE_SYSCACHE(LANGNAME, pg_language_name_index, 4)
// MAKE_SYSCACHE(LANGOID, pg_language_oid_index, 4)

