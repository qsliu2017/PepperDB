//! Translated from PostgreSQL src/include/catalog/pg_language.h

use crate::c::NameData;
use crate::postgres_ext::Oid;

pub const LanguageRelationId: Oid = Oid(2612);

#[repr(C)]
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
    pub lanacl: [Aclitem; 1], // aclitem[1]; TODO(struct-forward)
}

// aclitem placeholder; real def lives in utils/acl.h.
#[deprecated(note = "TODO(struct-forward): repoint to crate::utils::acl::AclItem in Phase 2")]
#[repr(C)]
pub struct Aclitem {
    pub ai_grantee: Oid,
    pub ai_grantor: Oid,
    pub ai_privs: u64,
}

pub type Form_pg_language = *mut FormData_pg_language; // TODO(ptr)

// DECLARE_TOAST(pg_language, 4157, 4158)
// DECLARE_UNIQUE_INDEX(pg_language_name_index, 2681, LanguageNameIndexId)
// DECLARE_UNIQUE_INDEX_PKEY(pg_language_oid_index, 2682, LanguageOidIndexId)
// MAKE_SYSCACHE(LANGNAME, pg_language_name_index, 4)
// MAKE_SYSCACHE(LANGOID, pg_language_oid_index, 4)

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_language_oid: i32 = 1;
pub const Anum_pg_language_lanname: i32 = 2;
pub const Anum_pg_language_lanowner: i32 = 3;
pub const Anum_pg_language_lanispl: i32 = 4;
pub const Anum_pg_language_lanpltrusted: i32 = 5;
pub const Anum_pg_language_lanplcallfoid: i32 = 6;
pub const Anum_pg_language_laninline: i32 = 7;
pub const Anum_pg_language_lanvalidator: i32 = 8;
pub const Anum_pg_language_lanacl: i32 = 9;
pub const Natts_pg_language: i32 = 9;
