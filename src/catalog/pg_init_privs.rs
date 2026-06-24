//! Translated from PostgreSQL src/include/catalog/pg_init_privs.h

use crate::postgres_ext::Oid;

pub const InitPrivsRelationId: Oid = Oid(3394);

#[repr(C)]
pub struct FormData_pg_init_privs {
    pub objoid: Oid,
    pub classoid: Oid, // BKI_LOOKUP(pg_class)
    pub objsubid: i32,
    pub privtype: i8,
    // CATALOG_VARLEN (not in fixed part):
    pub initprivs: [Aclitem; 1], // aclitem[1]; TODO(struct-forward)
}

// aclitem placeholder; real def lives in utils/acl.h.
#[deprecated(note = "TODO(struct-forward): repoint to crate::utils::acl::AclItem in Phase 2")]
#[repr(C)]
pub struct Aclitem {
    pub ai_grantee: Oid,
    pub ai_grantor: Oid,
    pub ai_privs: u64,
}

pub type Form_pg_init_privs = *mut FormData_pg_init_privs; // TODO(ptr)

// DECLARE_TOAST(pg_init_privs, 4155, 4156)
// DECLARE_UNIQUE_INDEX_PKEY(pg_init_privs_o_c_o_index, 3395, InitPrivsObjIndexId)

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_init_privs_objoid: i32 = 1;
pub const Anum_pg_init_privs_classoid: i32 = 2;
pub const Anum_pg_init_privs_objsubid: i32 = 3;
pub const Anum_pg_init_privs_privtype: i32 = 4;
pub const Anum_pg_init_privs_initprivs: i32 = 5;
pub const Natts_pg_init_privs: i32 = 5;

#[repr(i8)]
pub enum InitPrivsType {
    Initdb = b'i' as i8,
    Extension = b'e' as i8,
}
