//! Translated from PostgreSQL src/include/catalog/pg_opfamily.h

use crate::c::NameData;
use crate::postgres_ext::Oid;

pub const OperatorFamilyRelationId: Oid = Oid(2753);

#[repr(C)]
pub struct FormData_pg_opfamily {
    pub oid: Oid,
    pub opfmethod: Oid,
    pub opfname: NameData,
    pub opfnamespace: Oid,
    pub opfowner: Oid,
}

pub type Form_pg_opfamily = *mut FormData_pg_opfamily; // TODO(ptr)

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_opfamily_oid: i32 = 1;
pub const Anum_pg_opfamily_opfmethod: i32 = 2;
pub const Anum_pg_opfamily_opfname: i32 = 3;
pub const Anum_pg_opfamily_opfnamespace: i32 = 4;
pub const Anum_pg_opfamily_opfowner: i32 = 5;
pub const Natts_pg_opfamily: i32 = 5;

// DECLARE_UNIQUE_INDEX(pg_opfamily_am_name_nsp_index, 2754, OpfamilyAmNameNspIndexId, ...)
// DECLARE_UNIQUE_INDEX_PKEY(pg_opfamily_oid_index, 2755, OpfamilyOidIndexId, ...)
// MAKE_SYSCACHE(OPFAMILYAMNAMENSP, ...); MAKE_SYSCACHE(OPFAMILYOID, ...)

pub fn IsBuiltinBooleanOpfamily(opfamily: Oid) -> bool {
    opfamily == BOOL_BTREE_FAM_OID || opfamily == BOOL_HASH_FAM_OID
}

// BOOL_BTREE_FAM_OID / BOOL_HASH_FAM_OID come from pg_opfamily.dat (seed rows).
// TODO(catalog-derive): provide these well-known opfamily OIDs from the .dat build.
pub const BOOL_BTREE_FAM_OID: Oid = Oid(424);
pub const BOOL_HASH_FAM_OID: Oid = Oid(2222);
