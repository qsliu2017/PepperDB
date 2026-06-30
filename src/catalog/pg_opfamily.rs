//! Translated from PostgreSQL src/include/catalog/pg_opfamily.h

use crate::c::NameData;
use crate::postgres_ext::Oid;

pub const OperatorFamilyRelationId: Oid = Oid::new(2753);

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_opfamily {
    pub oid: Oid,
    pub opfmethod: Oid,
    pub opfname: NameData,
    pub opfnamespace: Oid,
    pub opfowner: Oid,
}

pub type Form_pg_opfamily = *mut FormData_pg_opfamily; // TODO(ptr)

// DECLARE_UNIQUE_INDEX(pg_opfamily_am_name_nsp_index, 2754, OpfamilyAmNameNspIndexId, ...)
// DECLARE_UNIQUE_INDEX_PKEY(pg_opfamily_oid_index, 2755, OpfamilyOidIndexId, ...)
// MAKE_SYSCACHE(OPFAMILYAMNAMENSP, ...); MAKE_SYSCACHE(OPFAMILYOID, ...)

// Well-known opfamily OIDs (pg_opfamily.dat `oid_symbol` rows) are generated into
// crate::catalog::genbki by build.rs.
use crate::catalog::genbki::{BOOL_BTREE_FAM_OID, BOOL_HASH_FAM_OID};

pub fn IsBuiltinBooleanOpfamily(opfamily: Oid) -> bool {
    opfamily == BOOL_BTREE_FAM_OID || opfamily == BOOL_HASH_FAM_OID
}
