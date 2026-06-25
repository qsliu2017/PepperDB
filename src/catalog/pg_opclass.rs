//! Translated from PostgreSQL src/include/catalog/pg_opclass.h

use crate::c::NameData;
use crate::postgres_ext::Oid;

// CATALOG(pg_opclass,2616,OperatorClassRelationId)
pub const OperatorClassRelationId: Oid = Oid(2616);

/// On-disk catalog row for pg_opclass.
#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_opclass {
    pub oid: Oid,
    pub opcmethod: Oid,        // index access method opclass is for (BKI_LOOKUP pg_am)
    pub opcname: NameData,     // name of this opclass
    pub opcnamespace: Oid,     // namespace of this opclass
    pub opcowner: Oid,         // opclass owner
    pub opcfamily: Oid,        // containing operator family
    pub opcintype: Oid,        // type of data indexed by opclass
    pub opcdefault: bool,      // T if opclass is default for opcintype
    pub opckeytype: Oid,       // type of data in index, or InvalidOid if same as input
}

pub type Form_pg_opclass = *mut FormData_pg_opclass; // TODO(ptr)

// DECLARE_UNIQUE_INDEX(pg_opclass_am_name_nsp_index, 2686, OpclassAmNameNspIndexId, ...)
pub const OpclassAmNameNspIndexId: Oid = Oid(2686);
// DECLARE_UNIQUE_INDEX_PKEY(pg_opclass_oid_index, 2687, OpclassOidIndexId, ...)
pub const OpclassOidIndexId: Oid = Oid(2687);
// MAKE_SYSCACHE(CLAAMNAMENSP, ...); MAKE_SYSCACHE(CLAOID, ...) - syscaches (later)
