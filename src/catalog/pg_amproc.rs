//! Translated from PostgreSQL src/include/catalog/pg_amproc.h

use crate::c::regproc;
use crate::postgres_ext::Oid;

pub const AccessMethodProcedureRelationId: Oid = Oid(2603);

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_amproc {
    pub oid: Oid,
    pub amprocfamily: Oid,    // BKI_LOOKUP(pg_opfamily)
    pub amproclefttype: Oid,  // BKI_LOOKUP(pg_type)
    pub amprocrighttype: Oid, // BKI_LOOKUP(pg_type)
    pub amprocnum: i16,
    pub amproc: regproc, // BKI_LOOKUP(pg_proc)
}

pub type Form_pg_amproc = *mut FormData_pg_amproc; // TODO(ptr)

// DECLARE_UNIQUE_INDEX(pg_amproc_fam_proc_index, 2655, AccessMethodProcedureIndexId)
// DECLARE_UNIQUE_INDEX_PKEY(pg_amproc_oid_index, 2757, AccessMethodProcedureOidIndexId)
// MAKE_SYSCACHE(AMPROCNUM, pg_amproc_fam_proc_index, 16)

