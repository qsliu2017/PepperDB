//! Translated from PostgreSQL src/include/catalog/pg_am.h

use crate::c::{regproc, NameData};
use crate::postgres_ext::Oid;

pub const AccessMethodRelationId: Oid = Oid(2601);

#[repr(C)]
pub struct FormData_pg_am {
    pub oid: Oid,
    pub amname: NameData,
    pub amhandler: regproc, // BKI_LOOKUP(pg_proc)
    pub amtype: i8,
}

pub type Form_pg_am = *mut FormData_pg_am; // TODO(ptr)

// DECLARE_UNIQUE_INDEX(pg_am_name_index, 2651, AmNameIndexId)
// DECLARE_UNIQUE_INDEX_PKEY(pg_am_oid_index, 2652, AmOidIndexId)
// MAKE_SYSCACHE(AMNAME, pg_am_name_index, 4)
// MAKE_SYSCACHE(AMOID, pg_am_oid_index, 4)

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_am_oid: i32 = 1;
pub const Anum_pg_am_amname: i32 = 2;
pub const Anum_pg_am_amhandler: i32 = 3;
pub const Anum_pg_am_amtype: i32 = 4;
pub const Natts_pg_am: i32 = 4;

// Allowed values for amtype
pub const AMTYPE_INDEX: i8 = b'i' as i8;
pub const AMTYPE_TABLE: i8 = b't' as i8;
