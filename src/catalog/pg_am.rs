//! Translated from PostgreSQL src/include/catalog/pg_am.h

use crate::c::{regproc, NameData};
use crate::postgres_ext::Oid;

pub const AccessMethodRelationId: Oid = Oid(2601);

// `Anum_pg_am_*` and `Natts_pg_am` are emitted by #[derive(Catalog)] from the
// field order below (replacing genbki's pg_am_d.h consts).
#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
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

// Allowed values for amtype
pub const AMTYPE_INDEX: i8 = b'i' as i8;
pub const AMTYPE_TABLE: i8 = b't' as i8;

// Sanity: #[derive(Catalog)] reproduces the former hand-emitted _d.h consts.
const _: () = assert!(Anum_pg_am_oid == 1 && Anum_pg_am_amhandler == 3 && Natts_pg_am == 4);
