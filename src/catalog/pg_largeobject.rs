//! Translated from PostgreSQL src/include/catalog/pg_largeobject.h

use crate::c::bytea;
use crate::postgres_ext::Oid;
use crate::utils::snapshot::Snapshot;

pub const LargeObjectRelationId: Oid = Oid::new(2613);

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_largeobject {
    pub loid: Oid, // BKI_LOOKUP(pg_largeobject_metadata)
    pub pageno: i32,
    pub data: bytea, // BKI_FORCE_NOT_NULL; variable length, direct access (inv_api.c)
}

pub type Form_pg_largeobject = *mut FormData_pg_largeobject; // TODO(ptr)

// DECLARE_UNIQUE_INDEX_PKEY(pg_largeobject_loid_pn_index, 2683, LargeObjectLOidPNIndexId)

pub fn LargeObjectCreate(_loid: Oid) -> Oid {
    unimplemented!()
}

pub fn LargeObjectDrop(_loid: Oid) {
    unimplemented!()
}

pub fn LargeObjectExists(_loid: Oid) -> bool {
    unimplemented!()
}

pub fn LargeObjectExistsWithSnapshot(_loid: Oid, _snapshot: Snapshot) -> bool {
    unimplemented!()
}
