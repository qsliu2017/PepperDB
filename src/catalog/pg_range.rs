//! Translated from PostgreSQL src/include/catalog/pg_range.h

use crate::c::{RegProcedure, regproc};
use crate::postgres_ext::Oid;

pub const RangeRelationId: Oid = Oid::new(3541);

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_range {
    pub rngtypid: Oid,
    pub rngsubtype: Oid,
    pub rngmultitypid: Oid,
    pub rngcollation: Oid,
    pub rngsubopc: Oid,
    pub rngcanonical: regproc,
    pub rngsubdiff: regproc,
}

pub type Form_pg_range = *mut FormData_pg_range; // TODO(ptr)

// DECLARE_UNIQUE_INDEX_PKEY(pg_range_rngtypid_index, 3542, RangeTypidIndexId, ...)
// DECLARE_UNIQUE_INDEX(pg_range_rngmultitypid_index, 2228, RangeMultirangeTypidIndexId, ...)
// MAKE_SYSCACHE(RANGETYPE, ...); MAKE_SYSCACHE(RANGEMULTIRANGE, ...)

pub fn RangeCreate(
    _range_type_oid: Oid,
    _range_sub_type: Oid,
    _range_collation: Oid,
    _range_sub_opclass: Oid,
    _range_canonical: RegProcedure,
    _range_sub_diff: RegProcedure,
    _multirange_type_oid: Oid,
) {
    unimplemented!()
}
pub fn RangeDelete(_range_type_oid: Oid) {
    unimplemented!()
}
