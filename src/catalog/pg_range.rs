//! Translated from PostgreSQL src/include/catalog/pg_range.h

use crate::c::{RegProcedure, regproc};
use crate::postgres_ext::Oid;

pub const RangeRelationId: Oid = Oid(3541);

#[repr(C)]
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

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_range_rngtypid: i32 = 1;
pub const Anum_pg_range_rngsubtype: i32 = 2;
pub const Anum_pg_range_rngmultitypid: i32 = 3;
pub const Anum_pg_range_rngcollation: i32 = 4;
pub const Anum_pg_range_rngsubopc: i32 = 5;
pub const Anum_pg_range_rngcanonical: i32 = 6;
pub const Anum_pg_range_rngsubdiff: i32 = 7;
pub const Natts_pg_range: i32 = 7;

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
