//! Translated from PostgreSQL src/include/catalog/pg_ts_parser.h

use crate::c::{NameData, regproc};
use crate::postgres_ext::Oid;

pub const TSParserRelationId: Oid = Oid(3601);

#[repr(C)]
pub struct FormData_pg_ts_parser {
    pub oid: Oid,
    pub prsname: NameData,
    pub prsnamespace: Oid,
    pub prsstart: regproc,
    pub prstoken: regproc,
    pub prsend: regproc,
    pub prsheadline: regproc,
    pub prslextype: regproc,
}

pub type Form_pg_ts_parser = *mut FormData_pg_ts_parser; // TODO(ptr)

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_ts_parser_oid: i32 = 1;
pub const Anum_pg_ts_parser_prsname: i32 = 2;
pub const Anum_pg_ts_parser_prsnamespace: i32 = 3;
pub const Anum_pg_ts_parser_prsstart: i32 = 4;
pub const Anum_pg_ts_parser_prstoken: i32 = 5;
pub const Anum_pg_ts_parser_prsend: i32 = 6;
pub const Anum_pg_ts_parser_prsheadline: i32 = 7;
pub const Anum_pg_ts_parser_prslextype: i32 = 8;
pub const Natts_pg_ts_parser: i32 = 8;

// DECLARE_UNIQUE_INDEX(pg_ts_parser_prsname_index, 3606, TSParserNameNspIndexId, ...)
// DECLARE_UNIQUE_INDEX_PKEY(pg_ts_parser_oid_index, 3607, TSParserOidIndexId, ...)
// MAKE_SYSCACHE(TSPARSERNAMENSP, ...); MAKE_SYSCACHE(TSPARSEROID, ...)
