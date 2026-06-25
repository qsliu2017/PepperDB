//! Translated from PostgreSQL src/include/catalog/pg_ts_parser.h

use crate::c::{NameData, regproc};
use crate::postgres_ext::Oid;

pub const TSParserRelationId: Oid = Oid(3601);

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
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

// DECLARE_UNIQUE_INDEX(pg_ts_parser_prsname_index, 3606, TSParserNameNspIndexId, ...)
// DECLARE_UNIQUE_INDEX_PKEY(pg_ts_parser_oid_index, 3607, TSParserOidIndexId, ...)
// MAKE_SYSCACHE(TSPARSERNAMENSP, ...); MAKE_SYSCACHE(TSPARSEROID, ...)
