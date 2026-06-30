//! Translated from PostgreSQL src/include/catalog/pg_sequence.h

use crate::postgres_ext::Oid;

pub const SequenceRelationId: Oid = Oid(2224);
/// pg_sequence composite rowtype OID. Nails the descriptor at bootstrap.
pub const SequenceRelation_Rowtype_Id: Oid = Oid(11633);
/// pg_sequence_seqrelid_index (pkey on seqrelid).
pub const SequenceRelidIndexId: Oid = Oid(5002);

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_sequence {
    pub seqrelid: Oid,
    pub seqtypid: Oid,
    pub seqstart: i64,
    pub seqincrement: i64,
    pub seqmax: i64,
    pub seqmin: i64,
    pub seqcache: i64,
    pub seqcycle: bool,
}

pub type Form_pg_sequence = *mut FormData_pg_sequence; // TODO(ptr)

// DECLARE_UNIQUE_INDEX_PKEY(pg_sequence_seqrelid_index, 5002, SequenceRelidIndexId, ...)
// MAKE_SYSCACHE(SEQRELID, ...)
