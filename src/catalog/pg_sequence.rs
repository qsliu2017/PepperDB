//! Translated from PostgreSQL src/include/catalog/pg_sequence.h

use crate::postgres_ext::Oid;

pub const SequenceRelationId: Oid = Oid(2224);

#[repr(C)]
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

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_sequence_seqrelid: i32 = 1;
pub const Anum_pg_sequence_seqtypid: i32 = 2;
pub const Anum_pg_sequence_seqstart: i32 = 3;
pub const Anum_pg_sequence_seqincrement: i32 = 4;
pub const Anum_pg_sequence_seqmax: i32 = 5;
pub const Anum_pg_sequence_seqmin: i32 = 6;
pub const Anum_pg_sequence_seqcache: i32 = 7;
pub const Anum_pg_sequence_seqcycle: i32 = 8;
pub const Natts_pg_sequence: i32 = 8;

// DECLARE_UNIQUE_INDEX_PKEY(pg_sequence_seqrelid_index, 5002, SequenceRelidIndexId, ...)
// MAKE_SYSCACHE(SEQRELID, ...)
