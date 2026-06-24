//! Translated from PostgreSQL src/include/utils/uuid.h
//
// The "uuid" ADT. On-disk: pg_uuid_t is exactly 16 raw bytes, so #[repr(C)] with a
// layout assert. The fmgr interface helpers (UUIDPGetDatum/DatumGetUUIDP) bridge to
// the Datum representation; left as TODO(fmgr) stubs since DatumGetPointer/
// PointerGetDatum live in fmgr.h/postgres.h and are not in this batch.

use crate::postgres::Datum;

pub const UUID_LEN: usize = 16;

/// Named pg_uuid_t in C to avoid clashing with system uuid_t. On-disk: 16 bytes.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct pg_uuid_t {
    pub data: [u8; UUID_LEN],
}
const _: () = assert!(core::mem::size_of::<pg_uuid_t>() == 16);

// === fmgr interface ===

/// UUIDPGetDatum: pointer-to-uuid as a Datum.
// TODO(fmgr): route through PointerGetDatum once postgres.h/fmgr.h is translated.
pub fn UUIDPGetDatum(_x: &pg_uuid_t) -> Datum {
    unimplemented!()
}

/// DatumGetUUIDP: a Datum back to a pg_uuid_t pointer.
// TODO(fmgr): route through DatumGetPointer once postgres.h/fmgr.h is translated.
pub fn DatumGetUUIDP(_x: Datum) -> *mut pg_uuid_t {
    unimplemented!()
}
