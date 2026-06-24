//! Translated from PostgreSQL src/include/fe_utils/recovery_gen.h

// PGconn (libpq-fe.h) and PQExpBuffer (pqexpbuffer.h) are external client types.
// TODO(struct-forward): repoint to crate::interfaces::libpq::{PGconn,PQExpBuffer} in Phase 2.
#[deprecated(note = "TODO(struct-forward): repoint to crate::interfaces::libpq::PGconn in Phase 2")]
pub struct PGconn {
    _private: (),
}
// PQExpBuffer is a resizable string buffer; an owned String is the idiomatic
// stand-in until the libpq client buffer type is ported.
// TODO(struct-forward): repoint to crate::interfaces::libpq::pqexpbuffer::PQExpBuffer in Phase 2.
pub type PQExpBuffer = String;

/// recovery configuration is part of postgresql.conf in v12+.
pub const MINIMUM_VERSION_FOR_RECOVERY_GUC: i32 = 120000;

#[allow(deprecated)]
pub fn generate_recovery_config(
    _pgconn: &PGconn,
    _replication_slot: &str,
    _dbname: &str,
) -> PQExpBuffer {
    unimplemented!()
}

#[allow(deprecated)]
pub fn write_recovery_config(_pgconn: &PGconn, _target_dir: &str, _contents: PQExpBuffer) {
    unimplemented!()
}

pub fn get_dbname_from_connection_options(_connstr: &str) -> Option<String> {
    unimplemented!()
}
