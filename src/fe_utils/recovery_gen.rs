//! Translated from PostgreSQL src/include/fe_utils/recovery_gen.h

use crate::fe_utils::string_utils::PQExpBuffer;

/// Opaque frontend libpq handle; client lib not ported.
pub struct PGconn {
    _private: (),
}

/// recovery configuration is part of postgresql.conf in v12+.
pub const MINIMUM_VERSION_FOR_RECOVERY_GUC: i32 = 120000;

pub fn generate_recovery_config(
    _pgconn: &PGconn,
    _replication_slot: &str,
    _dbname: &str,
) -> PQExpBuffer {
    unimplemented!()
}

pub fn write_recovery_config(_pgconn: &PGconn, _target_dir: &str, _contents: PQExpBuffer) {
    unimplemented!()
}

pub fn get_dbname_from_connection_options(_connstr: &str) -> Option<String> {
    unimplemented!()
}
