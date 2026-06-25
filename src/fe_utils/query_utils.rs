//! Translated from PostgreSQL src/include/fe_utils/query_utils.h

/// Opaque frontend libpq handle; client lib not ported.
pub struct PGconn {
    _private: (),
}
/// Opaque frontend libpq handle; client lib not ported.
pub struct PGresult {
    _private: (),
}

pub fn execute_query(_conn: &PGconn, _query: &str, _echo: bool) -> PGresult {
    unimplemented!()
}

pub fn execute_command(_conn: &PGconn, _query: &str, _echo: bool) {
    unimplemented!()
}

pub fn execute_maintenance_command(_conn: &PGconn, _query: &str, _echo: bool) -> bool {
    unimplemented!()
}
