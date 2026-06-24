//! Translated from PostgreSQL src/include/fe_utils/query_utils.h

// PGconn / PGresult are libpq client types (interfaces/libpq/libpq-fe.h), not
// part of this batch.
// TODO(struct-forward): repoint to crate::interfaces::libpq::{PGconn,PGresult} in Phase 2.
#[deprecated(note = "TODO(struct-forward): repoint to crate::interfaces::libpq::PGconn in Phase 2")]
pub struct PGconn {
    _private: (),
}
#[deprecated(note = "TODO(struct-forward): repoint to crate::interfaces::libpq::PGresult in Phase 2")]
pub struct PGresult {
    _private: (),
}

#[allow(deprecated)]
pub fn execute_query(_conn: &PGconn, _query: &str, _echo: bool) -> PGresult {
    unimplemented!()
}

#[allow(deprecated)]
pub fn execute_command(_conn: &PGconn, _query: &str, _echo: bool) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn execute_maintenance_command(_conn: &PGconn, _query: &str, _echo: bool) -> bool {
    unimplemented!()
}
