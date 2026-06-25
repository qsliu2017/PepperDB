//! Translated from PostgreSQL src/include/fe_utils/connect_utils.h

/// Opaque frontend libpq handle; client lib not ported.
pub struct PGconn {
    _private: (),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Trivalue {
    Default,
    No,
    Yes,
}

/// Parameters needed by connectDatabase/connectMaintenanceDatabase.
pub struct ConnParams {
    pub dbname: Option<String>, // may be a connstring
    pub pghost: Option<String>,
    pub pgport: Option<String>,
    pub pguser: Option<String>,
    pub prompt_password: Trivalue,
    /// Overrides only the DB name from the command line (not the connstring).
    pub override_dbname: Option<String>,
}

pub fn connect_database(
    _cparams: &ConnParams,
    _progname: &str,
    _echo: bool,
    _fail_ok: bool,
    _allow_password_reuse: bool,
) -> PGconn {
    unimplemented!()
}

pub fn connect_maintenance_database(
    _cparams: &ConnParams,
    _progname: &str,
    _echo: bool,
) -> PGconn {
    unimplemented!()
}

pub fn disconnect_database(_conn: &PGconn) {
    unimplemented!()
}
