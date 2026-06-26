//! Translated from PostgreSQL src/include/postmaster/pgarch.h
//!
//! Non-type-centric header: the archivable-WAL-name constants live here (the
//! backend module imports them); the functions re-export the backend
//! implementation (`pub use`) under their C names. Bodies live in
//! `crate::backend::postmaster::pgarch`.

// Archivable WAL file name constraints.
pub const MIN_XFN_CHARS: usize = 16;
pub const MAX_XFN_CHARS: usize = 40;
pub const VALID_XFN_CHARS: &str = "0123456789ABCDEF.history.backup.partial";

pub use crate::backend::postmaster::pgarch::{
    pgarch_can_restart as PgArchCanRestart, pgarch_force_dir_scan as PgArchForceDirScan,
    pgarch_main as PgArchiverMain, pgarch_wakeup as PgArchWakeup,
};
