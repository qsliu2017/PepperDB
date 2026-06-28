//! Translated from PostgreSQL src/include/bootstrap/bootstrap.h
//!
//! The bodies live in `crate::backend::bootstrap::bootstrap`. The BKI text
//! pipeline (`bootparse.y` / `bootscanner.l`) is tombstoned: `build.rs` emits the
//! catalog schema + seed rows as Rust data (gating decision 2), and the bootstrap
//! driver consumes that directly. See the backend module for the details and the
//! tombstone note.

pub use crate::backend::bootstrap::bootstrap::{
    boot_get_type_io_data, bootstrap_catalogs, formrdesc, formrdesc_tupdesc, BootTypeIoData,
    BootstrapAttr, BootstrapCatalog, FORMRDESC_CATALOGS, MAXATTR,
};

// PG bootstrap-mode null-handling codes for a column (BKI `_null_` / forced).
pub const BOOTCOL_NULL_AUTO: i32 = 1;
pub const BOOTCOL_NULL_FORCE_NULL: i32 = 2;
pub const BOOTCOL_NULL_FORCE_NOT_NULL: i32 = 3;

/// PG `BootstrapModeMain` (`postgres --boot`). Re-exported under the C name; the
/// body documents that bootstrap is driven by [`bootstrap_catalogs`].
pub use crate::backend::bootstrap::bootstrap::boot_strap_mode_main as BootstrapModeMain;
