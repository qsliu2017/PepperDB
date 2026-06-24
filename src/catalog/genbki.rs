//! Translated from PostgreSQL src/include/catalog/genbki.h
//!
//! genbki.h defines the CATALOG()/BKI_*/DECLARE_* macros that let catalog
//! header files be read by both the C compiler and genbki.pl. In this port the
//! BKI bootstrap pipeline is replaced by a `#[derive(Catalog)]` proc-macro and
//! build-time codegen, so these macros have no runtime equivalent and become
//! no-op markers. This module is intentionally a tombstone: the only carried-
//! over symbol is the catalog-version dependency note.
//!
//! TODO(generated): the CATALOG()/DECLARE_INDEX/DECLARE_TOAST/MAKE_SYSCACHE
//! family is handled by attribute macros + build.rs codegen in a later phase.
