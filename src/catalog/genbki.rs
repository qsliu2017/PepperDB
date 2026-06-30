//! Translated from PostgreSQL src/include/catalog/genbki.h
//!
//! genbki.h defines the CATALOG()/BKI_*/DECLARE_* macros that let catalog header
//! files be read by both the C compiler and genbki.pl. In this port the BKI
//! pipeline is replaced by the `#[derive(Catalog)]` proc-macro (Anum_*/Natts_*)
//! plus build-time codegen, so those macros have no runtime equivalent.
//!
//! What genbki additionally emits into the `pg_*_d.h` headers - the symbolic OID
//! constants (each row's `oid_symbol`, plus pg_type's `<TYPNAME>OID`/`ARRAYOID`) -
//! is generated here by `build.rs` from the `catalog/*.dat` files (kept verbatim
//! in the `ref/postgres` submodule) and included below. (DECLARE_INDEX / TOAST /
//! MAKE_SYSCACHE remain catalog metadata handled elsewhere.)

use crate::postgres_ext::Oid;

include!(concat!(env!("OUT_DIR"), "/catalog_oids_generated.rs"));

// Pin a few generated OIDs against known PostgreSQL values (genbki fidelity).
const _: () = assert!(BOOLOID.get() == 16 && INT4OID.get() == 23 && TIMESTAMPOID.get() == 1114);
const _: () = assert!(BOOL_BTREE_FAM_OID.get() == 424 && INT4ARRAYOID.get() == 1007);
