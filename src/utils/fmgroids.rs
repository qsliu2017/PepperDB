//! Translated from PostgreSQL src/include/utils/fmgroids.h
//!
//! `F_*` builtin-function OID constants. In C these are emitted by
//! `Gen_fmgrtab.pl` from `pg_proc.dat`; here `build.rs` reads the same `.dat`
//! (kept verbatim in the `ref/postgres` submodule) and emits them into OUT_DIR.

use crate::postgres_ext::Oid;

include!(concat!(env!("OUT_DIR"), "/fmgroids_generated.rs"));
