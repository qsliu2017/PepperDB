//! Frontend utility library (postgres/src/fe_utils + postgres/src/include/fe_utils).
//!
//! Shared helpers used by the client programs (psql, pg_dump, etc.). Header-only
//! type/prototype layer so far; the implementations are future work. Many of
//! these reference libpq frontend types (PGconn/PGresult/PQExpBuffer) that are
//! locally stubbed as c_void until the libpq frontend interface is ported.

pub mod astreamer;
pub mod cancel;
pub mod conditional;
pub mod connect_utils;
pub mod mbprint;
pub mod option_utils;
pub mod parallel_slot;
pub mod psqlscan;
pub mod psqlscan_int;
pub mod query_utils;
pub mod recovery_gen;
pub mod simple_list;
pub mod string_utils;
