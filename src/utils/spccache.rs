//! Translated from PostgreSQL src/include/utils/spccache.h
//
// Tablespace cache lookups. get_tablespace_page_costs has two float8 out-params ->
// returned as a tuple (random_page_cost, seq_page_cost).

use crate::postgres_ext::Oid;

/// Returns (spc_random_page_cost, spc_seq_page_cost).
pub fn get_tablespace_page_costs(_spcid: Oid) -> (f64, f64) {
    unimplemented!()
}

pub fn get_tablespace_io_concurrency(_spcid: Oid) -> i32 {
    unimplemented!()
}

pub fn get_tablespace_maintenance_io_concurrency(_spcid: Oid) -> i32 {
    unimplemented!()
}
