//! Translated from PostgreSQL src/include/utils/pg_rusage.h
//
// Resource usage measurement (wall clock + getrusage). In-memory state. The C
// struct holds `struct timeval` + `struct rusage`; wall time maps to Instant and
// the CPU/rusage fields are kept for the user/sys time deltas pg_rusage_show prints.

use std::time::Instant;

/// State for pg_rusage_init/pg_rusage_show.
pub struct PGRUsage {
    pub tv: Instant,
    // getrusage() snapshot. TODO(rusage): fill user/sys time via libc::getrusage.
    pub user_sec: i64,
    pub user_usec: i64,
    pub sys_sec: i64,
    pub sys_usec: i64,
}

pub fn pg_rusage_init(_ru0: &mut PGRUsage) {
    unimplemented!()
}

pub fn pg_rusage_show(_ru0: &PGRUsage) -> String {
    unimplemented!()
}
