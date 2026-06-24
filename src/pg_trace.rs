//! Translated from PostgreSQL src/include/pg_trace.h
//
// Tombstone: DTrace tracing framework. The C header only re-exports the
// generated utils/probes.h SDT probe macros (TRACE_POSTGRESQL_*). DTrace static
// probes are not ported; tracing maps to the `tracing` crate later.
