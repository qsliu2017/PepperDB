//! Translated from PostgreSQL src/include/pg_getopt.h
//!
//! Tombstone. pg_getopt.h re-exports the platform `getopt(3)` plus the
//! `optarg`/`optind`/`opterr`/`optopt`/`optreset` globals. In the port,
//! command-line parsing is handled by `clap` (per translation-rules.md), so the
//! C getopt surface and its mutable globals are intentionally not translated.
