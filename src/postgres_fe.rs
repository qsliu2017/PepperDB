//! Translated from PostgreSQL src/include/postgres_fe.h
//!
//! Tombstone: the frontend primary include. In C it only sets `FRONTEND` and
//! pulls in `c.h` + `common/fe_memutils.h` for client programs. The backend uses
//! `postgres.h` instead (which adds the `Datum` family). In this single-binary
//! Rust port there is no frontend/backend split, so there are no items to carry
//! over - use `crate::c` and `crate::common::fe_memutils` directly.
