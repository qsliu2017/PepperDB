//! Translated from PostgreSQL src/include/storage/barrier.h
//!
//! TOMBSTONE. PG's `barrier.h` shim carries no real definitions of its own; the
//! header notes that atomics + compiler/memory barriers moved to
//! `port/atomics.h`, which is replaced wholesale by `core::sync::atomic`:
//!   - `pg_memory_barrier()`   -> `core::sync::atomic::fence(Ordering::SeqCst)`
//!   - `pg_read_barrier()`     -> `fence(Ordering::Acquire)`
//!   - `pg_write_barrier()`    -> `fence(Ordering::Release)`
//!   - `pg_compiler_barrier()` -> `core::sync::atomic::compiler_fence(Ordering::SeqCst)`
//!
//! The `Barrier` struct (cooperating-process phase barrier) is a parallel-query
//! construct; under the single-process async model it maps to tokio
//! synchronization (`tokio::sync::Barrier`/`Notify`) and is reintroduced where
//! parallel query is implemented. Nothing to translate here.
