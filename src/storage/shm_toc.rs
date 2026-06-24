//! Translated from PostgreSQL src/include/storage/shm_toc.h
//!
//! TOMBSTONE. `shm_toc` is the table-of-contents for carving a dynamic
//! shared-memory segment into keyed regions (used to hand state from the leader
//! to parallel workers). Under the single-process async model there is no shared
//! segment: parallel-query state is just `Arc`-shared heap data, and the
//! leader-to-worker handoff becomes ordinary moves / `tokio` channels.
//!
//! Replacement mapping:
//!   - `shm_toc_create` / `shm_toc_attach` -> construct an `Arc<T>` (or a struct
//!     of `Arc` fields) and clone it into each spawned task.
//!   - `shm_toc_insert(key, ptr)` / `shm_toc_lookup(key)` -> typed struct fields
//!     or a `HashMap<u64, Arc<...>>` behind the shared state.
//!   - `shm_toc_allocate` / `shm_toc_freespace` / `shm_toc_estimate*`
//!     (the `shm_toc_estimator` sizing helpers) -> obsolete; the Rust allocator
//!     sizes heap allocations, so no pre-estimation is needed.
//!
//! Nothing to translate here.
