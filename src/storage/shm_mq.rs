//! Translated from PostgreSQL src/include/storage/shm_mq.h
//!
//! TOMBSTONE (deleted/replaced: dsm-dsa). The single-reader/single-writer shared
//! memory message queue is replaced by tokio mpsc channels for parallel query.
//!
//! Mapping for dependents:
//!   - `shm_mq_create` / `shm_mq_attach`        -> `tokio::sync::mpsc::channel()`
//!   - `shm_mq_set_sender` / `shm_mq_set_receiver` -> holding the `Sender` / `Receiver`
//!   - `shm_mq_send` / `shm_mq_sendv`           -> `Sender::send` / `try_send`
//!   - `shm_mq_receive`                          -> `Receiver::recv` / `try_recv`
//!   - `shm_mq_detach`                           -> dropping the channel half
//!   - `shm_mq_result` (SUCCESS/WOULD_BLOCK/DETACHED) -> the channel's
//!     `Result`/`TryRecvError`/`SendError` (closed channel == DETACHED).
//!
//! No Rust items are emitted; this header has no callers in the port.
