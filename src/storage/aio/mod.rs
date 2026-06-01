//! Asynchronous I/O subsystem (postgres/src/backend/storage/aio).
//!
//! The shmem init/sizing, per-IO state machine, target dispatch, and SQL
//! introspection function (`pg_get_aios`) so far; the io_uring/worker method
//! backends are future work.

pub mod aio;
pub mod aio_callback;
pub mod aio_funcs;
pub mod aio_init;
pub mod aio_io;
pub mod aio_target;
pub mod method_sync;
pub mod method_worker;
pub mod read_stream;
