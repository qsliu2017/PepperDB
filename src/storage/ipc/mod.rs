//! Inter-process communication primitives (postgres/src/backend/storage/ipc).
//!
//! So far: the shared-memory table-of-contents (`shm_toc`).

pub mod shm_mq;
pub mod dsm;
pub mod barrier;
pub mod dsm_registry;
pub mod ipc;
pub mod ipci;
pub mod latch;
pub mod pmsignal;
pub mod shm_toc;
pub mod signalfuncs;
pub mod sinval;
pub mod sinvaladt;
pub mod shmem;
pub mod procsignal;
pub mod dsm_impl;
pub mod procarray;
