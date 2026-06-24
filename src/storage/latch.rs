//! Tombstone: src/include/storage/latch.h
//!
//! Latch (process wakeup) replaced by tokio::sync::Notify. Callers await a Notify instead of WaitLatch.
