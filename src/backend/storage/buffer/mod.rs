//! Directory module: src/backend/storage/buffer
//!
//! The shared buffer manager. Part A: the buffer pool allocation (`buf_init`),
//! the sharded tag->buffer map (`buf_table`), and the clock-sweep replacement
//! strategy (`freelist`). Part B: the read/pin/dirty/flush I/O entry points
//! (`bufmgr`) on top of that core. Part C: the per-task local-relation buffers
//! (`localbuf`) for temp tables.

pub mod buf_init;
pub mod buf_table;
pub mod bufmgr;
pub mod freelist;
pub mod localbuf;
