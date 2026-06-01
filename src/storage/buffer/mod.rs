//! Buffer manager (postgres/src/backend/storage/buffer).
//!
//! So far: the shared buffer lookup hash table (`buf_table`).

pub mod buf_init;
pub mod buf_table;
pub mod freelist;
pub mod localbuf;
pub mod bufmgr;
