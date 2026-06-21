//! Storage manager (postgres/src/backend/storage/smgr).
//!
//! So far: the bulk-write facility (`bulk_write`).

pub mod bulk_write;
pub mod md;
pub mod smgr;
