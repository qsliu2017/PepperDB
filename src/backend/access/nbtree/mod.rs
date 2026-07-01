//! Directory module: src/backend/access/nbtree
//!
//! The btree (nbtree) access method bodies. Headers live under `src/access/`
//! (`nbtree.rs`, `nbtutils` decls, ...) and re-export these via `pub use`.

pub mod nbtcompare;
pub mod nbtinsert;
pub mod nbtpage;
pub mod nbtsearch;
pub mod nbtsort;
pub mod nbtree;
pub mod nbtutils;
pub mod nbtxlog;
