//! Hash access method (postgres/src/backend/access/hash).
//!
//! Only the datatype hash support functions (hashfunc) and the opclass
//! validator (hashvalidate) are present so far.

pub mod hash;
pub mod hash_xlog;
pub mod hashovfl;
pub mod hashfunc;
pub mod hashinsert;
pub mod hashpage;
pub mod hashsort;
pub mod hashvalidate;
pub mod hashutil;
pub mod hashsearch;
