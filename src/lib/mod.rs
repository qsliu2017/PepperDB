//! Backend `lib` subsystem: general-purpose data structures shared across the
//! backend (postgres/src/backend/lib + postgres/src/include/lib).
//!
//! Each module merges the C header (`include/lib/<name>.h`) with its
//! implementation (`backend/lib/<name>.c`, or `common/<name>.c` for shared code).

pub mod binaryheap;
pub mod bipartite_match;
pub mod bloomfilter;
pub mod hyperloglog;
pub mod ilist;
pub mod integerset;
pub mod knapsack;
pub mod pairingheap;
pub mod qunique;
pub mod radixtree;
pub mod rbtree;
pub mod simplehash;
pub mod sort_template;
pub mod stringinfo;
pub mod dshash;
