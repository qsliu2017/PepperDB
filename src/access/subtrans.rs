//! Translated from PostgreSQL src/include/access/subtrans.h
//!
//! Definitions live in transam/subtrans.c; re-exported here (rules s2). The
//! subtrans ops became inherent methods on the subtrans `SlruCtl`
//! (`shared.subtrans().sub_trans_get_parent(...)` etc., refactor14); they cannot
//! be `pub use`d, and all callers use the methods, so only the remaining free fn
//! is re-exported.

pub use crate::backend::access::transam::subtrans::subtrans_shmem_size;
