//! Translated from PostgreSQL src/include/access/subtrans.h
//!
//! Definitions live in transam/subtrans.c; re-exported here (rules s2). The
//! subtrans ops became async + threaded through `&Arc<SharedState>` (async
//! coloring from the SLRU leaf, design s4); `*_get_parent`/`*_get_topmost`
//! additionally take `transaction_xmin` (the ex process-global, snapmgr-owned).

pub use crate::backend::access::transam::subtrans::{
    boot_strap_subtrans, check_point_subtrans, extend_subtrans, startup_subtrans,
    sub_trans_get_parent, sub_trans_get_topmost_transaction, sub_trans_set_parent,
    subtrans_shmem_size, truncate_subtrans,
};
