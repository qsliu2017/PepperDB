//! Translated from PostgreSQL src/include/access/subtrans.h

use crate::c::TransactionId;

pub fn sub_trans_set_parent(_xid: TransactionId, _parent: TransactionId) {
    unimplemented!()
}

pub fn sub_trans_get_parent(_xid: TransactionId) -> TransactionId {
    unimplemented!()
}

pub fn sub_trans_get_topmost_transaction(_xid: TransactionId) -> TransactionId {
    unimplemented!()
}

pub fn subtrans_shmem_size() -> usize {
    unimplemented!()
}

pub fn subtrans_shmem_init() {
    unimplemented!()
}

pub fn boot_strap_subtrans() {
    unimplemented!()
}

pub fn startup_subtrans(_oldest_active_xid: TransactionId) {
    unimplemented!()
}

pub fn check_point_subtrans() {
    unimplemented!()
}

pub fn extend_subtrans(_newest_xact: TransactionId) {
    unimplemented!()
}

pub fn truncate_subtrans(_oldest_xact: TransactionId) {
    unimplemented!()
}
