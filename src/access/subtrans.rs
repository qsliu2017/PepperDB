//! Translated from PostgreSQL src/include/access/subtrans.h

// TODO(struct-forward): TransactionId lives in c.h; repoint to crate::c in Phase 2.
#[deprecated(note = "TODO(struct-forward): repoint to crate::c::TransactionId in Phase 2")]
pub type TransactionId = u32;

#[allow(deprecated)]
pub fn sub_trans_set_parent(_xid: TransactionId, _parent: TransactionId) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn sub_trans_get_parent(_xid: TransactionId) -> TransactionId {
    unimplemented!()
}

#[allow(deprecated)]
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

#[allow(deprecated)]
pub fn startup_subtrans(_oldest_active_xid: TransactionId) {
    unimplemented!()
}

pub fn check_point_subtrans() {
    unimplemented!()
}

#[allow(deprecated)]
pub fn extend_subtrans(_newest_xact: TransactionId) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn truncate_subtrans(_oldest_xact: TransactionId) {
    unimplemented!()
}
