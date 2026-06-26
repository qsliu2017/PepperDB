//! Translated from PostgreSQL src/backend/access/transam/subtrans.c
//!
//! The pg_subtrans manager: stores the immediate parent xid of each
//! subtransaction over the SLRU async leaf. No WAL (not preserved across
//! crashes). The subtrans `SlruCtl` lives on `SharedState`; reached via
//! `shared.subtrans()`.

use std::sync::Arc;

use crate::access::transam::FIRST_NORMAL_TRANSACTION_ID;
use crate::backend::access::transam::slru::{autotune_buffers, SlruCtl, SLRU_BANK_SIZE};
use crate::c::TransactionId;
use crate::pg_config::BLCKSZ;
use crate::shared_state::SharedState;
use crate::storage::sync::SyncRequestHandler;

// subtrans.c: four bytes (one TransactionId) per xact.
const SUBTRANS_XACTS_PER_PAGE: u32 = BLCKSZ / 4;

#[inline]
fn xid_to_page(xid: TransactionId) -> i64 {
    xid.0 as i64 / SUBTRANS_XACTS_PER_PAGE as i64
}
#[inline]
fn xid_to_entry(xid: TransactionId) -> usize {
    (xid.0 % SUBTRANS_XACTS_PER_PAGE) as usize
}

fn subtrans_shmem_buffers(nbuffers: usize) -> usize {
    autotune_buffers(nbuffers, 512, 1024)
}

/// subtrans.c SUBTRANSShmemSize (estimate under the Arc model).
pub fn subtrans_shmem_size(nbuffers: usize) -> usize {
    subtrans_shmem_buffers(nbuffers) * BLCKSZ as usize
}

/// subtrans.c SUBTRANSShmemInit: build the subtrans SlruCtl over `pg_subtrans`.
pub fn subtrans_shmem_init_handles(
    nbuffers: usize,
    fd: Arc<crate::backend::storage::file::fd::FdManager>,
    xlog: Arc<crate::backend::access::transam::xlog::XLogCtl>,
    sync_requests: Arc<crate::storage::sync::SyncRequests>,
    data_dir: Option<String>,
) -> Arc<SlruCtl> {
    let nslots = subtrans_shmem_buffers(nbuffers);
    debug_assert!(nslots % SLRU_BANK_SIZE == 0);
    SlruCtl::new(
        nslots,
        0,
        "pg_subtrans",
        SyncRequestHandler::None,
        false,
        subtrans_page_precedes,
        fd,
        xlog,
        sync_requests,
        data_dir,
    )
}

/// subtrans.c SubTransPagePrecedes.
fn subtrans_page_precedes(page1: i64, page2: i64) -> bool {
    let base = FIRST_NORMAL_TRANSACTION_ID.0.wrapping_add(1);
    let xid1 = TransactionId((page1 as u32).wrapping_mul(SUBTRANS_XACTS_PER_PAGE).wrapping_add(base));
    let xid2 = TransactionId((page2 as u32).wrapping_mul(SUBTRANS_XACTS_PER_PAGE).wrapping_add(base));
    let xid2_hi = TransactionId(xid2.0.wrapping_add(SUBTRANS_XACTS_PER_PAGE - 1));
    crate::access::transam::TransactionIdPrecedes(xid1, xid2)
        && crate::access::transam::TransactionIdPrecedes(xid1, xid2_hi)
}

fn parent_to_le(parent: TransactionId) -> [u8; 4] {
    parent.0.to_le_bytes()
}
fn parent_from_le(buf: &[u8], entry: usize) -> TransactionId {
    let off = entry * 4;
    TransactionId(u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]]))
}

/// subtrans.c SubTransSetParent: record `xid`'s parent.
pub async fn sub_trans_set_parent(
    shared: &Arc<SharedState>,
    xid: TransactionId,
    parent: TransactionId,
) {
    debug_assert!(crate::access::transam::transaction_id_is_valid(parent));
    debug_assert!(crate::access::transam::TransactionIdFollows(xid, parent));
    let pageno = xid_to_page(xid);
    let entry = xid_to_entry(xid);
    let st = shared.subtrans();
    let slot = st.read_page(pageno, true, xid).await;
    st.with_page_mut(pageno, slot, |buf| {
        let cur = parent_from_le(buf, entry);
        if cur.0 != parent.0 {
            debug_assert!(cur.0 == 0, "subtrans parent must not change from one valid xid to another");
            let le = parent_to_le(parent);
            buf[entry * 4..entry * 4 + 4].copy_from_slice(&le);
        }
    });
}

/// subtrans.c SubTransGetParent.
pub async fn sub_trans_get_parent(shared: &Arc<SharedState>, xid: TransactionId) -> TransactionId {
    // Bootstrap and frozen XIDs have no parent.
    if !crate::access::transam::transaction_id_is_normal(xid) {
        return TransactionId(0);
    }
    let pageno = xid_to_page(xid);
    let entry = xid_to_entry(xid);
    let st = shared.subtrans();
    let slot = st.read_page_readonly(pageno, xid).await;
    st.with_page(pageno, slot, |buf| parent_from_le(buf, entry))
}

/// subtrans.c SubTransGetTopmostTransaction: walk the parent chain to the top
/// (bounded by `transaction_xmin`, the oldest visible xid).
pub async fn sub_trans_get_topmost_transaction(
    shared: &Arc<SharedState>,
    xid: TransactionId,
    transaction_xmin: TransactionId,
) -> TransactionId {
    let mut parent = xid;
    let mut previous = xid;
    while crate::access::transam::transaction_id_is_valid(parent) {
        previous = parent;
        if crate::access::transam::TransactionIdPrecedes(parent, transaction_xmin) {
            break;
        }
        parent = sub_trans_get_parent(shared, parent).await;
        // Parent is allocated before child, so must precede it; else corruption.
        if !crate::access::transam::TransactionIdPrecedes(parent, previous) {
            panic!(
                "pg_subtrans contains invalid entry: xid {} points to parent xid {}",
                previous.0, parent.0
            );
        }
    }
    previous
}

/// subtrans.c BootStrapSUBTRANS: create + zero + flush the first page.
pub async fn boot_strap_subtrans(shared: &Arc<SharedState>) {
    let st = shared.subtrans();
    let slot = st.zero_page(0).await;
    st.write_page(slot).await;
}

/// subtrans.c StartupSUBTRANS: zero the currently-active page span.
pub async fn startup_subtrans(shared: &Arc<SharedState>, oldest_active_xid: TransactionId) {
    let next = crate::access::transam::ReadNextFullTransactionId(shared);
    let end_page = xid_to_page(crate::access::transam::xid_from_full_transaction_id(next));
    let mut page = xid_to_page(oldest_active_xid);
    let st = shared.subtrans();
    let max_page = xid_to_page(crate::access::transam::MAX_TRANSACTION_ID);
    loop {
        let _ = st.zero_page(page).await;
        if page == end_page {
            break;
        }
        page += 1;
        if page > max_page {
            page = 0;
        }
    }
}

/// subtrans.c CheckPointSUBTRANS.
pub async fn check_point_subtrans(shared: &Arc<SharedState>) {
    shared.subtrans().write_all().await;
}

/// subtrans.c ExtendSUBTRANS: zero a fresh page at a page boundary.
pub async fn extend_subtrans(shared: &Arc<SharedState>, newest_xact: TransactionId) {
    if (newest_xact.0 % SUBTRANS_XACTS_PER_PAGE) != 0
        && newest_xact.0 != FIRST_NORMAL_TRANSACTION_ID.0
    {
        return;
    }
    let pageno = xid_to_page(newest_xact);
    let _ = shared.subtrans().zero_page(pageno).await;
}

/// subtrans.c TruncateSUBTRANS.
pub async fn truncate_subtrans(shared: &Arc<SharedState>, oldest_xact: TransactionId) {
    let mut ox = oldest_xact;
    crate::access::transam::transaction_id_retreat(&mut ox);
    let cutoff_page = xid_to_page(ox);
    shared.subtrans().truncate(cutoff_page).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared_state::{SharedState, SharedStateConfig};

    fn temp_shared(tag: &str) -> Arc<SharedState> {
        let dir = std::env::temp_dir().join(format!(
            "pepperdb_subtrans_{tag}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        // data_dir must be set in the config so the subtrans SLRU resolves
        // pg_subtrans under the tempdir at construction (not the relative default).
        SharedState::new(SharedStateConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            ..SharedStateConfig::default()
        })
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn parent_chain_walk() {
        let shared = temp_shared("chain");
        boot_strap_subtrans(&shared).await;
        // Build a chain: 100 (top) <- 200 <- 300.
        let top = TransactionId(100);
        let mid = TransactionId(200);
        let leaf = TransactionId(300);
        sub_trans_set_parent(&shared, mid, top).await;
        sub_trans_set_parent(&shared, leaf, mid).await;

        assert_eq!(sub_trans_get_parent(&shared, leaf).await.0, 200);
        assert_eq!(sub_trans_get_parent(&shared, mid).await.0, 100);
        // top has no recorded parent -> 0 (Invalid).
        assert_eq!(sub_trans_get_parent(&shared, top).await.0, 0);

        // Topmost from leaf, with xmin low enough to walk to the top.
        let xmin = TransactionId(50);
        assert_eq!(sub_trans_get_topmost_transaction(&shared, leaf, xmin).await.0, 100);
    }
}
