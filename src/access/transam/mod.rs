//! Transaction manager: XID assignment, CLOG-based commit/abort tracking,
//! and MVCC snapshot support.
//! For the PoC, each SQL statement auto-commits (single-statement transactions).

pub mod clog;
pub mod pg_control;
pub mod xlog;
pub mod xlogrecord;
pub mod xlogrecovery;

use crate::access::transam::clog::{Clog, XidStatus};
use std::path::Path;

/// First normal transaction ID (matches PostgreSQL's FirstNormalTransactionId).
pub const FIRST_NORMAL_XID: u32 = 3;
/// Frozen transaction ID (used by VACUUM to mark old committed tuples).
pub const FROZEN_XID: u32 = 2;
/// Bootstrap transaction ID.
pub const BOOTSTRAP_XID: u32 = 1;

/// MVCC snapshot: all XIDs < xmin are visible (committed), all >= xmax invisible,
/// and XIDs in xip were in-progress at snapshot time.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub xmin: u32,
    pub xmax: u32,
    pub xip: Vec<u32>,
}

pub struct TxnManager {
    next_xid: u32,
    clog: Clog,
}

impl TxnManager {
    pub fn new(clog_dir: &Path, next_xid: u32) -> Self {
        let mut clog = Clog::new(clog_dir);
        // Mark bootstrap XID as committed
        if clog.get_status(BOOTSTRAP_XID) == XidStatus::InProgress {
            clog.set_status(BOOTSTRAP_XID, XidStatus::Committed);
        }
        Self { next_xid, clog }
    }

    pub fn assign_xid(&mut self) -> u32 {
        let xid = self.next_xid;
        self.next_xid += 1;
        xid
    }

    pub fn commit(&mut self, xid: u32) {
        self.clog.set_status(xid, XidStatus::Committed);
    }

    pub fn abort(&mut self, xid: u32) {
        self.clog.set_status(xid, XidStatus::Aborted);
    }

    pub fn is_committed(&mut self, xid: u32) -> bool {
        self.clog.get_status(xid) == XidStatus::Committed
    }

    pub fn next_xid(&self) -> u32 {
        self.next_xid
    }

    /// Access the underlying CLOG for visibility checks.
    pub fn clog(&mut self) -> &mut Clog {
        &mut self.clog
    }

    /// Take an MVCC snapshot. For the single-connection PoC, no XIDs are
    /// in-progress (everything auto-commits), so xmin=xmax=next_xid, xip=[].
    pub fn take_snapshot(&self) -> Snapshot {
        Snapshot {
            xmin: self.next_xid,
            xmax: self.next_xid,
            xip: Vec::new(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn xid_increments() {
        let dir = tempfile::tempdir().unwrap();
        let mut txn = TxnManager::new(&dir.path().join("pg_xact"), FIRST_NORMAL_XID);
        assert_eq!(txn.assign_xid(), 3);
        assert_eq!(txn.assign_xid(), 4);
        assert_eq!(txn.assign_xid(), 5);
    }

    #[test]
    fn commit_and_abort() {
        let dir = tempfile::tempdir().unwrap();
        let mut txn = TxnManager::new(&dir.path().join("pg_xact"), FIRST_NORMAL_XID);
        let xid1 = txn.assign_xid();
        let xid2 = txn.assign_xid();

        assert!(!txn.is_committed(xid1));
        txn.commit(xid1);
        assert!(txn.is_committed(xid1));

        txn.abort(xid2);
        assert!(!txn.is_committed(xid2));
    }

    #[test]
    fn bootstrap_xid_committed() {
        let dir = tempfile::tempdir().unwrap();
        let mut txn = TxnManager::new(&dir.path().join("pg_xact"), FIRST_NORMAL_XID);
        assert!(txn.is_committed(BOOTSTRAP_XID));
    }

    #[test]
    fn snapshot_visibility_committed_before() {
        use crate::access::heap;
        use crate::catalog::Column;
        use crate::storage::bufpage::PAGE_SIZE;
        use crate::types::{Datum, TypeId};

        let dir = tempfile::tempdir().unwrap();
        let mut txn = TxnManager::new(&dir.path().join("pg_xact"), FIRST_NORMAL_XID);

        let cols = vec![Column {
            name: "a".into(),
            type_id: TypeId::Int4,
            col_num: 0,
            typmod: -1,
        }];

        // Insert with xid=3, commit it
        let xid = txn.assign_xid();
        let tuple = heap::build_tuple_with_xid(&[Datum::Int4(42)], &cols, xid, false);
        let mut page = [0u8; PAGE_SIZE];
        crate::storage::bufpage::init_page(&mut page);
        heap::insert_tuple(&mut page, &tuple, 0).unwrap();
        txn.commit(xid);

        // Snapshot taken after commit -- tuple should be visible
        let snap = txn.take_snapshot();
        assert!(heap::tuple_visible(&page, 0, &snap, txn.clog()));
        let datums = heap::read_tuple_mvcc(&page, 0, &cols, &snap, txn.clog()).unwrap();
        assert_eq!(datums, vec![Datum::Int4(42)]);
    }

    #[test]
    fn snapshot_visibility_committed_after() {
        use crate::access::heap;
        use crate::catalog::Column;
        use crate::storage::bufpage::PAGE_SIZE;
        use crate::types::{Datum, TypeId};

        let dir = tempfile::tempdir().unwrap();
        let mut txn = TxnManager::new(&dir.path().join("pg_xact"), FIRST_NORMAL_XID);

        // Take snapshot before any transaction
        let snap = txn.take_snapshot(); // xmin=xmax=3

        let cols = vec![Column {
            name: "a".into(),
            type_id: TypeId::Int4,
            col_num: 0,
            typmod: -1,
        }];

        // Insert with xid=3, commit it after snapshot
        let xid = txn.assign_xid();
        let tuple = heap::build_tuple_with_xid(&[Datum::Int4(42)], &cols, xid, false);
        let mut page = [0u8; PAGE_SIZE];
        crate::storage::bufpage::init_page(&mut page);
        heap::insert_tuple(&mut page, &tuple, 0).unwrap();
        txn.commit(xid);

        // Tuple committed after snapshot -- should NOT be visible
        assert!(!heap::tuple_visible(&page, 0, &snap, txn.clog()));
    }
}
