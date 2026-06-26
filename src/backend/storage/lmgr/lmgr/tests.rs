//! Tests for the high-level lock manager wrappers (15d). Multi-thread tokio so
//! the conflict-wait path (one task holds, another blocks then is woken on
//! release) is real. Each test builds its own `SharedState` (tempdir data_dir,
//! no repo-root pollution).

use std::sync::Arc;
use std::time::Duration;

use super::*;
use crate::backend::storage::lmgr::lock::local_lock_scope;
use crate::c::TransactionId;
use crate::miscadmin::BackendType;
use crate::session::{Session, scope as session_scope};
use crate::shared_state::{SharedState, SharedStateConfig};
use crate::storage::lockdefs::LockMode;
use crate::storage::proc::my_proc_scope;

fn shared() -> Arc<SharedState> {
    SharedState::new(SharedStateConfig::default())
}

/// Wrap a backend body: session -> my_proc -> local-lock -> spec-token scope,
/// with InitProcess/ProcKill bracketing.
async fn backend<F, Fut, T>(db: u32, body: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let sess = Arc::new(Session::new(BackendType::BACKEND));
    sess.set_database_id(crate::postgres_ext::Oid(db));
    session_scope(
        sess,
        my_proc_scope(local_lock_scope(speculative_token_scope(async move {
            crate::backend::storage::lmgr::proc::InitProcess();
            let r = body().await;
            crate::backend::storage::lmgr::proc::ProcKill();
            r
        }))),
    )
    .await
}

// The object-lock SESSION wrappers exercise the grant + unlock machinery without
// touching AcceptInvalidationMessages (the non-session LockRelationOid /
// LockDatabaseObject family calls the sinval stub on the grant path, deferred to
// step 16) and without IsSharedRelation (a catalog.c stub). Same underlying
// SET_LOCKTAG_OBJECT + LockAcquire/LockRelease as the relation wrappers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lock_shared_object_grant_and_unlock() {
    let _s = shared();
    backend(0, || async {
        let (classid, objid) = (crate::postgres_ext::Oid(2000), crate::postgres_ext::Oid(3000));
        let tag = crate::storage::lock::LOCKTAG::set_object(0, 2000, 3000, 0);
        LockSharedObjectForSession(classid, objid, 0, LockMode::ExclusiveLock as i32).await;
        assert!(crate::backend::storage::lmgr::lock::LockHeldByMe(
            &tag,
            LockMode::ExclusiveLock as i32,
            false,
        ));
        UnlockSharedObjectForSession(classid, objid, 0, LockMode::ExclusiveLock as i32);
        assert!(!crate::backend::storage::lmgr::lock::LockHeldByMe(
            &tag,
            LockMode::ExclusiveLock as i32,
            false,
        ));
    })
    .await;
}

// ConditionalLockSharedObject exercises the Conditional* -> dontWait=true ->
// NotAvail -> false path (identical to ConditionalLockRelationOid's logic). On a
// conflict it returns false BEFORE AcceptInvalidationMessages, so neither task
// hits the sinval stub (the holder uses the session wrapper, also inval-free).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conditional_lock_shared_object_false_on_conflict() {
    let _s = shared();
    let classid = crate::postgres_ext::Oid(2001);
    let objid = crate::postgres_ext::Oid(3001);

    backend(0, || async move {
        // Hold Exclusive on the object (session wrapper -> no inval).
        LockSharedObjectForSession(classid, objid, 0, LockMode::ExclusiveLock as i32).await;

        // Another backend tries to conditionally take a conflicting lock.
        let got = tokio::spawn(async move {
            backend(0, || async move {
                ConditionalLockSharedObject(classid, objid, 0, LockMode::ExclusiveLock as i32).await
            })
            .await
        })
        .await
        .unwrap();
        assert!(!got, "conditional acquire must fail while Exclusive held");

        UnlockSharedObjectForSession(classid, objid, 0, LockMode::ExclusiveLock as i32);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn xact_lock_table_wait_blocks_until_holder_releases() {
    let s = shared();
    // Small xid (FirstNormal) so procarray reports it NOT in progress -> the wait
    // loop does one iteration: block on the ShareLock, then return.
    let xid = TransactionId(crate::c::FirstNormalTransactionId.0);

    let (acquired_tx, acquired_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();

    // Holder: take the xid's Exclusive lock (XactLockTableInsert) and hold it
    // until told to release (mimics xact end).
    let holder = tokio::spawn(async move {
        backend(0, || async move {
            XactLockTableInsert(xid).await;
            acquired_tx.send(()).unwrap();
            release_rx.await.unwrap();
            // Release via XactLockTableDelete (subxid-style explicit release).
            XactLockTableDelete(xid);
        })
        .await;
    });

    acquired_rx.await.unwrap();

    // Waiter: XactLockTableWait must block until the holder releases.
    let s2 = s.clone();
    let waiter = tokio::spawn(async move {
        Box::pin(backend(0, || async move {
            let mut ctid = crate::storage::itemptr::ItemPointerData {
                blkid: crate::storage::block::BlockIdData { hi: 0, lo: 0 },
                posid: 0,
            };
            ctid.set_invalid();
            XactLockTableWait(&s2, xid, std::ptr::null_mut(), &ctid, XLTW_Oper::XltwNone).await;
        }))
        .await;
    });

    // The waiter should still be blocked shortly after start.
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(!waiter.is_finished(), "waiter must block while xid lock is held");

    // Release; the waiter should then complete.
    release_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("waiter did not wake after release")
        .unwrap();
    holder.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conditional_xact_lock_table_wait_false_while_held() {
    let s = shared();
    let xid = TransactionId(crate::c::FirstNormalTransactionId.0);

    let (acquired_tx, acquired_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();

    let holder = tokio::spawn(async move {
        backend(0, || async move {
            XactLockTableInsert(xid).await;
            acquired_tx.send(()).unwrap();
            release_rx.await.unwrap();
            XactLockTableDelete(xid);
        })
        .await;
    });

    acquired_rx.await.unwrap();

    let s2 = s.clone();
    let got = tokio::spawn(async move {
        Box::pin(backend(0, || async move {
            ConditionalXactLockTableWait(&s2, xid, false).await
        }))
        .await
    })
    .await
    .unwrap();
    assert!(!got, "conditional wait must return false while xid lock held");

    release_tx.send(()).unwrap();
    holder.await.unwrap();
}

#[test]
fn describe_lock_tag_formats() {
    let mut buf = String::new();
    DescribeLockTag(&mut buf, &crate::storage::lock::LOCKTAG::set_relation(5, 42));
    assert_eq!(buf, "relation 42 of database 5");

    buf.clear();
    DescribeLockTag(&mut buf, &crate::storage::lock::LOCKTAG::set_transaction(99));
    assert_eq!(buf, "transaction 99");

    assert_eq!(GetLockNameFromTagType(0), "relation");
    assert_eq!(GetLockNameFromTagType(5), "transactionid");
    assert_eq!(GetLockNameFromTagType(250), "???");
}
