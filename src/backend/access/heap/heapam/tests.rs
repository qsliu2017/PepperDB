//! Integration tests for the M2 heap insert + forward seqscan + MVCC path.
//!
//! Each test stands up a real foundation `SharedState` over a tempdir, the full
//! per-task scope stack (Session / ResourceOwner / xact / snapmgr / combocid /
//! WAL insertion), starts a transaction, creates a heap relation's main fork,
//! inserts tuples through `heap_insert` (which emits WAL), then recovers them via
//! `heap_beginscan` + `heap_getnext` + `heap_deform_tuple` and checks the exact
//! Datums. The slot-based `table_scan_getnextslot` depends on the executor
//! slot-store routines (staged), so the exact-Datum recovery is verified at the
//! heap level (the complete M2 path).

use std::sync::Arc;

use super::*;
use crate::access::tableam::ScanOptions;
use crate::backend::access::common::heaptuple::{heap_deform_tuple, heap_form_tuple};
use crate::backend::access::transam::xact::{
    CommandCounterIncrement, GetCurrentCommandId, StartTransactionCommand,
};
use crate::backend::utils::time::snapmgr::GetTransactionSnapshot;
use crate::catalog::pg_class::{RELKIND_RELATION, RELPERSISTENCE_PERMANENT};
use crate::common::relpath::ForkNumber;
use crate::postgres::{DatumGetInt32, Int32GetDatum};
use crate::postgres_ext::Oid;
use crate::storage::relfilelocator::RelFileLocator;
use crate::utils::rel::{LockInfoData, LockRelId, RelationData};

static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

fn new_shared() -> Arc<SharedState> {
    use crate::shared_state::SharedStateConfig;
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("pepperdb-heapam-{}-{}", std::process::id(), n));
    let _ = std::fs::create_dir_all(dir.join(crate::access::xlog_internal::XLOGDIR));
    let _ = std::fs::create_dir_all(dir.join("base").join("90000"));
    SharedState::new(SharedStateConfig {
        data_dir: Some(dir.to_string_lossy().into_owned()),
        nbuffers: 64,
        ..Default::default()
    })
}

/// Wrap a test body in the per-task scope set heap_insert/scan rely on.
async fn in_all_scopes<F, Fut, T>(shared: Arc<SharedState>, f: F) -> T
where
    F: FnOnce(Arc<SharedState>) -> Fut,
    Fut: std::future::Future<Output = T>,
{
    use crate::backend::access::transam::xloginsert::with_insertion;
    use crate::backend::utils::time::{combocid::combocid_scope, snapmgr::snapmgr_scope};
    let sess = Arc::new(crate::session::Session::new(crate::miscadmin::BackendType::BACKEND));
    let owner = crate::backend::utils::resowner::resowner::ResourceOwner::create(None, "Test");
    crate::session::scope(
        sess,
        crate::backend::utils::resowner::resowner::scope(
            owner,
            crate::backend::access::transam::xact::xact_scope(snapmgr_scope(combocid_scope(
                with_insertion(f(shared)),
            ))),
        ),
    )
    .await
}

fn rloc(rel: u32) -> RelFileLocator {
    RelFileLocator { spcOid: Oid(1663), dbOid: Oid(90000), relNumber: Oid(80000 + rel) }
}

/// Build a minimal heap `RelationData` (boxed, leaked) backed by `locator`. The
/// `tupdesc` describes the rowtype. Only the fields heap insert/scan read are
/// set; the catalog form is zeroed and patched with relkind/persistence/natts.
fn make_relation(
    locator: RelFileLocator,
    tupdesc: crate::access::tupdesc::TupleDesc,
) -> Arc<RelationData> {
    use crate::catalog::pg_class::FormData_pg_class;
    use std::sync::atomic::Ordering;

    // SAFETY: FormData_pg_class is repr(C) POD (Oid/int/bool/NameData/varlena
    // arrays); all-zero is a valid bit pattern. We patch the fields heap reads.
    let mut form: Box<FormData_pg_class> = Box::new(unsafe { core::mem::zeroed() });
    form.relkind = RELKIND_RELATION;
    form.relpersistence = RELPERSISTENCE_PERMANENT;
    form.relnatts = tupdesc.natts as i16;
    form.relam = Oid(2); // HEAP_TABLE_AM_OID (any nonzero handler -> Heap kind)
    let form_ptr = Some(form);

    // A fresh RelationData with the heap-relevant fields set; the rest are blank
    // (pointer fields null, Vec/Option fields empty/None). Shared via Arc.
    let mut rel = RelationData::blank();
    rel.rd_locator = locator;
    rel.rd_refcnt.store(1, Ordering::Relaxed);
    rel.rd_isvalid.store(true, Ordering::Relaxed);
    rel.rd_rel = form_ptr;
    rel.rd_att = Some(tupdesc);
    rel.rd_id = locator.relNumber;
    rel.rd_lockInfo = LockInfoData {
        lockRelId: LockRelId { relId: locator.relNumber, dbId: locator.dbOid },
    };
    rel.rd_amhandler = Oid(2);
    Arc::new(rel)
}

/// A 2-column (int4, int4) tuple descriptor. Uses `init_builtin_entry` (which
/// fills attlen/attbyval/attalign from a compiled-in table) so the test does not
/// depend on the syscache (step 14).
fn two_int4_desc() -> crate::access::tupdesc::TupleDesc {
    use crate::access::tupdesc::TupleDescData;
    const INT4OID: Oid = Oid(23);
    let mut d = TupleDescData::create_template(2);
    d.init_builtin_entry(1, "a", INT4OID, -1, 0);
    d.init_builtin_entry(2, "b", INT4OID, -1, 0);
    Arc::new(d)
}

/// Create the relation's main fork file so the buffer pool can extend it.
async fn create_main_fork(shared: &Arc<SharedState>, locator: RelFileLocator) {
    let mut smgr = crate::storage::smgr::SmgrRelation::open(
        locator,
        crate::storage::procnumber::INVALID_PROC_NUMBER,
    );
    smgr.create(shared, ForkNumber::MAIN_FORKNUM, false).await;
}

/// Insert one (a, b) int4 tuple into `relation` via heap_insert.
async fn insert_row(shared: &Arc<SharedState>, relation: &Arc<RelationData>, a: i32, b: i32) {
    let desc = relation.rd_att.clone().unwrap();
    let values = [Int32GetDatum(a), Int32GetDatum(b)];
    let isnull = [false, false];
    let mut tuple = heap_form_tuple(&desc, &values, &isnull);
    let cid = GetCurrentCommandId(true);
    heap_insert(shared, relation, &mut tuple, cid, 0).await;
}

/// Scan `relation` under the current transaction snapshot, returning each tuple's
/// (a, b) int4 values in scan order.
async fn scan_rows(shared: &Arc<SharedState>, relation: &Arc<RelationData>) -> Vec<(i32, i32)> {
    let mut snap = GetTransactionSnapshot(shared).expect("a transaction snapshot");
    // GetSnapshotData should set curcid = GetCurrentCommandId(false); the
    // foundation's build_snapshot leaves it 0 (TODO in procarray), so set it here
    // so own-xact command visibility (cmin < curcid) is exercised correctly.
    Arc::make_mut(&mut snap).curcid = GetCurrentCommandId(false);
    let desc = relation.rd_att.clone().unwrap();
    let mut scan = heap_beginscan(relation, &snap, 0, ScanOptions::ALLOW_PAGEMODE);

    let mut out = Vec::new();
    while let Some(tup) = heap_getnext(shared, &mut scan, ScanDirection::Forward).await {
        // SAFETY: heap_getnext returns a pointer into scan.ctup, valid until the
        // next call; deform reads the header + data.
        let htd = unsafe { &*tup };
        let (vals, nulls) = unsafe { heap_deform_tuple(htd, &desc) };
        assert!(!nulls[0] && !nulls[1]);
        out.push((DatumGetInt32(vals[0]), DatumGetInt32(vals[1])));
    }
    heap_endscan(shared, &mut scan);
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn insert_then_seqscan_one_tuple() {
    let shared = new_shared();
    Box::pin(in_all_scopes(shared.clone(), |shared| async move {
        StartTransactionCommand(&shared).await;
        let loc = rloc(1);
        create_main_fork(&shared, loc).await;
        let rel = make_relation(loc, two_int4_desc());

        let lsn_before = shared.xlog().get_flush_rec_ptr();
        insert_row(&shared, &rel, 42, 99).await;

        // WAL was emitted: the inserted LSN advanced past the pre-insert point.
        let inserted_lsn = shared.xlog().get_xlog_insert_rec_ptr();
        assert!(inserted_lsn.0 > lsn_before.0, "heap_insert must emit a WAL record");

        // Advance the command counter so a later command's snapshot sees the
        // just-inserted (own-xact) tuple.
        CommandCounterIncrement();
        let rows = scan_rows(&shared, &rel).await;
        assert_eq!(rows, vec![(42, 99)]);
    }))
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn insert_several_seqscan_in_order() {
    let shared = new_shared();
    Box::pin(in_all_scopes(shared.clone(), |shared| async move {
        StartTransactionCommand(&shared).await;
        let loc = rloc(2);
        create_main_fork(&shared, loc).await;
        let rel = make_relation(loc, two_int4_desc());

        let expected: Vec<(i32, i32)> = (0..25).map(|i| (i, i * 10)).collect();
        for &(a, b) in &expected {
            insert_row(&shared, &rel, a, b).await;
        }

        CommandCounterIncrement();
        let rows = scan_rows(&shared, &rel).await;
        assert_eq!(rows, expected);
    }))
    .await;
}

// MVCC command-visibility: a tuple inserted by the CURRENT command (cmin ==
// curcid) is NOT visible to a snapshot whose curcid predates that command; after
// CommandCounterIncrement it becomes visible. This exercises the
// current-transaction / cmin-vs-curcid arm of HeapTupleSatisfiesMVCC.
#[tokio::test(flavor = "multi_thread")]
async fn own_command_insert_invisible_until_cci() {
    let shared = new_shared();
    Box::pin(in_all_scopes(shared.clone(), |shared| async move {
        StartTransactionCommand(&shared).await;
        let loc = rloc(3);
        create_main_fork(&shared, loc).await;
        let rel = make_relation(loc, two_int4_desc());

        // Insert under the current command id, but do NOT advance the counter.
        insert_row(&shared, &rel, 7, 7).await;

        // A snapshot taken now has curcid == the inserting command's cmin, so the
        // tuple is "inserted after scan started" -> invisible.
        let rows_same_command = scan_rows(&shared, &rel).await;
        assert_eq!(rows_same_command, Vec::<(i32, i32)>::new());

        // After the command boundary, the own-xact tuple becomes visible.
        CommandCounterIncrement();
        let rows_next_command = scan_rows(&shared, &rel).await;
        assert_eq!(rows_next_command, vec![(7, 7)]);
    }))
    .await;
}
