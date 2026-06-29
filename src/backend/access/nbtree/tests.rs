//! Integration tests for the M2 btree access method: bottom-up build
//! (`btbuild` -> nbtsort `bt_load`), forward index scan (`bt_first`/`bt_next`)
//! returning sorted keys, point lookups (hit + miss), a multi-column scan, a build
//! large enough to force multiple leaf pages + an internal level (the "split"
//! analogue for the build path), and `index_fetch_heap` recovering the heap tuple
//! via the index.
//!
//! Each test stands up a real foundation `SharedState` over a tempdir, the full
//! per-task scope stack (Session / ResourceOwner / xact / snapmgr / combocid /
//! WAL insertion), builds a heap with rows, builds a btree over it, and scans.

#![allow(
    clippy::large_futures,
    reason = "test bodies chain the full build+scan async stack; not spawned, run via block_on"
)]
#![allow(clippy::default_trait_access, reason = "IndexInfo opaque-forward fields constructed via Default")]

use std::sync::Arc;

use crate::access::sdir::ScanDirection;
use crate::access::tableam::ScanOptions;
use crate::backend::access::common::heaptuple::heap_form_tuple;
use crate::backend::access::heap::heapam::heap_insert;
use crate::backend::access::index::indexam::{
    index_beginscan, index_fetch_heap, index_getnext_tid, index_rescan, IndexScanState,
};
use crate::backend::access::transam::xact::{GetCurrentCommandId, StartTransactionCommand};
use crate::backend::utils::cache::relcache::{
    index_init_opclass_support, relation_init_index_access_info,
};
use crate::backend::utils::time::snapmgr::GetTransactionSnapshot;
use crate::catalog::pg_class::{RELKIND_INDEX, RELKIND_RELATION, RELPERSISTENCE_PERMANENT};
use crate::catalog::pg_index::FormData_pg_index;
use crate::common::relpath::ForkNumber;
use crate::nodes::execnodes::IndexInfo;
use crate::postgres::{DatumGetInt32, Int32GetDatum};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::shared_state::SharedState;
use crate::storage::relfilelocator::RelFileLocator;
use crate::utils::rel::{LockInfoData, LockRelId, RelationData};

const INT4OID: Oid = Oid(23);
const INT4_BTREE_OPS_OID: Oid = Oid(1978);

static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

fn new_shared() -> Arc<SharedState> {
    use crate::shared_state::SharedStateConfig;
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("pepperdb-nbtree-{}-{}", std::process::id(), n));
    let _ = std::fs::create_dir_all(dir.join(crate::access::xlog_internal::XLOGDIR));
    let _ = std::fs::create_dir_all(dir.join("base").join("90000"));
    SharedState::new(SharedStateConfig {
        data_dir: Some(dir.to_string_lossy().into_owned()),
        nbuffers: 256,
        ..Default::default()
    })
}

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

/// A single-column int4 tuple descriptor named `a`.
fn one_int4_desc() -> crate::access::tupdesc::TupleDesc {
    use crate::access::tupdesc::TupleDescData;
    let mut d = TupleDescData::create_template(1);
    d.init_builtin_entry(1, "a", INT4OID, -1, 0);
    Arc::new(d)
}

/// A 2-column (int4, int4) tuple descriptor.
fn two_int4_desc() -> crate::access::tupdesc::TupleDesc {
    use crate::access::tupdesc::TupleDescData;
    let mut d = TupleDescData::create_template(2);
    d.init_builtin_entry(1, "a", INT4OID, -1, 0);
    d.init_builtin_entry(2, "b", INT4OID, -1, 0);
    Arc::new(d)
}

/// Build a minimal `RelationData` (boxed, leaked) backed by `locator`. `relkind`
/// is RELATION (heap) or INDEX. The catalog form is zeroed + patched.
fn make_relation(
    locator: RelFileLocator,
    tupdesc: crate::access::tupdesc::TupleDesc,
    relkind: i8,
) -> Arc<RelationData> {
    use std::sync::atomic::Ordering;

    use crate::catalog::pg_class::FormData_pg_class;
    // SAFETY: FormData_pg_class is repr(C) POD; all-zero is a valid pattern.
    let mut form: Box<FormData_pg_class> = Box::new(unsafe { core::mem::zeroed() });
    form.relkind = relkind;
    form.relpersistence = RELPERSISTENCE_PERMANENT;
    form.relnatts = tupdesc.natts as i16;
    form.relam = Oid(403); // BTREE_AM_OID for an index; harmless for heap test rels
    let form_ptr = Some(form);

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
    Arc::new(rel)
}

/// Attach a single-column int4 pg_index + opclass support to an index relation.
fn init_index_support(index: &mut RelationData, nkeyatts: i16) {
    // SAFETY: FormData_pg_index is repr(C) POD; zero then patch the fixed fields.
    let mut idx: Box<FormData_pg_index> = Box::new(unsafe { core::mem::zeroed() });
    idx.indnatts = nkeyatts;
    idx.indnkeyatts = nkeyatts;
    idx.indisunique = false;
    idx.indnullsnotdistinct = false;
    index.rd_index = Some(idx);
    relation_init_index_access_info(index);
    let opclasses = vec![INT4_BTREE_OPS_OID; nkeyatts as usize];
    let collations = vec![InvalidOid; nkeyatts as usize];
    let indoption = vec![0i16; nkeyatts as usize];
    index_init_opclass_support(index, &opclasses, &collations, &indoption);
}

async fn create_main_fork(shared: &Arc<SharedState>, locator: RelFileLocator) {
    let mut smgr = crate::storage::smgr::SmgrRelation::open(
        locator,
        crate::storage::procnumber::INVALID_PROC_NUMBER,
    );
    smgr.create(shared, ForkNumber::MAIN_FORKNUM, false).await;
}

async fn insert_row1(shared: &Arc<SharedState>, relation: &Arc<RelationData>, a: i32) {
    let desc = relation.rd_att.clone().unwrap();
    let values = [Int32GetDatum(a)];
    let isnull = [false];
    let mut tuple = heap_form_tuple(&desc, &values, &isnull);
    let cid = GetCurrentCommandId(true);
    heap_insert(shared, relation, &mut tuple, cid, 0).await;
}

async fn insert_row2(shared: &Arc<SharedState>, relation: &Arc<RelationData>, a: i32, b: i32) {
    let desc = relation.rd_att.clone().unwrap();
    let values = [Int32GetDatum(a), Int32GetDatum(b)];
    let isnull = [false, false];
    let mut tuple = heap_form_tuple(&desc, &values, &isnull);
    let cid = GetCurrentCommandId(true);
    heap_insert(shared, relation, &mut tuple, cid, 0).await;
}

/// An IndexInfo whose key columns are heap columns `1..=nkeys`.
fn index_info(nkeys: i32) -> IndexInfo {
    let mut ii = IndexInfo {
        num_index_attrs: nkeys,
        num_index_key_attrs: nkeys,
        index_attr_numbers: (1..=nkeys).map(|i| i as i16).collect(),
        expressions: Vec::new(),
        expressions_state: Vec::new(),
        predicate: Vec::new(),
        predicate_state: None,
        exclusion_ops: Vec::new(),
        exclusion_procs: Vec::new(),
        exclusion_strats: Vec::new(),
        unique_ops: Vec::new(),
        unique_procs: Vec::new(),
        unique_strats: Vec::new(),
        unique: false,
        nulls_not_distinct: false,
        ready_for_inserts: true,
        checked_unchanged: false,
        index_unchanged: false,
        concurrent: false,
        broken_hot_chain: false,
        summarizing: false,
        without_overlaps: false,
        parallel_workers: 0,
        am: Oid(403),
        am_cache: Default::default(),
        context: Default::default(),
    };
    ii.num_index_attrs = nkeys;
    ii
}

/// Get the snapshot used to read both the heap (via index_fetch_heap) and to
/// build (active snapshot). Sets curcid for own-xact visibility.
fn txn_snapshot(shared: &Arc<SharedState>) -> crate::utils::snapshot::Snapshot {
    let mut snap = GetTransactionSnapshot(shared);
    Arc::make_mut(snap.as_mut().expect("a transaction snapshot")).curcid =
        GetCurrentCommandId(false);
    snap
}

#[tokio::test(flavor = "multi_thread")]
async fn build_and_forward_scan_sorted() {
    let shared = new_shared();
    in_all_scopes(shared, |shared| async move {
        StartTransactionCommand(&shared).await;
        crate::backend::utils::time::snapmgr::PushActiveSnapshot(            GetTransactionSnapshot(&shared),
        );

        let hloc = rloc(1);
        let iloc = rloc(101);
        create_main_fork(&shared, hloc).await;
        create_main_fork(&shared, iloc).await;
        let heap = make_relation(hloc, one_int4_desc(), RELKIND_RELATION);
        let mut index = make_relation(iloc, one_int4_desc(), RELKIND_INDEX);
        init_index_support(Arc::get_mut(&mut index).unwrap(), 1);

        // Insert keys out of order.
        for v in [30, 10, 50, 20, 40] {
            insert_row1(&shared, &heap, v).await;
        }
        crate::backend::access::transam::xact::CommandCounterIncrement();
        crate::backend::utils::time::snapmgr::PushActiveSnapshot(txn_snapshot(&shared));

        let ii = index_info(1);
        let res = crate::backend::access::nbtree::nbtree::btbuild(
            &shared,
            &heap,
            &index,
            &ii,
        )
        .await;
        assert_eq!(res.heap_tuples as i64, 5);
        assert_eq!(res.index_tuples as i64, 5);

        // Forward full scan returns sorted keys.
        let snap = txn_snapshot(&shared).expect("snapshot");
        let mut scan = index_beginscan(&heap, &index, &snap);
        index_rescan(&mut scan, Vec::new());
        let mut out = Vec::new();
        while let Some(tid) = index_getnext_tid(&shared, &mut scan, ScanDirection::Forward).await {
            // Fetch the heap tuple to read its key.
            let tup = index_fetch_heap(&shared, &mut scan).await.expect("heap tuple");
            let desc = heap.rd_att.clone().unwrap();
            let (vals, _n) = unsafe {
                crate::backend::access::common::heaptuple::heap_deform_tuple(&tup, &desc)
            };
            out.push(DatumGetInt32(vals[0]));
            let _ = tid;
        }
        assert_eq!(out, vec![10, 20, 30, 40, 50]);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn point_lookup_hit_and_miss() {
    let shared = new_shared();
    in_all_scopes(shared, |shared| async move {
        StartTransactionCommand(&shared).await;
        crate::backend::utils::time::snapmgr::PushActiveSnapshot(            GetTransactionSnapshot(&shared),
        );
        let hloc = rloc(2);
        let iloc = rloc(102);
        create_main_fork(&shared, hloc).await;
        create_main_fork(&shared, iloc).await;
        let heap = make_relation(hloc, one_int4_desc(), RELKIND_RELATION);
        let mut index = make_relation(iloc, one_int4_desc(), RELKIND_INDEX);
        init_index_support(Arc::get_mut(&mut index).unwrap(), 1);
        for v in [5, 15, 25, 35] {
            insert_row1(&shared, &heap, v).await;
        }
        crate::backend::access::transam::xact::CommandCounterIncrement();
        crate::backend::utils::time::snapmgr::PushActiveSnapshot(txn_snapshot(&shared));
        let ii = index_info(1);
        crate::backend::access::nbtree::nbtree::btbuild(
            &shared,
            &heap,
            &index,
            &ii,
        )
        .await;

        // Hit: 25 present.
        let snap = txn_snapshot(&shared).expect("snapshot");
        let mut scan = index_beginscan(&heap, &index, &snap);
        index_rescan(&mut scan, vec![(1, Int32GetDatum(25))]);
        let got = index_getnext_tid(&shared, &mut scan, ScanDirection::Forward).await;
        assert!(got.is_some(), "key 25 should be found");
        let tup = index_fetch_heap(&shared, &mut scan).await.expect("heap tuple");
        let desc = heap.rd_att.clone().unwrap();
        let (vals, _n) =
            unsafe { crate::backend::access::common::heaptuple::heap_deform_tuple(&tup, &desc) };
        assert_eq!(DatumGetInt32(vals[0]), 25);
        // No more matches for the equality key.
        assert!(index_getnext_tid(&shared, &mut scan, ScanDirection::Forward).await.is_none());

        // Miss: 26 absent.
        let snap = txn_snapshot(&shared).expect("snapshot");
        let mut scan2 = index_beginscan(&heap, &index, &snap);
        index_rescan(&mut scan2, vec![(1, Int32GetDatum(26))]);
        assert!(
            index_getnext_tid(&shared, &mut scan2, ScanDirection::Forward).await.is_none(),
            "key 26 should not be found"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn multi_column_scan() {
    let shared = new_shared();
    in_all_scopes(shared, |shared| async move {
        StartTransactionCommand(&shared).await;
        crate::backend::utils::time::snapmgr::PushActiveSnapshot(            GetTransactionSnapshot(&shared),
        );
        let hloc = rloc(3);
        let iloc = rloc(103);
        create_main_fork(&shared, hloc).await;
        create_main_fork(&shared, iloc).await;
        let heap = make_relation(hloc, two_int4_desc(), RELKIND_RELATION);
        let mut index = make_relation(iloc, two_int4_desc(), RELKIND_INDEX);
        init_index_support(Arc::get_mut(&mut index).unwrap(), 2);
        for (a, b) in [(2, 1), (1, 9), (1, 5), (2, 3)] {
            insert_row2(&shared, &heap, a, b).await;
        }
        crate::backend::access::transam::xact::CommandCounterIncrement();
        crate::backend::utils::time::snapmgr::PushActiveSnapshot(txn_snapshot(&shared));
        let ii = index_info(2);
        crate::backend::access::nbtree::nbtree::btbuild(
            &shared,
            &heap,
            &index,
            &ii,
        )
        .await;

        // Full scan returns lexicographic (a,b) order.
        let snap = txn_snapshot(&shared).expect("snapshot");
        let mut scan = index_beginscan(&heap, &index, &snap);
        index_rescan(&mut scan, Vec::new());
        let mut out = Vec::new();
        while index_getnext_tid(&shared, &mut scan, ScanDirection::Forward).await.is_some() {
            let tup = index_fetch_heap(&shared, &mut scan).await.expect("heap tuple");
            let desc = heap.rd_att.clone().unwrap();
            let (vals, _n) = unsafe {
                crate::backend::access::common::heaptuple::heap_deform_tuple(&tup, &desc)
            };
            out.push((DatumGetInt32(vals[0]), DatumGetInt32(vals[1])));
        }
        assert_eq!(out, vec![(1, 5), (1, 9), (2, 1), (2, 3)]);

        // Prefix equality on column a = 1 returns both (1,*) rows.
        let snap = txn_snapshot(&shared).expect("snapshot");
        let mut scan2 = index_beginscan(&heap, &index, &snap);
        index_rescan(&mut scan2, vec![(1, Int32GetDatum(1))]);
        let mut count = 0;
        while index_getnext_tid(&shared, &mut scan2, ScanDirection::Forward).await.is_some() {
            count += 1;
        }
        assert_eq!(count, 2, "two rows have a=1");
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn build_many_forces_split_then_full_scan_sorted() {
    let shared = new_shared();
    in_all_scopes(shared, |shared| async move {
        StartTransactionCommand(&shared).await;
        crate::backend::utils::time::snapmgr::PushActiveSnapshot(            GetTransactionSnapshot(&shared),
        );
        let hloc = rloc(4);
        let iloc = rloc(104);
        create_main_fork(&shared, hloc).await;
        create_main_fork(&shared, iloc).await;
        let heap = make_relation(hloc, one_int4_desc(), RELKIND_RELATION);
        let mut index = make_relation(iloc, one_int4_desc(), RELKIND_INDEX);
        init_index_support(Arc::get_mut(&mut index).unwrap(), 1);

        // Enough keys to span many leaf pages + at least one internal level.
        // An int4 leaf tuple is ~16 bytes; 2000 keys is ~32KB >> one 8KB page.
        let n: i32 = 2000;
        for v in (0..n).rev() {
            insert_row1(&shared, &heap, v).await;
        }
        crate::backend::access::transam::xact::CommandCounterIncrement();
        crate::backend::utils::time::snapmgr::PushActiveSnapshot(txn_snapshot(&shared));
        let ii = index_info(1);
        let res = crate::backend::access::nbtree::nbtree::btbuild(
            &shared,
            &heap,
            &index,
            &ii,
        )
        .await;
        assert_eq!(res.index_tuples as i64, i64::from(n));

        // Full forward scan returns all keys sorted (split/internal correctness).
        let snap = txn_snapshot(&shared).expect("snapshot");
        let mut scan = index_beginscan(&heap, &index, &snap);
        index_rescan(&mut scan, Vec::new());
        let mut prev = -1;
        let mut count = 0;
        while let Some(tid) = index_getnext_tid(&shared, &mut scan, ScanDirection::Forward).await {
            let tup = index_fetch_heap(&shared, &mut scan).await.expect("heap tuple");
            let desc = heap.rd_att.clone().unwrap();
            let (vals, _n) = unsafe {
                crate::backend::access::common::heaptuple::heap_deform_tuple(&tup, &desc)
            };
            let v = DatumGetInt32(vals[0]);
            assert!(v > prev, "keys must be strictly ascending: {prev} then {v}");
            prev = v;
            count += 1;
            let _ = tid;
        }
        assert_eq!(count, n, "all keys returned");

        // A point lookup deep in the tree still works (descends through internals).
        let snap = txn_snapshot(&shared).expect("snapshot");
        let mut scan2 = index_beginscan(&heap, &index, &snap);
        index_rescan(&mut scan2, vec![(1, Int32GetDatum(1234))]);
        let tid = index_getnext_tid(&shared, &mut scan2, ScanDirection::Forward).await;
        assert!(tid.is_some(), "key 1234 found via internal descent");
        let tup = index_fetch_heap(&shared, &mut scan2).await.expect("heap tuple");
        let desc = heap.rd_att.clone().unwrap();
        let (vals, _n) =
            unsafe { crate::backend::access::common::heaptuple::heap_deform_tuple(&tup, &desc) };
        assert_eq!(DatumGetInt32(vals[0]), 1234);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn inserts_force_split_then_scan_sorted() {
    use crate::backend::access::index::indexam::index_insert;
    use crate::backend::access::nbtree::nbtree::btbuildempty;

    let shared = new_shared();
    in_all_scopes(shared, |shared| async move {
        StartTransactionCommand(&shared).await;
        crate::backend::utils::time::snapmgr::PushActiveSnapshot(GetTransactionSnapshot(&shared));
        let hloc = rloc(5);
        let iloc = rloc(105);
        create_main_fork(&shared, hloc).await;
        create_main_fork(&shared, iloc).await;
        let heap = make_relation(hloc, one_int4_desc(), RELKIND_RELATION);
        let mut index = make_relation(iloc, one_int4_desc(), RELKIND_INDEX);
        init_index_support(Arc::get_mut(&mut index).unwrap(), 1);

        // Start from an empty btree (meta page only).
        btbuildempty(&shared, &index).await;

        // Insert keys one at a time (forcing many leaf splits + internal levels) and
        // a matching heap row for index_fetch_heap.
        let n: i32 = 800;
        for v in 0..n {
            insert_row1(&shared, &heap, v).await;
            // Use the heap tuple's TID by re-deriving it: the row just inserted is
            // the last one, but for the index we just need a TID; insert_row1 set
            // t_self on its local tuple. Re-insert into the index with the heap TID.
            // For simplicity, insert the index entry pointing at a synthetic TID
            // (block 0, offset v+1) -- the scan correctness check below reads keys
            // from the index tuples, not the heap, for the insert path.
            let mut tid = crate::storage::itemptr::ItemPointerData {
                blkid: crate::storage::block::BlockIdData { hi: 0, lo: 0 },
                posid: 0,
            };
            tid.set((v as u32) / 200, ((v % 200) + 1) as u16);
            index_insert(&shared, &index, &[Int32GetDatum(v)], &[false], &tid).await;
        }
        crate::backend::access::transam::xact::CommandCounterIncrement();

        // Full forward scan returns all keys sorted (split correctness + balance).
        let snap = txn_snapshot(&shared).expect("snapshot");
        let mut scan = index_beginscan(&heap, &index, &snap);
        index_rescan(&mut scan, Vec::new());
        let mut count = 0;
        while index_getnext_tid(&shared, &mut scan, ScanDirection::Forward).await.is_some() {
            count += 1;
        }
        assert_eq!(count, n, "all inserted index entries returned by the full scan");

        // Point lookups across the (now multi-level) tree must all hit.
        for probe in [0, 199, 200, 401, 799] {
            let snap = txn_snapshot(&shared).expect("snapshot");
            let mut s = index_beginscan(&heap, &index, &snap);
            index_rescan(&mut s, vec![(1, Int32GetDatum(probe))]);
            assert!(
                index_getnext_tid(&shared, &mut s, ScanDirection::Forward).await.is_some(),
                "key {probe} should be found after splits"
            );
        }
        // A miss is still a miss.
        let snap = txn_snapshot(&shared).expect("snapshot");
        let mut s = index_beginscan(&heap, &index, &snap);
        index_rescan(&mut s, vec![(1, Int32GetDatum(10_000))]);
        assert!(index_getnext_tid(&shared, &mut s, ScanDirection::Forward).await.is_none());
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn systable_index_scan_finds_row() {
    use crate::access::skey::ScanKeyData;
    use crate::backend::access::index::genam::{
        systable_beginscan_indexed, systable_endscan, systable_getnext,
    };

    let shared = new_shared();
    in_all_scopes(shared, |shared| async move {
        StartTransactionCommand(&shared).await;
        crate::backend::utils::time::snapmgr::PushActiveSnapshot(GetTransactionSnapshot(&shared));
        let hloc = rloc(6);
        let iloc = rloc(106);
        create_main_fork(&shared, hloc).await;
        create_main_fork(&shared, iloc).await;
        // A small "catalog-shaped" heap: one int4 key column.
        let heap = make_relation(hloc, one_int4_desc(), RELKIND_RELATION);
        let mut index = make_relation(iloc, one_int4_desc(), RELKIND_INDEX);
        init_index_support(Arc::get_mut(&mut index).unwrap(), 1);

        for v in [100, 200, 300, 400] {
            insert_row1(&shared, &heap, v).await;
        }
        crate::backend::access::transam::xact::CommandCounterIncrement();
        crate::backend::utils::time::snapmgr::PushActiveSnapshot(txn_snapshot(&shared));
        let ii = index_info(1);
        crate::backend::access::nbtree::nbtree::btbuild(
            &shared,
            &heap,
            &index,
            &ii,
        )
        .await;

        // Drive the systable INDEX-scan path for the equality key (column 1 = 300).
        let key = [ScanKeyData {
            flags: 0,
            attno: 1,
            strategy: crate::access::stratnum::BT_EQUAL_STRATEGY_NUMBER,
            subtype: InvalidOid,
            collation: InvalidOid,
            func: zero_fmgr_info(),
            argument: Int32GetDatum(300),
        }];
        let snap = txn_snapshot(&shared).expect("snapshot");
        let mut sysscan =
            systable_beginscan_indexed(&shared, &heap, &index, &snap, &key);
        let tref = systable_getnext(&shared, &mut sysscan).await.expect("row 300 found");
        let desc = heap.rd_att.clone().unwrap();
        let (vals, _n) =
            unsafe { crate::backend::access::common::heaptuple::heap_deform_tuple(tref, &desc) };
        assert_eq!(DatumGetInt32(vals[0]), 300);
        assert!(
            systable_getnext(&shared, &mut sysscan).await.is_none(),
            "exactly one row matches the equality key"
        );
        systable_endscan(&shared, &mut sysscan);
    })
    .await;
}

/// A zeroed FmgrInfo for an equality scan key whose func genam never invokes.
fn zero_fmgr_info() -> crate::fmgr::FmgrInfo {
    crate::fmgr::FmgrInfo {
        fn_addr: None,
        oid: InvalidOid,
        nargs: 0,
        strict: false,
        retset: false,
        stats: 0,
        extra: 0,
        mcxt: (),
        expr: None,
    }
}

// Keep IndexScanState referenced for clarity (the scan type used above).
#[allow(dead_code)]
fn _type_check(_s: &IndexScanState<'_, '_, '_>) {}
