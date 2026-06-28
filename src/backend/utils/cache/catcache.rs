//! The generic catalog cache. Translated from `src/backend/utils/cache/catcache.c`.
//!
//! A catcache keeps recently-read rows of one system catalog, keyed by 1..4
//! attributes, so repeated lookups (`SearchSysCacheN`) avoid a catalog scan. Each
//! backend has its own caches (PG: `CacheMemoryContext`); here the cache array is
//! per-task state, mirroring `inval.rs` (a `tokio::task_local!` + `RefCell`).
//!
//! Async coloring (rules.md s5):
//!  - A cache HIT is synchronous (it only touches in-memory state). The public
//!    `SearchSysCacheN` are therefore SYNC, matching their many sync callers
//!    (printtup/tupdesc/relation_open). On a cold MISS in a sync caller they
//!    return `None` (PG would scan; here the entry must have been warmed first).
//!  - A cache MISS scans the catalog through the heap AM, which is ASYNC. The
//!    populate path (`search_cat_cache_populate`) is async and is what the
//!    bootstrap / executor call to WARM a cache before the sync lookups run.
//!
//! This is the M2-faithful split of PG's single `SearchCatCacheInternal`: the
//! fast in-memory path is sync; the catalog-scan miss path is the async warm.

#![allow(
    clippy::future_not_send,
    reason = "rules.md s5: the catalog caches are PER-BACKEND task-confined state (raw HeapTuple/FmgrInfo pointers); their populate futures never migrate threads mid-await. await_holding_lock/refcell are clean (enforced)."
)]
#![allow(
    clippy::not_unsafe_ptr_arg_deref,
    reason = "catalog-cache routines take raw Relation/HeapTuple pointers per the C API; the deref is faithful to C (callers pass live handles)"
)]
#![allow(
    clippy::vec_box,
    reason = "CatCTup is boxed so its address (and the HeapTuple pointer returned into ct.tuple) stays stable when a bucket Vec reallocates"
)]
use std::cell::RefCell;

use crate::access::htup::{HeapTuple, HeapTupleData};
use crate::access::skey::ScanKeyData;
use crate::access::tupdesc::TupleDesc;
use crate::backend::access::common::heaptuple::{heap_freetuple, heap_getattr};
use crate::backend::access::heap::heapam::SendPtr;
use crate::backend::access::index::genam::{systable_beginscan, systable_endscan, systable_getnext};
use crate::postgres::{Datum, DatumGetInt16, DatumGetInt32, DatumGetObjectId};
use crate::postgres_ext::Oid;
use crate::shared_state::SharedState;
use crate::utils::snapshot::Snapshot;
use std::sync::Arc;

/// Max number of keys in a catcache (C `CATCACHE_MAXKEYS`).
pub const CATCACHE_MAXKEYS: usize = 4;

/// The fast-equal/hash key kinds the catalog indexes use. Catalog cache keys are
/// always one of these physical types (oid/regproc, int2, int4, or the fixed
/// `name`); we dispatch on the kind rather than threading the full fmgr.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyKind {
    Oid,
    Int2,
    Int4,
    Name,
}

/// One catalog cache: metadata (which catalog/keys) plus the hash buckets of
/// cached tuples. Runtime analog of C `CatCache` (the intrusive dlist buckets
/// collapse to `Vec`s of owned entries).
pub struct CatCache {
    /// Cache identifier (the `SysCacheIdentifier` discriminant).
    pub id: i32,
    /// OID of the cached catalog relation.
    pub cc_reloid: Oid,
    /// OID of the matching unique index (used by the staged index-scan arm).
    pub cc_indexoid: Oid,
    /// Number of lookup keys (1..4).
    pub cc_nkeys: usize,
    /// 1-based attribute number of each key.
    pub cc_keyno: [i16; CATCACHE_MAXKEYS],
    /// Equality/hash kind of each key.
    pub cc_keykind: [KeyKind; CATCACHE_MAXKEYS],
    /// Number of hash buckets.
    pub cc_nbuckets: usize,
    /// Buckets of cached entries.
    buckets: Vec<Vec<Box<CatCTup>>>,
    /// Whether the second-phase init (resolve key kinds from the tupdesc) is done.
    initialized: bool,
}

/// One cached catalog tuple (C `CatCTup`). Owns its tuple copy.
pub struct CatCTup {
    /// Hash value of this entry's keys.
    pub hash_value: u32,
    /// The lookup keys (by-value datums; name keys point into `tuple`).
    pub keys: [Datum; CATCACHE_MAXKEYS],
    /// Active reference count.
    pub refcount: i32,
    /// Negative (not-found) entry?
    pub negative: bool,
    /// The owned tuple copy (its `t_data` is freed on drop / cache reset).
    pub tuple: HeapTupleData,
}

impl CatCache {
    fn new(id: i32, reloid: Oid, indexoid: Oid, keyno: &[i16], nbuckets: usize) -> Self {
        let mut kn = [0i16; CATCACHE_MAXKEYS];
        for (i, &k) in keyno.iter().enumerate() {
            kn[i] = k;
        }
        Self {
            id,
            cc_reloid: reloid,
            cc_indexoid: indexoid,
            cc_nkeys: keyno.len(),
            cc_keyno: kn,
            cc_keykind: [KeyKind::Oid; CATCACHE_MAXKEYS],
            cc_nbuckets: nbuckets,
            buckets: (0..nbuckets).map(|_| Vec::new()).collect(),
            initialized: false,
        }
    }

    /// Resolve each key's physical kind from the catalog's tuple descriptor
    /// (C `CatalogCacheInitializeCache` reads `cc_tupdesc` to choose hash/eq fns).
    fn initialize(&mut self, tupdesc: &TupleDesc) {
        if self.initialized {
            return;
        }
        for i in 0..self.cc_nkeys {
            let att = tupdesc.attr((self.cc_keyno[i] - 1) as usize);
            self.cc_keykind[i] = match (att.attbyval, att.attlen) {
                (true, 2) => KeyKind::Int2,
                (false, 64) => KeyKind::Name,
                // by-value 4-byte (oid resolved below + int4) and any other: Int4.
                _ => KeyKind::Int4,
            };
            // oid-typed columns are by-value 4 bytes; treat them as Oid kind so
            // equality uses oid semantics (identical to int4 here, kept distinct
            // for faithfulness/readability).
            if att.atttypid == crate::catalog::genbki::OIDOID {
                self.cc_keykind[i] = KeyKind::Oid;
            }
        }
        self.initialized = true;
    }

    fn bucket_index(&self, hash: u32) -> usize {
        (hash as usize) & (self.cc_nbuckets - 1)
    }
}

/// Compute the hash of one key datum per its kind (self-contained integer hash;
/// in-memory bucketing only, so it need not match PG's wire hash).
fn hash_one_key(kind: KeyKind, v: Datum) -> u32 {
    let raw: u32 = match kind {
        KeyKind::Oid => DatumGetObjectId(v).0,
        KeyKind::Int4 => DatumGetInt32(v) as u32,
        KeyKind::Int2 => DatumGetInt16(v) as u32,
        KeyKind::Name => {
            // name key: hash the NUL-padded bytes.
            if v.0 == 0 {
                0
            } else {
                // SAFETY: a name Datum points at a NameData.
                let nd = unsafe { &*(v.0 as *const crate::c::NameData) };
                let mut h: u32 = 2166136261;
                for &b in &nd.data {
                    if b == 0 {
                        break;
                    }
                    h = (h ^ u32::from(b)).wrapping_mul(16777619);
                }
                return h;
            }
        }
    };
    // FNV-ish finalizer over the 4 raw bytes.
    let mut h: u32 = 2166136261;
    for b in raw.to_le_bytes() {
        h = (h ^ u32::from(b)).wrapping_mul(16777619);
    }
    h
}

/// Combine per-key hashes (C `CatalogCacheComputeHashValue`: XOR with per-key
/// left-rotation).
fn compute_hash(cache: &CatCache, keys: &[Datum]) -> u32 {
    let mut hv: u32 = 0;
    for i in (0..cache.cc_nkeys).rev() {
        let one = hash_one_key(cache.cc_keykind[i], keys[i]);
        hv ^= one.rotate_left((i as u32) * 8);
    }
    hv
}

/// Equality of two key datums of a given kind (C fast-equal).
fn key_eq(kind: KeyKind, a: Datum, b: Datum) -> bool {
    match kind {
        KeyKind::Oid => DatumGetObjectId(a) == DatumGetObjectId(b),
        KeyKind::Int4 => DatumGetInt32(a) == DatumGetInt32(b),
        KeyKind::Int2 => DatumGetInt16(a) == DatumGetInt16(b),
        KeyKind::Name => {
            if a.0 == 0 || b.0 == 0 {
                return a.0 == b.0;
            }
            // SAFETY: name Datums point at NameData.
            let pa = unsafe { &*(a.0 as *const crate::c::NameData) };
            let pb = unsafe { &*(b.0 as *const crate::c::NameData) };
            pa.data == pb.data
        }
    }
}

fn keys_match(cache: &CatCache, cached: &[Datum], search: &[Datum]) -> bool {
    (0..cache.cc_nkeys).all(|i| key_eq(cache.cc_keykind[i], cached[i], search[i]))
}

// ---------------------------------------------------------------------------
// Per-task cache state
// ---------------------------------------------------------------------------

/// The per-backend catalog caches (C: file-statics in `CacheMemoryContext`).
pub struct CatCacheState {
    /// Indexed by `SysCacheIdentifier` discriminant.
    pub caches: Vec<CatCache>,
}

tokio::task_local! {
    static CATCACHE_STATE: RefCell<CatCacheState>;
}

/// Initialize the per-task catcache state from the syscache descriptor table and
/// run `f`. Establishes the cache array before any lookup (PG `InitCatalogCache`).
pub fn scope<F, T>(f: F) -> T
where
    F: FnOnce() -> T,
{
    CATCACHE_STATE.sync_scope(RefCell::new(init_state()), f)
}

/// Async-scope variant for futures.
pub async fn scope_async<F, T>(fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    CATCACHE_STATE.scope(RefCell::new(init_state()), fut).await
}

fn init_state() -> CatCacheState {
    let caches = crate::backend::utils::cache::syscache::cacheinfo()
        .iter()
        .enumerate()
        .map(|(id, ci)| {
            CatCache::new(
                i32::try_from(id).unwrap_or(0),
                ci.reloid,
                ci.indoid,
                &ci.key[..ci.nkeys],
                ci.nbuckets,
            )
        })
        .collect();
    CatCacheState { caches }
}

/// Whether the per-task catcache state exists.
#[must_use]
pub fn state_present() -> bool {
    CATCACHE_STATE.try_with(|_| ()).is_ok()
}

/// Run `f` with the cache identified by `cache_id`, if state is present.
fn with_cache<R>(cache_id: usize, f: impl FnOnce(&mut CatCache) -> R) -> Option<R> {
    CATCACHE_STATE
        .try_with(|cell| {
            let mut st = cell.borrow_mut();
            f(&mut st.caches[cache_id])
        })
        .ok()
}

// ---------------------------------------------------------------------------
// Sync lookup (hit-only) + async populate (scan)
// ---------------------------------------------------------------------------

/// SYNC catalog-cache lookup (C `SearchCatCacheInternal`, hit path only). Returns
/// a held `HeapTuple` on a positive HIT, `None` on a negative HIT or a cold MISS.
/// The miss SCAN is the async `search_cat_cache_populate`; sync callers must have
/// warmed the entry (M2 warms during bootstrap).
#[must_use]
pub fn search_cat_cache(cache_id: usize, keys: &[Datum]) -> Option<HeapTuple> {
    with_cache(cache_id, |cache| {
        let search: Vec<Datum> = keys.iter().copied().take(cache.cc_nkeys).collect();
        if search.len() != cache.cc_nkeys {
            return None;
        }
        if !cache.initialized {
            // Cannot resolve key kinds without the tupdesc; treat as a miss. The
            // populate path initializes the cache. (PG defers init to first use.)
            return None;
        }
        let hv = compute_hash(cache, &search);
        let bi = cache.bucket_index(hv);
        let nkeys = cache.cc_nkeys;
        let keykind = cache.cc_keykind;
        for ct in &mut cache.buckets[bi] {
            if ct.hash_value != hv {
                continue;
            }
            let matched = (0..nkeys).all(|i| key_eq(keykind[i], ct.keys[i], search[i]));
            if !matched {
                continue;
            }
            if ct.negative {
                return None;
            }
            ct.refcount += 1;
            let ptr: *mut HeapTupleData = std::ptr::from_mut(&mut ct.tuple);
            return Some(ptr);
        }
        None
    })
    .flatten()
}

/// ASYNC catalog-cache populate (C `SearchCatCacheMiss`): scan the catalog for the
/// keyed row, add a positive or negative entry, and leave the positive entry with
/// refcount 1 (a held reference). Returns the held `HeapTuple` (positive) or
/// `None` (added a negative entry; the absence is now cached).
pub async fn search_cat_cache_populate(
    shared: &Arc<SharedState>,
    cache_id: usize,
    keys: &[Datum],
) -> Option<HeapTuple> {
    // Resolve cache metadata and ensure it's initialized from the relation's
    // tupdesc (we need it open to know the key kinds and to scan).
    let (reloid, indexoid, nkeys, keyno) = with_cache(cache_id, |c| {
        (c.cc_reloid, c.cc_indexoid, c.cc_nkeys, c.cc_keyno)
    })?;
    if keys.len() < nkeys {
        return None;
    }
    let search: Vec<Datum> = keys.iter().copied().take(nkeys).collect();

    // Open the catalog through the relcache. The M2 catalog caches read nailed
    // catalogs (pg_type/pg_proc/pg_class/pg_attribute), so we use the sync
    // `RelationIdGetRelation` directly and skip `table_open`'s AccessShareLock: the
    // nailed entry is never invalidated mid-scan, and the lock-tag path needs
    // `IsSharedRelation` (not yet translated). A non-nailed catalog is built async
    // first. Wrap as SendPtr so the handle can live across the scan's `.await`.
    if !crate::backend::utils::cache::relcache::is_nailed_catalog(reloid)
        && crate::utils::relcache::RelationIdGetRelation(reloid).is_none()
    {
        crate::backend::utils::cache::relcache::relation_build_desc(shared, reloid).await;
    }
    let relation = SendPtr(
        crate::utils::relcache::RelationIdGetRelation(reloid)?,
    );

    // Initialize key kinds from the relation's tupdesc.
    // SAFETY: open relation.
    let tupdesc = unsafe { (*relation.get()).rd_att.clone() };
    if let Some(td) = tupdesc.as_ref() {
        with_cache(cache_id, |c| c.initialize(td));
    }

    // Build the per-key scan keys (heap attno + argument); equality is applied in
    // genam (no fmgr needed for the M2 catalog key types).
    let mut skeys: Vec<ScanKeyData> = Vec::with_capacity(nkeys);
    for i in 0..nkeys {
        skeys.push(make_scan_key(keyno[i], search[i]));
    }

    let snapshot: Snapshot = None; // genam takes a catalog snapshot
    let mut scan = systable_beginscan(shared, relation.get(), indexoid, false, snapshot, &skeys);

    let matched = Box::pin(systable_getnext(shared, &mut scan)).await;
    let found: Option<HeapTuple> = matched.and_then(|ntp| {
        // ntp is an owned copy held by the scan; copy it into a cache entry.
        // SAFETY: ntp is a live tuple held by the scan.
        let tref = unsafe { &*ntp };
        let entry = build_entry(cache_id, tref, &search, false);
        insert_entry(cache_id, entry)
    });
    systable_endscan(shared, &mut scan);
    crate::utils::relcache::RelationClose(relation.get());

    if found.is_none() {
        // Add a negative entry (absence is cached) -- but NOT during bootstrap
        // processing: the catalogs are still being seeded, so a "miss" now may
        // become a hit once the row is inserted, and a cached negative entry would
        // wrongly shadow it (PG's SearchCatCacheMiss likewise skips negative
        // caching while bootstrapping).
        if !crate::miscadmin::is_bootstrap_processing_mode() {
            let entry = build_negative_entry(cache_id, &search);
            insert_entry(cache_id, entry);
        }
        return None;
    }
    found
}

/// Build a positive cache entry from a scanned tuple.
fn build_entry(cache_id: usize, tref: &HeapTupleData, search: &[Datum], negative: bool) -> CatCTup {
    // SAFETY: tref is a live tuple body.
    let copy = unsafe { crate::backend::access::common::heaptuple::heap_copytuple(tref) };
    let mut keys = [Datum(0); CATCACHE_MAXKEYS];
    // Re-extract key datums from the COPIED tuple so name keys point into it.
    let (nkeys, keyno) = with_cache(cache_id, |c| (c.cc_nkeys, c.cc_keyno)).unwrap_or((0, [0; 4]));
    if let Some(td) = entry_tupdesc(cache_id) {
        for i in 0..nkeys {
            // SAFETY: copy is a live tuple; keyno[i] valid.
            let (v, _isnull) = unsafe { heap_getattr(&copy, i32::from(keyno[i]), &td) };
            keys[i] = v;
        }
    } else {
        keys[..search.len()].copy_from_slice(search);
    }
    let hv = with_cache(cache_id, |c| compute_hash(c, &keys[..c.cc_nkeys])).unwrap_or(0);
    CatCTup { hash_value: hv, keys, refcount: 0, negative, tuple: copy }
}

/// Build a negative cache entry (fake tuple, key columns only).
fn build_negative_entry(cache_id: usize, search: &[Datum]) -> CatCTup {
    let mut keys = [Datum(0); CATCACHE_MAXKEYS];
    keys[..search.len()].copy_from_slice(search);
    let hv = with_cache(cache_id, |c| compute_hash(c, &keys[..c.cc_nkeys])).unwrap_or(0);
    let tuple = HeapTupleData {
        t_len: 0,
        t_self: crate::storage::itemptr::ItemPointerData {
            blkid: crate::storage::block::BlockIdData { hi: 0, lo: 0 },
            posid: 0,
        },
        t_tableOid: crate::postgres_ext::InvalidOid,
        t_data: core::ptr::null_mut(),
    };
    CatCTup { hash_value: hv, keys, refcount: 0, negative: true, tuple }
}

/// Insert an entry into its bucket; for positive entries set refcount 1 and return
/// the held tuple pointer.
fn insert_entry(cache_id: usize, entry: CatCTup) -> Option<HeapTuple> {
    let mut entry = Box::new(entry);
    with_cache(cache_id, |cache| {
        let bi = cache.bucket_index(entry.hash_value);
        let result = if entry.negative {
            None
        } else {
            entry.refcount = 1;
            let ptr: *mut HeapTupleData = std::ptr::from_mut(&mut entry.tuple);
            Some(ptr)
        };
        cache.buckets[bi].push(entry);
        result
    })
    .flatten()
}

/// The catalog's tuple descriptor, if the cache's relation is cached. Used to
/// re-extract key datums after copying. Falls back to a fresh open is avoided
/// here (the populate caller already opened it); returns None if unavailable.
fn entry_tupdesc(cache_id: usize) -> Option<TupleDesc> {
    let reloid = with_cache(cache_id, |c| c.cc_reloid)?;
    // The relation was just opened by the caller and is in the relcache.
    // RelationIdGetRelation bumps the refcount; balance it with RelationClose after
    // cloning the (Arc) tuple descriptor so the pin is not leaked.
    let rel = crate::utils::relcache::RelationIdGetRelation(reloid)?;
    // SAFETY: cached relation.
    let td = unsafe { (*rel).rd_att.clone() };
    crate::utils::relcache::RelationClose(rel);
    td
}

/// Construct a scan key for `attno` with `argument` (equality). The strategy/func
/// are nominal; genam applies equality directly for the catalog key types.
fn make_scan_key(attno: i16, argument: Datum) -> ScanKeyData {
    ScanKeyData {
        flags: 0,
        attno,
        strategy: crate::access::stratnum::BT_EQUAL_STRATEGY_NUMBER,
        subtype: crate::postgres_ext::InvalidOid,
        collation: crate::postgres_ext::InvalidOid,
        func: zero_fmgr_info(),
        argument,
    }
}

/// A zero-initialized `FmgrInfo` (genam applies equality directly for the catalog
/// key types, so the scan key's `func` is never invoked on the M2 heap-scan path).
fn zero_fmgr_info() -> crate::fmgr::FmgrInfo {
    crate::fmgr::FmgrInfo {
        fn_addr: None,
        oid: crate::postgres_ext::InvalidOid,
        nargs: 0,
        strict: false,
        retset: false,
        stats: 0,
        extra: 0,
        mcxt: core::ptr::null_mut(),
        expr: core::ptr::null_mut(),
    }
}

/// `ReleaseCatCache`: drop a reference taken by a successful search. The entry is
/// kept (LRU); only the refcount is decremented.
pub fn release_cat_cache(tuple: HeapTuple) {
    if tuple.is_null() {
        return;
    }
    CATCACHE_STATE
        .try_with(|cell| {
            let mut st = cell.borrow_mut();
            for cache in &mut st.caches {
                for bucket in &mut cache.buckets {
                    for ct in bucket {
                        let p: *mut HeapTupleData = std::ptr::from_mut(&mut ct.tuple);
                        if std::ptr::eq(p, tuple) && ct.refcount > 0 {
                            ct.refcount -= 1;
                            return;
                        }
                    }
                }
            }
        })
        .ok();
}

impl Drop for CatCTup {
    fn drop(&mut self) {
        if !self.tuple.t_data.is_null() {
            let t = HeapTupleData {
                t_len: self.tuple.t_len,
                t_self: self.tuple.t_self,
                t_tableOid: self.tuple.t_tableOid,
                t_data: self.tuple.t_data,
            };
            self.tuple.t_data = core::ptr::null_mut();
            heap_freetuple(t);
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, reason = "tests")]
#[allow(clippy::cast_ptr_alignment, reason = "GETSTRUCT overlay of a MAXALIGN'd tuple body to Form_pg_type")]
mod tests {
    use super::*;
    use crate::backend::access::common::heaptuple::heap_form_tuple;
    use crate::backend::access::heap::heapam::heap_insert;
    use crate::backend::utils::cache::relcache::{relation_close, relation_id_get_relation};
    use crate::backend::access::transam::xact::{
        CommandCounterIncrement, GetCurrentCommandId, StartTransactionCommand,
    };
    use crate::catalog::genbki::INT4OID;
    use crate::catalog::pg_type::{
        Natts_pg_type, TypeRelationId, TYPALIGN_INT, TYPSTORAGE_PLAIN, TYPTYPE_BASE,
        TYPCATEGORY_NUMERIC,
    };
    use crate::common::relpath::ForkNumber;
    use crate::postgres::{
        BoolGetDatum, CharGetDatum, Int16GetDatum, NameGetDatum, ObjectIdGetDatum,
    };
    use crate::shared_state::{SharedState, SharedStateConfig};
    use crate::utils::syscache::SysCacheIdentifier;

    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

    fn new_shared() -> Arc<SharedState> {
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("pepperdb-catcache-{}-{}", std::process::id(), n));
        let _ = std::fs::create_dir_all(dir.join(crate::access::xlog_internal::XLOGDIR));
        let _ = std::fs::create_dir_all(dir.join("base").join("90000"));
        SharedState::new(SharedStateConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            nbuffers: 64,
            ..Default::default()
        })
    }

    /// Set up the whole per-task scope stack (session/resowner/xact/snapmgr/
    /// combocid/WAL + inval + the catalog caches) and run the async body. The
    /// session's database id is set to 90000 so the nailed catalogs' relfilenode
    /// locator (db = current database) matches the heap files we create.
    async fn in_scopes<F, Fut, T>(shared: Arc<SharedState>, f: F) -> T
    where
        F: FnOnce(Arc<SharedState>) -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        use crate::backend::access::transam::xloginsert::with_insertion;
        use crate::backend::utils::cache::relcache::scope_async as relcache_scope;
        use crate::backend::utils::time::{combocid::combocid_scope, snapmgr::snapmgr_scope};
        let sess = Arc::new(crate::session::Session::new(crate::miscadmin::BackendType::BACKEND));
        sess.set_database_id(Oid(90000));
        sess.set_database_tablespace(crate::common::relpath::DEFAULTTABLESPACE_OID);
        let owner = crate::backend::utils::resowner::resowner::ResourceOwner::create(None, "Test");
        // Box::pin each scope layer's inner future so the deeply-nested
        // TaskLocalFuture stack stays heap-allocated (debug builds otherwise
        // overflow the 2 MB test-thread stack).
        let body = Box::pin(scope_async(relcache_scope(Box::pin(f(shared)))));
        let body = Box::pin(with_insertion(body));
        let body = Box::pin(combocid_scope(body));
        let body = Box::pin(snapmgr_scope(body));
        let body = Box::pin(crate::backend::access::transam::xact::xact_scope(body));
        crate::session::scope(
            sess,
            crate::backend::utils::resowner::resowner::scope(owner, body),
        )
        .await
    }

    /// Create the relation's main fork file (so the buffer pool can extend it) at a
    /// catalog's nailed locator (filenode == oid, db 90000, pg_default tablespace).
    async fn create_catalog_fork(shared: &Arc<SharedState>, relid: Oid) {
        let loc = crate::storage::relfilelocator::RelFileLocator {
            spcOid: crate::common::relpath::DEFAULTTABLESPACE_OID,
            dbOid: Oid(90000),
            relNumber: relid,
        };
        let mut smgr = crate::storage::smgr::SmgrRelation::open(
            loc,
            crate::storage::procnumber::INVALID_PROC_NUMBER,
        );
        smgr.create(shared, ForkNumber::MAIN_FORKNUM, false).await;
    }

    /// Seed one pg_type row (for the int4 type) into the open pg_type heap. Only the
    /// fixed columns are filled (non-null, so GETSTRUCT's struct overlay is valid);
    /// the trailing varlena columns are NULL.
    #[allow(clippy::future_not_send, reason = "test helper")]
    async fn seed_int4_pg_type(shared: &Arc<SharedState>, name: &crate::c::NameData) {
        use crate::catalog::pg_type as t;
        let pg_type = relation_id_get_relation(TypeRelationId).expect("pg_type nailed");
        // SAFETY: nailed relation has a descriptor.
        let desc = unsafe { (*pg_type).rd_att.clone() }.expect("pg_type desc");
        let natts = Natts_pg_type as usize;

        let mut values = vec![Datum(0); natts];
        let mut isnull = vec![false; natts];
        // Fill the fixed columns by attnum-1. Anum_* are 1-based.
        let set = |v: &mut [Datum], anum: i32, d: Datum| v[(anum - 1) as usize] = d;
        set(&mut values, t::Anum_pg_type_oid, ObjectIdGetDatum(INT4OID));
        set(&mut values, t::Anum_pg_type_typname, NameGetDatum(name));
        set(&mut values, t::Anum_pg_type_typnamespace, ObjectIdGetDatum(Oid(11)));
        set(&mut values, t::Anum_pg_type_typowner, ObjectIdGetDatum(Oid(10)));
        set(&mut values, t::Anum_pg_type_typlen, Int16GetDatum(4));
        set(&mut values, t::Anum_pg_type_typbyval, BoolGetDatum(true));
        set(&mut values, t::Anum_pg_type_typtype, CharGetDatum(TYPTYPE_BASE));
        set(&mut values, t::Anum_pg_type_typcategory, CharGetDatum(TYPCATEGORY_NUMERIC));
        set(&mut values, t::Anum_pg_type_typispreferred, BoolGetDatum(false));
        set(&mut values, t::Anum_pg_type_typisdefined, BoolGetDatum(true));
        set(&mut values, t::Anum_pg_type_typdelim, CharGetDatum(b',' as i8));
        set(&mut values, t::Anum_pg_type_typrelid, ObjectIdGetDatum(Oid(0)));
        set(&mut values, t::Anum_pg_type_typsubscript, ObjectIdGetDatum(Oid(0)));
        set(&mut values, t::Anum_pg_type_typelem, ObjectIdGetDatum(Oid(0)));
        set(&mut values, t::Anum_pg_type_typarray, ObjectIdGetDatum(Oid(1007)));
        set(&mut values, t::Anum_pg_type_typinput, ObjectIdGetDatum(crate::utils::fmgroids::F_INT4IN));
        set(&mut values, t::Anum_pg_type_typoutput, ObjectIdGetDatum(crate::utils::fmgroids::F_INT4OUT));
        set(&mut values, t::Anum_pg_type_typreceive, ObjectIdGetDatum(Oid(0)));
        set(&mut values, t::Anum_pg_type_typsend, ObjectIdGetDatum(Oid(0)));
        set(&mut values, t::Anum_pg_type_typmodin, ObjectIdGetDatum(Oid(0)));
        set(&mut values, t::Anum_pg_type_typmodout, ObjectIdGetDatum(Oid(0)));
        set(&mut values, t::Anum_pg_type_typanalyze, ObjectIdGetDatum(Oid(0)));
        set(&mut values, t::Anum_pg_type_typalign, CharGetDatum(TYPALIGN_INT));
        set(&mut values, t::Anum_pg_type_typstorage, CharGetDatum(TYPSTORAGE_PLAIN));
        set(&mut values, t::Anum_pg_type_typnotnull, BoolGetDatum(false));
        set(&mut values, t::Anum_pg_type_typbasetype, ObjectIdGetDatum(Oid(0)));
        set(&mut values, t::Anum_pg_type_typtypmod, crate::postgres::Int32GetDatum(-1));
        set(&mut values, t::Anum_pg_type_typndims, crate::postgres::Int32GetDatum(0));
        set(&mut values, t::Anum_pg_type_typcollation, ObjectIdGetDatum(Oid(0)));
        // Trailing varlena columns are NULL.
        isnull[(t::Anum_pg_type_typdefaultbin - 1) as usize] = true;
        isnull[(t::Anum_pg_type_typdefault - 1) as usize] = true;
        isnull[(t::Anum_pg_type_typacl - 1) as usize] = true;

        let mut tuple = heap_form_tuple(&desc, &values, &isnull);
        let cid = GetCurrentCommandId(true);
        heap_insert(
            shared,
            crate::backend::access::heap::heapam::SendPtr(pg_type),
            crate::backend::access::heap::heapam::SendPtr(std::ptr::from_mut(&mut tuple)),
            cid,
            0,
        )
        .await;
        relation_close(pg_type);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn searchsyscache_typeoid_returns_int4_row_and_typeoutput() {
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            StartTransactionCommand(&shared).await;
            // Phase 3: nail pg_class/pg_attribute/pg_proc/pg_type.
            crate::backend::utils::cache::relcache::relation_cache_initialize_phase3();

            // RelationIdGetRelation on a nailed catalog returns the formrdesc desc.
            let pg_type = relation_id_get_relation(TypeRelationId).expect("nailed pg_type");
            // SAFETY: nailed relation.
            let natts = unsafe { (*pg_type).rd_att.as_ref().unwrap().natts };
            assert_eq!(natts, Natts_pg_type);
            relation_close(pg_type);

            // Create the pg_type heap file and seed the int4 row.
            create_catalog_fork(&shared, TypeRelationId).await;
            let name = {
                let mut nd = crate::c::NameData { data: [0u8; crate::c::NAMEDATALEN] };
                nd.data[..4].copy_from_slice(b"int4");
                nd
            };
            seed_int4_pg_type(&shared, &name).await;
            CommandCounterIncrement();

            // SearchSysCache1(TYPEOID, int4) -- async warm then a cache HIT.
            let tup = crate::backend::utils::cache::syscache::search_sys_cache_populate(
                &shared,
                SysCacheIdentifier::TYPEOID,
                &[ObjectIdGetDatum(INT4OID)],
            )
            .await;
            let tup = tup.expect("int4 pg_type row found via syscache");
            // GETSTRUCT overlay reads the typoutput column.
            // SAFETY: held syscache tuple over a pg_type row.
            let pt = unsafe {
                &*crate::access::htup_details::GETSTRUCT(&*tup)
                    .cast::<crate::catalog::pg_type::FormData_pg_type>()
            };
            assert_eq!(pt.oid, INT4OID);
            assert_eq!(pt.typlen, 4);
            assert!(pt.typbyval);
            assert_eq!(pt.typoutput, crate::utils::fmgroids::F_INT4OUT);
            crate::backend::utils::cache::syscache::release_sys_cache(tup);

            // getTypeOutputInfo now goes through the real (warm) syscache.
            let (typoutput, isvarlena) =
                crate::backend::utils::cache::lsyscache::get_type_output_info(INT4OID);
            assert_eq!(typoutput, crate::utils::fmgroids::F_INT4OUT);
            assert!(!isvarlena);

            // A second sync lookup is a pure HIT.
            let hit = search_cat_cache(
                SysCacheIdentifier::TYPEOID as usize,
                &[ObjectIdGetDatum(INT4OID)],
            );
            assert!(hit.is_some());
            release_cat_cache(hit.unwrap());
        }))
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn negative_entry_for_missing_key() {
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            StartTransactionCommand(&shared).await;
            crate::backend::utils::cache::relcache::relation_cache_initialize_phase3();
            create_catalog_fork(&shared, TypeRelationId).await;
            CommandCounterIncrement();

            // Look up a type OID that is not seeded -> miss -> negative entry cached.
            let missing = Oid(999_999);
            let r = crate::backend::utils::cache::syscache::search_sys_cache_populate(
                &shared,
                SysCacheIdentifier::TYPEOID,
                &[ObjectIdGetDatum(missing)],
            )
            .await;
            assert!(r.is_none(), "missing key yields no tuple");

            // A subsequent SYNC lookup is a negative-entry HIT (still None), proving
            // the absence was cached (no rescan needed).
            let r2 = search_cat_cache(SysCacheIdentifier::TYPEOID as usize, &[ObjectIdGetDatum(missing)]);
            assert!(r2.is_none());
            // The negative entry exists in the cache.
            let present = CATCACHE_STATE.with(|cell| {
                let st = cell.borrow();
                let cache = &st.caches[SysCacheIdentifier::TYPEOID as usize];
                cache.buckets.iter().flatten().any(|ct| ct.negative)
            });
            assert!(present, "a negative entry was cached for the missing key");
        }))
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn releasesyscache_decrements_refcount() {
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            StartTransactionCommand(&shared).await;
            crate::backend::utils::cache::relcache::relation_cache_initialize_phase3();
            create_catalog_fork(&shared, TypeRelationId).await;
            let name = {
                let mut nd = crate::c::NameData { data: [0u8; crate::c::NAMEDATALEN] };
                nd.data[..4].copy_from_slice(b"int4");
                nd
            };
            seed_int4_pg_type(&shared, &name).await;
            CommandCounterIncrement();

            let tup = crate::backend::utils::cache::syscache::search_sys_cache_populate(
                &shared,
                SysCacheIdentifier::TYPEOID,
                &[ObjectIdGetDatum(INT4OID)],
            )
            .await
            .expect("found");
            // refcount is 1 after the warm search holds a reference.
            let rc_before = CATCACHE_STATE.with(|cell| {
                cell.borrow().caches[SysCacheIdentifier::TYPEOID as usize]
                    .buckets
                    .iter()
                    .flatten()
                    .find(|ct| !ct.negative)
                    .map(|ct| ct.refcount)
            });
            assert_eq!(rc_before, Some(1));
            release_cat_cache(tup);
            let rc_after = CATCACHE_STATE.with(|cell| {
                cell.borrow().caches[SysCacheIdentifier::TYPEOID as usize]
                    .buckets
                    .iter()
                    .flatten()
                    .find(|ct| !ct.negative)
                    .map(|ct| ct.refcount)
            });
            assert_eq!(rc_after, Some(0));
        }))
        .await;
    }
}
