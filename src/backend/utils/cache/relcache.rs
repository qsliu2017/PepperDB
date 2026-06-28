//! The relation descriptor cache. Translated from the M2-reachable parts of
//! `src/backend/utils/cache/relcache.c`.
//!
//! The relcache maps a relation OID to its open `RelationData` (tuple descriptor,
//! `rd_rel`, access-method info). It is per-backend (PG: `CacheMemoryContext`);
//! here it is per-task state (a `tokio::task_local!` + `RefCell`, like inval.rs),
//! owning each entry in a `Box<RelationData>`.
//!
//! Bootstrap sequencing (PG `RelationCacheInitializePhase2/3`):
//!  1. Phase 2/3 builds NAILED `RelationData` for the formrdesc catalogs
//!     (pg_class/pg_attribute/pg_proc/pg_type) from the step-10 compiled-in schema
//!     -- NO syscache, NO disk. This breaks the relcache<->catalog cycle.
//!  2. With the nailed catalogs open, catcache/syscache populate by SCANNING them.
//!  3. A NON-nailed relation is built by `RelationBuildDesc`: read its pg_class row
//!     and its pg_attribute rows (heap scans) into a fresh `RelationData`.
//!
//! Async coloring (rules.md s5): a cache HIT and the nailed-catalog lookup are
//! SYNC (`RelationIdGetRelation`, the signature `relation_open` calls). The build
//! path scans the heap and is ASYNC (`RelationBuildDesc`, the Phase2/3 init); the
//! M2 path warms the relcache before the sync opens run.

#![allow(
    clippy::future_not_send,
    reason = "rules.md s5: the catalog caches are PER-BACKEND task-confined state (raw HeapTuple/FmgrInfo pointers); their populate futures never migrate threads mid-await. await_holding_lock/refcell are clean (enforced)."
)]
#![allow(
    clippy::not_unsafe_ptr_arg_deref,
    reason = "catalog-cache routines take raw Relation/HeapTuple pointers per the C API; the deref is faithful to C (callers pass live handles)"
)]
#![allow(
    clippy::cast_ptr_alignment,
    reason = "faithful GETSTRUCT reinterpretation of a heap tuple to a Form_* struct (MAXALIGN'd body covers the Form alignment)"
)]
use std::cell::RefCell;
use std::sync::Arc;

use crate::access::htup::HeapTupleData;
use crate::access::htup_details::GETSTRUCT;
use crate::access::skey::ScanKeyData;
use crate::backend::access::common::heaptuple::{heap_copytuple, heap_freetuple};
use crate::backend::access::heap::heapam::SendPtr;
use crate::backend::access::index::genam::{systable_beginscan, systable_endscan, systable_getnext};
use crate::backend::bootstrap::bootstrap::{formrdesc_tupdesc, BootstrapCatalog, FORMRDESC_CATALOGS};
use crate::catalog::pg_attribute::{
    AttributeRelationId, Anum_pg_attribute_attrelid,
};
use crate::catalog::pg_class::{
    Form_pg_class, FormData_pg_class, RelationRelationId, Anum_pg_class_oid, RELKIND_RELATION,
    RELPERSISTENCE_PERMANENT,
};
use crate::postgres::ObjectIdGetDatum;
use crate::postgres_ext::Oid;
use crate::shared_state::SharedState;
use crate::utils::rel::{LockRelId, RelationData};
use crate::utils::relcache::Relation;

// ---------------------------------------------------------------------------
// Per-task relcache state
// ---------------------------------------------------------------------------

struct RelCacheState {
    /// OID -> owned relation descriptor (C `RelationIdCache` dynahash).
    by_oid: std::collections::HashMap<u32, Box<RelationData>>,
    /// Whether the critical (formrdesc) catalogs have been nailed.
    phase3_done: bool,
}

impl RelCacheState {
    fn new() -> Self {
        Self { by_oid: std::collections::HashMap::new(), phase3_done: false }
    }
}

tokio::task_local! {
    static RELCACHE_STATE: RefCell<RelCacheState>;
}

/// Establish the per-task relcache (and the catcache it cooperates with) and run
/// `fut`. Used by the bootstrap entry and tests.
pub async fn scope_async<F, T>(fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    RELCACHE_STATE
        .scope(RefCell::new(RelCacheState::new()), fut)
        .await
}

/// Sync scope (no async body).
pub fn scope<F, T>(f: F) -> T
where
    F: FnOnce() -> T,
{
    RELCACHE_STATE.sync_scope(RefCell::new(RelCacheState::new()), f)
}

#[must_use]
pub fn state_present() -> bool {
    RELCACHE_STATE.try_with(|_| ()).is_ok()
}

fn with_state<R>(f: impl FnOnce(&mut RelCacheState) -> R) -> Option<R> {
    RELCACHE_STATE.try_with(|cell| f(&mut cell.borrow_mut())).ok()
}

/// Look up a cached relation by OID, returning a raw handle into the owned entry.
fn cache_lookup(relid: Oid) -> Option<Relation> {
    with_state(|st| {
        st.by_oid
            .get_mut(&relid.0)
            .map(|b| std::ptr::from_mut::<RelationData>(b.as_mut()))
    })
    .flatten()
}

/// Insert an owned relation; return a handle into the stored entry.
fn cache_insert(rel: Box<RelationData>) -> Relation {
    with_state(|st| {
        let oid = rel.rd_id.0;
        let entry = st.by_oid.entry(oid).or_insert(rel);
        std::ptr::from_mut::<RelationData>(entry.as_mut())
    })
    .unwrap_or(core::ptr::null_mut())
}

// ---------------------------------------------------------------------------
// RelationIdGetRelation (sync) + open/close
// ---------------------------------------------------------------------------

/// `RelationIdGetRelation`: open a relation by OID (SYNC). Returns a cached or
/// nailed entry; for a NOT-yet-cached non-nailed relation it returns `None` (PG
/// builds it here, but the build scans the heap and is async -- the M2 path warms
/// the entry first via [`relation_build_desc`] / Phase3). Increments the entry's
/// reference count on success.
#[must_use]
pub fn relation_id_get_relation(relation_id: Oid) -> Option<Relation> {
    let rd = cache_lookup(relation_id)?;
    // SAFETY: handle into a live owned entry.
    unsafe {
        if (*rd).rd_droppedSubid != crate::c::InvalidSubTransactionId {
            return None;
        }
        (*rd).rd_refcnt += 1;
    }
    Some(rd)
}

/// `RelationClose`: decrement the reference count. The entry stays cached (LRU);
/// nailed entries are never freed.
pub fn relation_close(relation: Relation) {
    if relation.is_null() {
        return;
    }
    // SAFETY: live relation handle.
    unsafe {
        if (*relation).rd_refcnt > 0 {
            (*relation).rd_refcnt -= 1;
        }
    }
}

// ---------------------------------------------------------------------------
// Phase 2/3: nail the formrdesc catalogs
// ---------------------------------------------------------------------------

/// `RelationCacheInitialize`: create the (empty) relcache. The per-task state is
/// established by [`scope`]/[`scope_async`]; this is a no-op placeholder for the
/// PG entry point.
pub fn relation_cache_initialize() {}

/// `RelationCacheInitializePhase3` (the local-catalog half): build a nailed
/// `RelationData` for each formrdesc catalog (pg_class/pg_attribute/pg_proc/
/// pg_type) from the step-10 compiled-in schema -- no syscache, no disk. After
/// this, the catalog caches can populate by scanning the nailed catalogs.
///
/// The shared-catalog half (Phase 2: pg_database/pg_authid/...) is deep-deferred
/// (not on the M2 path).
pub fn relation_cache_initialize_phase3() {
    if with_state(|st| st.phase3_done).unwrap_or(true) {
        return;
    }
    for cat in FORMRDESC_CATALOGS {
        nail_formrdesc_catalog(cat);
    }
    with_state(|st| st.phase3_done = true);
}

/// Build and insert the nailed `RelationData` for one bootstrap catalog (PG
/// `formrdesc`). The tuple descriptor is the step-10 compiled-in one; the rest of
/// the descriptor (`rd_rel`, nailing, lock info) is filled here.
fn nail_formrdesc_catalog(cat: &BootstrapCatalog) {
    let desc = formrdesc_tupdesc(cat.relid, cat.reltype, cat.schema);

    let mut rel = Box::new(RelationData::blank());
    rel.rd_id = cat.relid;
    rel.rd_isnailed = true;
    rel.rd_isvalid = true;
    rel.rd_refcnt = 1;
    rel.rd_att = Some(desc);

    // Physical address (PG RelationInitPhysicalAddr, bootstrap form: the
    // relfilenode equals the OID). Shared catalogs live in db 0; local catalogs in
    // the current database. The default tablespace is pg_default (1663).
    let dbid = if cat.isshared {
        crate::postgres_ext::InvalidOid
    } else {
        crate::session::current().database_id()
    };
    rel.rd_locator = crate::storage::relfilelocator::RelFileLocator {
        spcOid: crate::common::relpath::DEFAULTTABLESPACE_OID,
        dbOid: dbid,
        relNumber: cat.relid, // RelFileNumber = Oid; bootstrap: filenode == oid
    };
    rel.rd_lockInfo = crate::utils::rel::LockInfoData {
        lockRelId: LockRelId { relId: cat.relid, dbId: dbid },
    };

    // Fake up a minimal rd_rel (Form_pg_class). PG copies the compiled-in
    // attributes; M2 needs relkind/relam/relnatts/relisshared/relnamespace so heap
    // scans and the relcache predicates work.
    let mut form = Box::new(blank_pg_class());
    namestrcpy(&mut form.relname, cat.relname);
    form.oid = cat.relid;
    form.relnamespace = PG_CATALOG_NAMESPACE;
    form.reltype = cat.reltype;
    form.relisshared = cat.isshared;
    form.relkind = RELKIND_RELATION;
    form.relpersistence = RELPERSISTENCE_PERMANENT;
    form.relam = HEAP_TABLE_AM_OID;
    form.relnatts = i16::try_from(cat.schema.len()).unwrap_or(0);
    rel.rd_rel = Box::into_raw(form);

    cache_insert(rel);
}

/// Whether a relation OID has a nailed (formrdesc) entry.
#[must_use]
pub fn is_nailed_catalog(relid: Oid) -> bool {
    FORMRDESC_CATALOGS.iter().any(|c| c.relid == relid)
}

// ---------------------------------------------------------------------------
// RelationBuildDesc (async): build a non-nailed relation from pg_class/pg_attribute
// ---------------------------------------------------------------------------

/// `RelationBuildDesc`: build a relation descriptor by reading its pg_class row and
/// pg_attribute rows (heap scans). ASYNC (scans the buffer pool). On success the
/// new entry is inserted into the relcache and a handle returned; `None` if the
/// relation does not exist. The M2 path calls this to WARM the relcache before the
/// sync `relation_open`/`RelationIdGetRelation`.
pub async fn relation_build_desc(shared: &Arc<SharedState>, target_rel_id: Oid) -> Option<Relation> {
    if let Some(r) = cache_lookup(target_rel_id) {
        return Some(r);
    }

    // 1. Read the pg_class row for this OID (PG ScanPgRelation). Read everything we
    //    need out of the tuple and free it BEFORE the next scan's `.await`, so no
    //    non-Send `HeapTupleData` (raw t_data) is held across the await.
    let pg_class_tuple = Box::pin(scan_pg_relation(shared, target_rel_id)).await?;
    // SAFETY: a live owned tuple copy; relp -> its pg_class fixed part.
    let form_copy = {
        let relp: Form_pg_class = GETSTRUCT(&pg_class_tuple).cast::<FormData_pg_class>();
        // SAFETY: relp points at the pg_class fixed part of the owned tuple copy.
        Box::new(unsafe { core::ptr::read(relp) })
    };
    let relkind = form_copy.relkind;
    let relnatts = form_copy.relnatts;
    heap_freetuple(pg_class_tuple); // data is now copied into form_copy

    // 2. Allocate the descriptor and copy rd_rel.
    let mut rel = Box::new(RelationData::blank());
    rel.rd_id = target_rel_id;
    rel.rd_isnailed = false;
    rel.rd_isvalid = true;
    rel.rd_refcnt = 0;
    rel.rd_lockInfo = crate::utils::rel::LockInfoData {
        lockRelId: LockRelId { relId: target_rel_id, dbId: crate::postgres_ext::InvalidOid },
    };
    rel.rd_rel = Box::into_raw(form_copy);

    // 3. Build the tuple descriptor from pg_attribute (PG RelationBuildTupleDesc).
    let td = Box::pin(relation_build_tuple_desc(shared, target_rel_id, relnatts)).await;
    rel.rd_att = Some(td);

    // 4. Access-method info (index relations only). M2 reaches table rels; the
    //    index path is RelationInitIndexAccessInfo (staged support-proc resolution).
    if relkind == crate::catalog::pg_class::RELKIND_INDEX {
        relation_init_index_access_info(std::ptr::from_mut::<RelationData>(rel.as_mut()));
    }

    Some(cache_insert(rel))
}

/// `ScanPgRelation`: fetch the pg_class tuple for `target_rel_id` (heap scan of
/// pg_class on its oid key). Returns an owned tuple copy.
async fn scan_pg_relation(shared: &Arc<SharedState>, target_rel_id: Oid) -> Option<HeapTupleData> {
    // pg_class is nailed; open it (sync, nailed -> no build). SendPtr so the handle
    // can live across the scan's `.await`.
    let pg_class = SendPtr(relation_id_get_relation(RelationRelationId)?);
    let key = [ScanKeyData {
        flags: 0,
        attno: Anum_pg_class_oid as i16,
        strategy: crate::access::stratnum::BT_EQUAL_STRATEGY_NUMBER,
        subtype: crate::postgres_ext::InvalidOid,
        collation: crate::postgres_ext::InvalidOid,
        func: zero_fmgr_info(),
        argument: ObjectIdGetDatum(target_rel_id),
    }];
    let mut scan =
        systable_beginscan(shared, pg_class.get(), crate::postgres_ext::InvalidOid, false, None, &key);
    let result = Box::pin(systable_getnext(shared, &mut scan))
        .await
        // SAFETY: live scan tuple; copy before endscan.
        .map(|t| unsafe { heap_copytuple(&*t) });
    systable_endscan(shared, &mut scan);
    relation_close(pg_class.get());
    result
}

/// `RelationBuildTupleDesc`: read the relation's pg_attribute rows (attnum > 0)
/// and build the tuple descriptor. Heap-scans pg_attribute filtered by attrelid.
async fn relation_build_tuple_desc(
    shared: &Arc<SharedState>,
    relid: Oid,
    relnatts: i16,
) -> crate::access::tupdesc::TupleDesc {
    use crate::access::tupdesc::TupleDescData;
    use crate::catalog::pg_attribute::FormData_pg_attribute;

    let natts = relnatts.max(0);
    let mut desc = TupleDescData::create_template(i32::from(natts));
    desc.tdtypmod = -1;

    let pg_attribute = SendPtr(
        relation_id_get_relation(AttributeRelationId)
            .unwrap_or_else(|| unreachable!("pg_attribute is nailed")),
    );
    let key = [ScanKeyData {
        flags: 0,
        attno: Anum_pg_attribute_attrelid as i16,
        strategy: crate::access::stratnum::BT_EQUAL_STRATEGY_NUMBER,
        subtype: crate::postgres_ext::InvalidOid,
        collation: crate::postgres_ext::InvalidOid,
        func: zero_fmgr_info(),
        argument: ObjectIdGetDatum(relid),
    }];
    let mut scan = systable_beginscan(
        shared,
        pg_attribute.get(),
        crate::postgres_ext::InvalidOid,
        false,
        None,
        &key,
    );

    while let Some(t) = Box::pin(systable_getnext(shared, &mut scan)).await {
        // SAFETY: live scan tuple.
        let tref: &HeapTupleData = unsafe { &*t };
        let attp = GETSTRUCT(tref).cast::<FormData_pg_attribute>();
        // SAFETY: attp points at the pg_attribute fixed part.
        let att = unsafe { &*attp };
        let attnum = att.attnum;
        if attnum < 1 || i32::from(attnum) > i32::from(natts) {
            continue; // system / out-of-range columns
        }
        // SAFETY: attp valid; copy the fixed part into the descriptor slot.
        let slot = &mut desc.attrs[(attnum - 1) as usize];
        *slot = unsafe { core::ptr::read(attp) };
        desc.populate_compact_attribute((attnum - 1) as usize);
    }

    systable_endscan(shared, &mut scan);
    relation_close(pg_attribute.get());

    Arc::new(desc)
}

// ---------------------------------------------------------------------------
// RelationInitIndexAccessInfo (what step-13 btree consumes)
// ---------------------------------------------------------------------------

/// btree's `amsupport`: the number of support procedures per opclass column.
/// Mirrors `BTNProcs` (nbtree.h). The relcache arrays for a btree index are
/// sized `indnatts * BT_AMSUPPORT`.
pub const BT_AMSUPPORT: usize = crate::access::nbtree::BTNProcs as usize;

/// `RelationInitIndexAccessInfo`: allocate an index relation's per-column access
/// arrays (`rd_indoption`, `rd_opfamily`, `rd_opcintype`, `rd_support`,
/// `rd_supportinfo`, `rd_indcollation`) and zero them, sized from the index's
/// pg_index row (`rd_index`).
///
/// The support-proc OIDs (`rd_support`) and per-column metadata are filled by
/// [`index_init_opclass_support`] from each key column's opclass. The C path
/// reads pg_opclass + pg_amproc via syscache inside `IndexSupportInitialize`; in
/// the M2 port the catalog seed rows are not yet loaded into a scannable heap
/// (that is step 15: `index_create` + bootstrap seeding), so callers that build
/// an index relation (tests, and step-15 `index_create`) drive
/// `index_init_opclass_support` with the column opclass OIDs directly. Once the
/// catalogs are populated the syscache-driven resolution can supersede it.
///
/// `rd_supportinfo` entries start with `fn_oid = InvalidOid`; `index_getprocinfo`
/// fills each on first use via `fmgr_info` (the builtin fast path resolves the
/// `bt*cmp` comparators).
pub fn relation_init_index_access_info(relation: Relation) {
    if relation.is_null() {
        return;
    }
    // SAFETY: live index relation.
    let rel = unsafe { &mut *relation };
    if rel.rd_index.is_null() {
        // No pg_index row attached yet: leave the descriptor index-invalid; the
        // builder will attach rd_index and call index_init_opclass_support.
        rel.rd_indexvalid = false;
        return;
    }
    // SAFETY: rd_index points at this index's pg_index fixed part.
    let (indnatts, indnkeyatts) = unsafe {
        ((*rel.rd_index).indnatts as usize, (*rel.rd_index).indnkeyatts as usize)
    };

    alloc_index_arrays(rel, indnatts, indnkeyatts);
    rel.rd_indexvalid = true;
}

/// Allocate (boxed-leak) the zeroed per-column index-access arrays on `rel`. The
/// support arrays span `indnatts` columns (included columns have no opclass, so
/// opclass arrays span only `indnkeyatts`). Idempotent: frees prior arrays.
fn alloc_index_arrays(rel: &mut RelationData, indnatts: usize, indnkeyatts: usize) {
    use crate::c::RegProcedure;
    use crate::fmgr::FmgrInfo;
    use crate::postgres_ext::InvalidOid;

    free_index_arrays(rel);

    let nsupport = indnatts * BT_AMSUPPORT;
    rel.rd_support = leak_slice::<RegProcedure>(vec![InvalidOid; nsupport]);
    rel.rd_supportinfo = leak_slice::<FmgrInfo>(
        (0..nsupport).map(|_| zero_fmgr_info()).collect::<Vec<_>>(),
    );
    rel.rd_opfamily = leak_slice::<Oid>(vec![InvalidOid; indnkeyatts]);
    rel.rd_opcintype = leak_slice::<Oid>(vec![InvalidOid; indnkeyatts]);
    rel.rd_indcollation = leak_slice::<Oid>(vec![InvalidOid; indnkeyatts]);
    rel.rd_indoption = leak_slice::<i16>(vec![0i16; indnkeyatts]);
}

/// Free the boxed index-access arrays previously allocated by [`alloc_index_arrays`].
fn free_index_arrays(rel: &mut RelationData) {
    use crate::c::RegProcedure;
    use crate::fmgr::FmgrInfo;
    // SAFETY: each pointer was produced by leak_slice (Box::into_raw of a slice's
    // first element with a known length recorded by the consumer); here we only
    // null them out -- the relcache leaks index arrays for a relation's lifetime
    // (PG keeps them in rd_indexcxt, freed on relcache flush, which M2 omits).
    rel.rd_support = core::ptr::null_mut::<RegProcedure>();
    rel.rd_supportinfo = core::ptr::null_mut::<FmgrInfo>();
    rel.rd_opfamily = core::ptr::null_mut();
    rel.rd_opcintype = core::ptr::null_mut();
    rel.rd_indcollation = core::ptr::null_mut();
    rel.rd_indoption = core::ptr::null_mut();
}

/// Leak a `Vec<T>` as a raw element pointer (the relcache owns index arrays for
/// the entry's lifetime; M2 does not flush the relcache, so this is a deliberate
/// long-lived allocation mirroring PG's `rd_indexcxt`).
fn leak_slice<T>(v: Vec<T>) -> *mut T {
    let boxed = v.into_boxed_slice();
    Box::into_raw(boxed).cast::<T>()
}

/// `IndexSupportInitialize` (M2 direct form): fill an index relation's support
/// arrays for each key column from the column's btree opclass. `opclasses[i]` is
/// the opclass OID for key column `i+1`; `collations[i]` its collation;
/// `indoption[i]` its DESC/NULLS_FIRST flags. The arrays must already be
/// allocated (via [`relation_init_index_access_info`]).
///
/// The comparator (`BTORDER_PROC`) OID is looked up from the opclass via
/// [`btree_opclass_cmp_proc`]; the other support slots stay `InvalidOid` (optional
/// for M2). This is the data `IndexSupportInitialize` reads from pg_amproc; the
/// seed mapping is encoded in [`btree_opclass_cmp_proc`].
pub fn index_init_opclass_support(
    relation: Relation,
    opclasses: &[Oid],
    collations: &[Oid],
    indoption: &[i16],
) {
    // SAFETY: live index relation with arrays allocated.
    let rel = unsafe { &mut *relation };
    debug_assert!(!rel.rd_support.is_null(), "call relation_init_index_access_info first");
    let indnkeyatts = opclasses.len();
    #[allow(
        clippy::needless_range_loop,
        reason = "index drives writes into several parallel raw relcache arrays by column number"
    )]
    for i in 0..indnkeyatts {
        let opclass = opclasses[i];
        // SAFETY: arrays sized >= indnkeyatts by alloc_index_arrays.
        unsafe {
            *rel.rd_opcintype.add(i) = btree_opclass_intype(opclass);
            *rel.rd_indcollation.add(i) = collations.get(i).copied().unwrap_or(crate::postgres_ext::InvalidOid);
            *rel.rd_indoption.add(i) = indoption.get(i).copied().unwrap_or(0);
            // rd_support layout: column-major, BT_AMSUPPORT slots per column.
            let base = i * BT_AMSUPPORT;
            *rel.rd_support.add(base + (crate::access::nbtree::BTORDER_PROC as usize - 1)) =
                btree_opclass_cmp_proc(opclass);
        }
    }
    rel.rd_indexvalid = true;
}

/// The `BTORDER_PROC` (comparator) function OID for a builtin btree opclass OID.
/// This is the M2 stand-in for the pg_amproc syscache lookup in
/// `IndexSupportInitialize`/`LookupOpclassInfo`: the same `(opclass -> cmp proc)`
/// mapping the seed data encodes, resolved statically for the builtin opclasses
/// the M2 catalogs use.
#[must_use]
pub fn btree_opclass_cmp_proc(opclass: Oid) -> Oid {
    use crate::utils::fmgroids as f;
    match opclass.0 {
        1978 => f::F_BTINT4CMP, // INT4_BTREE_OPS_OID
        1979 => f::F_BTINT2CMP, // INT2_BTREE_OPS_OID
        3124 => f::F_BTINT8CMP, // INT8_BTREE_OPS_OID
        1981 => f::F_BTOIDCMP,  // OID_BTREE_OPS_OID
        3126 => f::F_BTTEXTCMP, // TEXT_BTREE_OPS_OID
        _ => crate::postgres_ext::InvalidOid,
    }
}

/// The opclass input type for a builtin btree opclass OID (the `opcintype` the
/// syscache would return). M2 builtin set.
#[must_use]
fn btree_opclass_intype(opclass: Oid) -> Oid {
    match opclass.0 {
        1978 => Oid(23),  // int4
        1979 => Oid(21),  // int2
        3124 => Oid(20),  // int8
        1981 => Oid(26),  // oid
        3126 => Oid(25),  // text
        _ => crate::postgres_ext::InvalidOid,
    }
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

const PG_CATALOG_NAMESPACE: Oid = Oid(11);
const HEAP_TABLE_AM_OID: Oid = Oid(2); // pg_am: heap

fn namestrcpy(name: &mut crate::c::NameData, src: &str) {
    let bytes = src.as_bytes();
    let n = bytes.len().min(crate::c::NAMEDATALEN - 1);
    name.data = [0u8; crate::c::NAMEDATALEN];
    name.data[..n].copy_from_slice(&bytes[..n]);
}

/// A zeroed pg_class form for the nailed-catalog fake rd_rel.
fn blank_pg_class() -> FormData_pg_class {
    // SAFETY: FormData_pg_class is a #[repr(C)] POD of Oid/int/bool/Name fields;
    // an all-zero bit pattern is a valid (if empty) instance, which we then fill.
    unsafe { core::mem::zeroed() }
}

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
