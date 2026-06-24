//! Translated from PostgreSQL src/include/utils/catcache.h
//! Low-level catalog cache definitions. In-memory caches (not on-disk).

use crate::access::htup::{HeapTuple, HeapTupleData};
use crate::access::skey::ScanKeyData;
use crate::access::tupdesc::TupleDesc;
use crate::lib::ilist::{dlist_head, dlist_node, slist_node};
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::utils::relcache::Relation;

pub const CATCACHE_MAXKEYS: usize = 4;

/// function computing a datum's hash
pub type CCHashFN = fn(datum: Datum) -> u32;
/// function computing equality of two datums
pub type CCFastEqualFN = fn(a: Datum, b: Datum) -> bool;

/// Information for managing a single catalog cache.
/// (CATCACHE_STATS counter fields are omitted; only present in stats builds.)
pub struct CatCache {
    /// cache identifier -- see syscache.h
    pub id: i32,
    /// # of hash buckets in this cache
    pub cc_nbuckets: i32,
    /// tuple descriptor (copied from reldesc)
    pub cc_tupdesc: TupleDesc,
    /// hash buckets. dlist_head array; intrusive dlist links per CatCTup.
    pub cc_bucket: Vec<dlist_head>,
    /// hash function for each key
    pub cc_hashfunc: [Option<CCHashFN>; CATCACHE_MAXKEYS],
    /// fast equal function for each key
    pub cc_fastequal: [Option<CCFastEqualFN>; CATCACHE_MAXKEYS],
    /// AttrNumber of each key
    pub cc_keyno: [i32; CATCACHE_MAXKEYS],
    /// # of keys (1..CATCACHE_MAXKEYS)
    pub cc_nkeys: i32,
    /// # of tuples currently in this cache
    pub cc_ntup: i32,
    /// # of CatCLists currently in this cache
    pub cc_nlist: i32,
    /// # of CatCList hash buckets in this cache
    pub cc_nlbuckets: i32,
    /// hash buckets for CatCLists
    pub cc_lbucket: Vec<dlist_head>,
    /// name of relation the tuples come from
    pub cc_relname: String,
    /// OID of relation the tuples come from
    pub cc_reloid: Oid,
    /// OID of index matching cache keys
    pub cc_indexoid: Oid,
    /// is relation shared across databases?
    pub cc_relisshared: bool,
    /// list link (slist member of CatCacheHeader's caches list)
    pub cc_next: slist_node,
    /// precomputed key info for heap scans
    pub cc_skey: [ScanKeyData; CATCACHE_MAXKEYS],
}

/// Magic for identifying CatCTup entries.
pub const CT_MAGIC: i32 = 0x57261502u32 as i32;

/// Individual tuple in the cache.
pub struct CatCTup {
    /// for identifying CatCTup entries (== CT_MAGIC)
    pub ct_magic: i32,
    /// hash value for this tuple's keys
    pub hash_value: u32,
    /// Lookup keys. By-ref datums point into `tuple` for positive entries, and
    /// are separately allocated for negative ones.
    pub keys: [Datum; CATCACHE_MAXKEYS],
    /// per-bucket dlist member (kept LRU-ordered). Intrusive link.
    pub cache_elem: dlist_node,
    /// number of active references
    pub refcount: i32,
    /// dead but not yet removed?
    pub dead: bool,
    /// negative cache entry?
    pub negative: bool,
    /// tuple management header
    pub tuple: HeapTupleData,
    /// containing CatCList, or None. TODO(ptr): intrusive back-link.
    pub c_list: Option<*mut CatCList>,
    /// link to owning catcache. TODO(ptr).
    pub my_cache: *mut CatCache,
    // properly aligned tuple data follows, unless a negative entry
}

/// Magic for identifying CatCList entries.
pub const CL_MAGIC: i32 = 0x52765103;

/// Result of a partial search (first K of N keys). This is the real body of the
/// `crate::utils::syscache::CatCList` forward-decl. The C struct ends with a FAM
/// `CatCTup *members[]`; modelled as a Vec.
pub struct CatCList {
    /// for identifying CatCList entries (== CL_MAGIC)
    pub cl_magic: i32,
    /// hash value for lookup keys
    pub hash_value: u32,
    /// per-catcache dlist member. Intrusive link.
    pub cache_elem: dlist_node,
    /// lookup keys, first `nkeys` valid; all by-ref separately allocated.
    pub keys: [Datum; CATCACHE_MAXKEYS],
    /// number of active references
    pub refcount: i32,
    /// dead but not yet removed?
    pub dead: bool,
    /// members listed in index order?
    pub ordered: bool,
    /// number of lookup keys specified
    pub nkeys: i16,
    /// number of member tuples
    pub n_members: i32,
    /// link to owning catcache. TODO(ptr).
    pub my_cache: *mut CatCache,
    /// member tuples (C FAM). TODO(ptr): intrusive membership.
    pub members: Vec<*mut CatCTup>,
}

/// Information for managing all the caches.
pub struct CatCacheHeader {
    /// head of list of CatCache structs (intrusive slist). TODO(ptr).
    pub ch_caches: Vec<*mut CatCache>,
    /// # of tuples in all caches
    pub ch_ntup: i32,
}

pub fn CreateCacheMemoryContext() {
    unimplemented!()
}

pub fn InitCatCache(
    _id: i32,
    _reloid: Oid,
    _indexoid: Oid,
    _nkeys: i32,
    _key: &[i32],
    _nbuckets: i32,
) -> *mut CatCache {
    unimplemented!() // TODO(ptr)
}
pub fn InitCatCachePhase2(_cache: &mut CatCache, _touch_index: bool) {
    unimplemented!()
}

// Search functions: invalid HeapTuple -> None (miss).
pub fn SearchCatCache(
    _cache: &mut CatCache,
    _v1: Datum,
    _v2: Datum,
    _v3: Datum,
    _v4: Datum,
) -> Option<HeapTuple> {
    unimplemented!()
}
pub fn SearchCatCache1(_cache: &mut CatCache, _v1: Datum) -> Option<HeapTuple> {
    unimplemented!()
}
pub fn SearchCatCache2(_cache: &mut CatCache, _v1: Datum, _v2: Datum) -> Option<HeapTuple> {
    unimplemented!()
}
pub fn SearchCatCache3(
    _cache: &mut CatCache,
    _v1: Datum,
    _v2: Datum,
    _v3: Datum,
) -> Option<HeapTuple> {
    unimplemented!()
}
pub fn SearchCatCache4(
    _cache: &mut CatCache,
    _v1: Datum,
    _v2: Datum,
    _v3: Datum,
    _v4: Datum,
) -> Option<HeapTuple> {
    unimplemented!()
}
pub fn ReleaseCatCache(_tuple: HeapTuple) {
    unimplemented!()
}

pub fn GetCatCacheHashValue(
    _cache: &mut CatCache,
    _v1: Datum,
    _v2: Datum,
    _v3: Datum,
    _v4: Datum,
) -> u32 {
    unimplemented!()
}

pub fn SearchCatCacheList(
    _cache: &mut CatCache,
    _nkeys: i32,
    _v1: Datum,
    _v2: Datum,
    _v3: Datum,
) -> *mut CatCList {
    unimplemented!() // TODO(ptr)
}
pub fn ReleaseCatCacheList(_list: &mut CatCList) {
    unimplemented!()
}

pub fn ResetCatalogCaches() {
    unimplemented!()
}
pub fn ResetCatalogCachesExt(_debug_discard: bool) {
    unimplemented!()
}
pub fn CatalogCacheFlushCatalog(_cat_id: Oid) {
    unimplemented!()
}
pub fn CatCacheInvalidate(_cache: &mut CatCache, _hash_value: u32) {
    unimplemented!()
}

/// `function(cacheid, hashvalue, dboid, context)` callback; `void *context`
/// becomes a captured closure.
pub fn PrepareToInvalidateCacheTuple(
    _relation: Relation,
    _tuple: HeapTuple,
    _newtuple: HeapTuple,
    _function: &mut dyn FnMut(i32, u32, Oid),
) {
    unimplemented!()
}
