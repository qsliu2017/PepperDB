//! src/backend/utils/cache/catcache.c
//!
//! System catalog cache for tuples matching a key.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! Companion header src/include/utils/catcache.h is merged below.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]

use crate::prelude::*;

use core::ffi::CStr;

use crate::access::common::scankey::ScanKeyData;
use crate::access::htup_details::{HeapTuple, HeapTupleData, HeapTupleHeader};
use crate::access::stratnum::BTEqualStrategyNumber;
use crate::common::hashfn::{hash_any, murmurhash32};
use crate::lib::ilist::{
    dlist_delete, dlist_head, dlist_iter, dlist_move_head, dlist_mutable_iter, dlist_node,
    dlist_push_head, slist_head, slist_init, slist_iter, slist_node, slist_push_head,
};
use crate::miscadmin::MyDatabaseId;
use crate::nodes::pg_list::{lappend, list_length, List, ListCell, NIL};
use crate::port::pg_bitutils::pg_rotate_left32;
use crate::{
    current_cell, dlist_container, dlist_foreach, dlist_foreach_modify, foreach, slist_container,
    slist_foreach,
};

// ---- stub aliases for not-yet-ported external types ----

type Relation = *mut c_void; // TODO(pg-port): utils/rel.h
type TupleDesc = *mut c_void; // TODO(pg-port): access/tupdesc.h
type SysScanDesc = *mut SysScanDescData; // TODO(pg-port): access/relscan.h
type ResourceOwner = *mut c_void; // TODO(pg-port): utils/resowner.h
type Form_pg_attribute = *mut FormData_pg_attribute; // TODO(pg-port): catalog/pg_attribute.h
type StrategyNumber = u16;
type AttrNumber = i16;

#[repr(C)]
pub struct SysScanDescData {
    pub irel: Relation, // index relation, or NULL for heap scan
}

#[repr(C)]
pub struct FormData_pg_attribute {
    pub atttypid: Oid,
    pub attlen: i16,
    pub attnotnull: bool,
    pub attbyval: bool,
}

#[repr(C)]
pub struct NameData {
    pub data: [c_char; NAMEDATALEN as usize],
}

// ---------------------------------------------------------------------------
//
// catcache.h declarations
//
// src/include/utils/catcache.h
//
// ---------------------------------------------------------------------------

pub const CATCACHE_MAXKEYS: usize = 4;

// function computing a datum's hash
pub type CCHashFN = unsafe fn(datum: Datum) -> uint32;

// function computing equality of two datums
pub type CCFastEqualFN = unsafe fn(a: Datum, b: Datum) -> bool;

#[repr(C)]
pub struct catcache {
    pub id: c_int,         // cache identifier --- see syscache.h
    pub cc_nbuckets: c_int, // # of hash buckets in this cache
    pub cc_tupdesc: TupleDesc, // tuple descriptor (copied from reldesc)
    pub cc_bucket: *mut dlist_head, // hash buckets
    pub cc_hashfunc: [Option<CCHashFN>; CATCACHE_MAXKEYS], // hash function for each key
    pub cc_fastequal: [Option<CCFastEqualFN>; CATCACHE_MAXKEYS], // fast equal function for each key
    pub cc_keyno: [c_int; CATCACHE_MAXKEYS], // AttrNumber of each key
    pub cc_nkeys: c_int,   // # of keys (1..CATCACHE_MAXKEYS)
    pub cc_ntup: c_int,    // # of tuples currently in this cache
    pub cc_nlist: c_int,   // # of CatCLists currently in this cache
    pub cc_nlbuckets: c_int, // # of CatCList hash buckets in this cache
    pub cc_lbucket: *mut dlist_head, // hash buckets for CatCLists
    pub cc_relname: *const c_char, // name of relation the tuples come from
    pub cc_reloid: Oid,    // OID of relation the tuples come from
    pub cc_indexoid: Oid,  // OID of index matching cache keys
    pub cc_relisshared: bool, // is relation shared across databases?
    pub cc_next: slist_node, // list link
    pub cc_skey: [ScanKeyData; CATCACHE_MAXKEYS], // precomputed key info for heap scans

    // Statistics fields, kept at end (CATCACHE_STATS).
    pub cc_searches: c_long, // total # searches against this cache
    pub cc_hits: c_long,     // # of matches against existing entry
    pub cc_neg_hits: c_long, // # of matches against negative entry
    pub cc_newloads: c_long, // # of successful loads of new entry
    pub cc_invals: c_long,   // # of entries invalidated from cache
    pub cc_lsearches: c_long, // total # list-searches
    pub cc_lhits: c_long,    // # of matches against existing lists
}

pub type CatCache = catcache;

pub const CT_MAGIC: c_int = 0x57261502u32 as c_int;

#[repr(C)]
pub struct catctup {
    pub ct_magic: c_int, // for identifying CatCTup entries

    pub hash_value: uint32, // hash value for this tuple's keys

    // Lookup keys for the entry. By-reference datums point into the tuple for
    // positive cache entries, and are separately allocated for negative ones.
    pub keys: [Datum; CATCACHE_MAXKEYS],

    // Each tuple in a cache is a member of a dlist that stores the elements
    // of its hash bucket.  We keep each dlist in LRU order to speed repeated
    // lookups.
    pub cache_elem: dlist_node, // list member of per-bucket list

    pub refcount: c_int, // number of active references
    pub dead: bool,      // dead but not yet removed?
    pub negative: bool,  // negative cache entry?
    pub tuple: HeapTupleData, // tuple management header

    // The tuple may also be a member of at most one CatCList.
    pub c_list: *mut catclist, // containing CatCList, or NULL if none

    pub my_cache: *mut CatCache, // link to owning catcache
    // properly aligned tuple data follows, unless a negative entry
}

pub type CatCTup = catctup;

pub const CL_MAGIC: c_int = 0x52765103u32 as c_int;

#[repr(C)]
pub struct catclist {
    pub cl_magic: c_int, // for identifying CatCList entries

    pub hash_value: uint32, // hash value for lookup keys

    pub cache_elem: dlist_node, // list member of per-catcache list

    // Lookup keys for the entry, with the first nkeys elements being valid.
    // All by-reference are separately allocated.
    pub keys: [Datum; CATCACHE_MAXKEYS],

    pub refcount: c_int, // number of active references
    pub dead: bool,      // dead but not yet removed?
    pub ordered: bool,   // members listed in index order?
    pub nkeys: c_short,  // number of lookup keys specified
    pub n_members: c_int, // number of member tuples
    pub my_cache: *mut CatCache, // link to owning catcache
    pub members: [*mut CatCTup; 0], // members (FLEXIBLE_ARRAY_MEMBER)
}

pub type CatCList = catclist;

#[repr(C)]
pub struct catcacheheader {
    pub ch_caches: slist_head, // head of list of CatCache structs
    pub ch_ntup: c_int,        // # of tuples in all caches
}

pub type CatCacheHeader = catcacheheader;

type c_short = i16;

// ---- TODO(pg-port) dependency stubs (functions in other .c files) ----

unsafe fn table_open(_relationId: Oid, _lockmode: c_int) -> Relation {
    // TODO(pg-port): access/table/table.c
    null_mut()
}
unsafe fn table_close(_relation: Relation, _lockmode: c_int) {
    // TODO(pg-port): access/table/table.c
}
unsafe fn index_open(_relationId: Oid, _lockmode: c_int) -> Relation {
    // TODO(pg-port): access/index/indexam.c
    null_mut()
}
unsafe fn index_close(_relation: Relation, _lockmode: c_int) {
    // TODO(pg-port): access/index/indexam.c
}
unsafe fn LockRelationOid(_relid: Oid, _lockmode: c_int) {
    // TODO(pg-port): storage/lmgr/lmgr.c
}
unsafe fn UnlockRelationOid(_relid: Oid, _lockmode: c_int) {
    // TODO(pg-port): storage/lmgr/lmgr.c
}
unsafe fn systable_beginscan(
    _heapRelation: Relation,
    _indexId: Oid,
    _indexOK: bool,
    _snapshot: *mut c_void,
    _nkeys: c_int,
    _key: *mut ScanKeyData,
) -> SysScanDesc {
    // TODO(pg-port): access/index/genam.c
    null_mut()
}
unsafe fn systable_getnext(_sysscan: SysScanDesc) -> HeapTuple {
    // TODO(pg-port): access/index/genam.c
    null_mut()
}
unsafe fn systable_endscan(_sysscan: SysScanDesc) {
    // TODO(pg-port): access/index/genam.c
}
unsafe fn CreateTupleDescCopyConstr(_tupdesc: TupleDesc) -> TupleDesc {
    // TODO(pg-port): access/common/tupdesc.c
    null_mut()
}
unsafe fn TupleDescAttr(_tupdesc: TupleDesc, _i: c_int) -> Form_pg_attribute {
    // TODO(pg-port): access/tupdesc.h
    null_mut()
}
unsafe fn RelationGetDescr(_relation: Relation) -> TupleDesc {
    // TODO(pg-port): utils/rel.h
    null_mut()
}
unsafe fn RelationGetRelationName(_relation: Relation) -> *const c_char {
    // TODO(pg-port): utils/rel.h
    c"(unknown)".as_ptr()
}
unsafe fn RelationGetForm_relisshared(_relation: Relation) -> bool {
    // TODO(pg-port): utils/rel.h (RelationGetForm(rel)->relisshared)
    false
}
unsafe fn RelationGetRelid(_relation: Relation) -> Oid {
    // TODO(pg-port): utils/rel.h
    InvalidOid
}
unsafe fn rd_index_indisunique(_relation: Relation) -> bool {
    // TODO(pg-port): utils/rel.h (rel->rd_index->indisunique)
    true
}
unsafe fn rd_index_indimmediate(_relation: Relation) -> bool {
    // TODO(pg-port): utils/rel.h (rel->rd_index->indimmediate)
    true
}
unsafe fn fastgetattr(
    _tup: HeapTuple,
    _attnum: c_int,
    _tupdesc: TupleDesc,
    _isnull: *mut bool,
) -> Datum {
    // TODO(pg-port): access/htup_details.h
    0
}
unsafe fn heap_getattr(
    _tup: HeapTuple,
    _attnum: c_int,
    _tupdesc: TupleDesc,
    _isnull: *mut bool,
) -> Datum {
    // TODO(pg-port): access/htup_details.h
    0
}
unsafe fn heap_freetuple(_htup: HeapTuple) {
    // TODO(pg-port): access/common/heaptuple.c
}
unsafe fn toast_flatten_tuple(_tup: HeapTuple, _tupdesc: TupleDesc) -> HeapTuple {
    // TODO(pg-port): access/heap/heaptoast.c
    null_mut()
}
unsafe fn HeapTupleHasExternal(_tuple: HeapTuple) -> bool {
    // TODO(pg-port): access/htup_details.h
    false
}
unsafe fn HeapTupleIsValid(tuple: HeapTuple) -> bool {
    !tuple.is_null()
}
unsafe fn RelationIsValid(relation: Relation) -> bool {
    !relation.is_null()
}
unsafe fn PointerIsValid(p: *const c_void) -> bool {
    !p.is_null()
}
unsafe fn ItemPointerEquals(_a: *const c_void, _b: *const c_void) -> bool {
    // TODO(pg-port): storage/itemptr.c
    false
}
unsafe fn ItemPointerGetBlockNumber(_pointer: *const c_void) -> u32 {
    // TODO(pg-port): storage/itemptr.h
    0
}
unsafe fn ItemPointerGetOffsetNumber(_pointer: *const c_void) -> u16 {
    // TODO(pg-port): storage/itemptr.h
    0
}
unsafe fn AttributeNumberIsValid(attno: c_int) -> bool {
    attno != 0
}
unsafe fn datumCopy(value: Datum, _typByVal: bool, _typLen: c_int) -> Datum {
    // TODO(pg-port): utils/adt/datum.c
    value
}
unsafe fn namestrcpy(_name: *mut NameData, _str: *const c_char) {
    // TODO(pg-port): utils/adt/name.c
}
unsafe fn pg_prng_uint32(_state: *mut pg_prng_state) -> uint32 {
    // TODO(pg-port): common/pg_prng.c
    0
}
unsafe fn IsTransactionState() -> bool {
    // TODO(pg-port): access/transam/xact.c
    true
}
unsafe fn IsBootstrapProcessingMode() -> bool {
    // TODO(pg-port): utils/init/miscinit.c
    false
}
unsafe fn CallSyscacheCallbacks(_cacheid: c_int, _hashvalue: uint32) {
    // TODO(pg-port): utils/cache/inval.c
}
unsafe fn ResourceOwnerEnlarge(_owner: ResourceOwner) {
    // TODO(pg-port): utils/resowner/resowner.c
}
unsafe fn ResourceOwnerRemember(_owner: ResourceOwner, _value: Datum, _kind: *const ResourceOwnerDesc) {
    // TODO(pg-port): utils/resowner/resowner.c
}
unsafe fn ResourceOwnerForget(_owner: ResourceOwner, _value: Datum, _kind: *const ResourceOwnerDesc) {
    // TODO(pg-port): utils/resowner/resowner.c
}
unsafe fn palloc_aligned(size: usize, _alignto: usize, _flags: c_int) -> *mut c_void {
    // TODO(pg-port): utils/mmgr/mcxt.c
    palloc(size)
}
unsafe fn psprintf_2(_fmt: *const c_char) -> *mut c_char {
    // TODO(pg-port): lib/psprintf.c -- callers build the string inline
    null_mut()
}

// fmgr support stubs
unsafe fn fmgr_info_cxt(_functionId: Oid, _finfo: *mut c_void, _mcxt: MemoryContext) {
    // TODO(pg-port): utils/fmgr/fmgr.c
}
unsafe fn DirectFunctionCall1Coll(_func: c_int, _collation: Oid, _arg1: Datum) -> Datum {
    // TODO(pg-port): utils/fmgr/fmgr.c
    0
}
unsafe fn DirectFunctionCall2Coll(_func: c_int, _collation: Oid, _arg1: Datum, _arg2: Datum) -> Datum {
    // TODO(pg-port): utils/fmgr/fmgr.c
    0
}
unsafe fn DirectFunctionCall1(_func: c_int, _arg1: Datum) -> Datum {
    // TODO(pg-port): utils/fmgr/fmgr.c
    0
}
unsafe fn DirectFunctionCall2(_func: c_int, _arg1: Datum, _arg2: Datum) -> Datum {
    // TODO(pg-port): utils/fmgr/fmgr.c
    0
}

// Datum getters / makers (utils stubs)
unsafe fn DatumGetChar(d: Datum) -> c_char {
    d as c_char
}
unsafe fn DatumGetInt16(d: Datum) -> i16 {
    d as i16
}
unsafe fn DatumGetInt32(d: Datum) -> i32 {
    d as i32
}
unsafe fn DatumGetBool(d: Datum) -> bool {
    (d & 1) != 0
}
unsafe fn DatumGetName(d: Datum) -> *mut NameData {
    d as *mut NameData
}
unsafe fn DatumGetCString(d: Datum) -> *const c_char {
    d as *const c_char
}
unsafe fn DatumGetPointer(d: Datum) -> *mut c_void {
    d as *mut c_void
}
unsafe fn PointerGetDatum(p: *const c_void) -> Datum {
    p as Datum
}
unsafe fn NameGetDatum(name: *const NameData) -> Datum {
    name as Datum
}
unsafe fn NameStr(name: *mut NameData) -> *mut c_char {
    (*name).data.as_mut_ptr()
}

// fmgroids stubs (utils/fmgroids.h)
const F_BOOLEQ: Oid = 0;
const F_CHAREQ: Oid = 0;
const F_NAMEEQ: Oid = 0;
const F_INT2EQ: Oid = 0;
const F_INT4EQ: Oid = 0;
const F_TEXTEQ: Oid = 0;
const F_OIDEQ: Oid = 0;
const F_OIDVECTOREQ: Oid = 0;

// pg_type / pg_collation OIDs (catalog stubs)
const BOOLOID: Oid = 16;
const CHAROID: Oid = 18;
const NAMEOID: Oid = 19;
const INT2OID: Oid = 21;
const INT4OID: Oid = 23;
const TEXTOID: Oid = 25;
const OIDOID: Oid = 26;
const OIDVECTOROID: Oid = 30;
const REGPROCOID: Oid = 24;
const REGPROCEDUREOID: Oid = 2202;
const REGOPEROID: Oid = 2203;
const REGOPERATOROID: Oid = 2204;
const REGCLASSOID: Oid = 2205;
const REGTYPEOID: Oid = 2206;
const REGCOLLATIONOID: Oid = 4191;
const REGCONFIGOID: Oid = 3734;
const REGDICTIONARYOID: Oid = 3769;
const REGROLEOID: Oid = 4096;
const REGNAMESPACEOID: Oid = 4089;
const DEFAULT_COLLATION_OID: Oid = 100;
const C_COLLATION_OID: Oid = 950;

// SQL-callable function name placeholders for DirectFunctionCallN
const texteq: c_int = 0;
const hashtext: c_int = 0;
const oidvectoreq: c_int = 0;
const hashoidvector: c_int = 0;

// syscache.h cache IDs referenced here (genbki stubs)
const TYPEOID: c_int = 0;
const ATTNUM: c_int = 0;
const INDEXRELID: c_int = 0;
const AMOID: c_int = 0;
const AMNAME: c_int = 0;
const AUTHNAME: c_int = 0;
const AUTHOID: c_int = 0;
const AUTHMEMMEMROLE: c_int = 0;
const DATABASEOID: c_int = 0;

// misc globals (TODO(pg-port): backend globals)
static mut criticalRelcachesBuilt: bool = false;
static mut criticalSharedRelcachesBuilt: bool = false;
static mut CacheMemoryContext: MemoryContext = null_mut();

// TODO(pg-port): common/pg_prng.c -- pg_prng_state
#[repr(C)]
pub struct pg_prng_state {
    pub s0: u64,
    pub s1: u64,
}
static mut pg_global_prng_state: pg_prng_state = pg_prng_state { s0: 0, s1: 0 };
const PG_UINT32_MAX: uint32 = u32::MAX;
const PG_CACHE_LINE_SIZE: usize = 128;
const MAXIMUM_ALIGNOF: usize = 8;
const MCXT_ALLOC_ZERO: c_int = 0x02;
const NAMEDATALEN: c_int = 64;

unsafe fn MAXALIGN(len: usize) -> usize {
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

// ResourceOwner descriptor (utils/resowner.h) stubs
#[repr(C)]
pub struct ResourceOwnerDesc {
    pub name: *const c_char,
    pub release_phase: c_int,
    pub release_priority: u32,
    pub ReleaseResource: Option<unsafe fn(res: Datum)>,
    pub DebugPrint: Option<unsafe fn(res: Datum) -> *mut c_char>,
}
unsafe impl Sync for ResourceOwnerDesc {}

const RESOURCE_RELEASE_AFTER_LOCKS: c_int = 2;
const RELEASE_PRIO_CATCACHE_REFS: u32 = 100;
const RELEASE_PRIO_CATCACHE_LIST_REFS: u32 = 200;

// If a catcache invalidation is processed while we are in the middle of
// creating a catcache entry (or list), it might apply to the entry we're
// creating, making it invalid before it's been inserted to the catcache.  To
// catch such cases, we have a stack of "create-in-progress" entries.  Cache
// invalidation marks any matching entries in the stack as dead, in addition
// to the actual CatCTup and CatCList entries.
#[repr(C)]
struct CatCInProgress {
    cache: *mut CatCache,    // cache that the entry belongs to
    hash_value: uint32,      // hash of the entry; ignored for lists
    list: bool,              // is it a list entry?
    dead: bool,              // set when the entry is invalidated
    next: *mut CatCInProgress,
}

static mut catcache_in_progress_stack: *mut CatCInProgress = null_mut();

// Given a hash value and the size of the hash table, find the bucket
// in which the hash value belongs. Since the hash table must contain
// a power-of-2 number of elements, this is a simple bitmask.
#[inline]
fn HASH_INDEX(h: uint32, sz: c_int) -> Index {
    (h & ((sz as uint32).wrapping_sub(1))) as Index
}

// Cache management header --- pointer is NULL until created
static mut CacheHdr: *mut CatCacheHeader = null_mut();

// ResourceOwner callbacks to hold catcache references
static catcache_resowner_desc: ResourceOwnerDesc = ResourceOwnerDesc {
    // catcache references
    name: c"catcache reference".as_ptr(),
    release_phase: RESOURCE_RELEASE_AFTER_LOCKS,
    release_priority: RELEASE_PRIO_CATCACHE_REFS,
    ReleaseResource: Some(ResOwnerReleaseCatCache),
    DebugPrint: Some(ResOwnerPrintCatCache),
};

static catlistref_resowner_desc: ResourceOwnerDesc = ResourceOwnerDesc {
    // catcache-list pins
    name: c"catcache list reference".as_ptr(),
    release_phase: RESOURCE_RELEASE_AFTER_LOCKS,
    release_priority: RELEASE_PRIO_CATCACHE_LIST_REFS,
    ReleaseResource: Some(ResOwnerReleaseCatCacheList),
    DebugPrint: Some(ResOwnerPrintCatCacheList),
};

// Convenience wrappers over ResourceOwnerRemember/Forget
#[inline]
unsafe fn ResourceOwnerRememberCatCacheRef(owner: ResourceOwner, tuple: *mut HeapTupleData) {
    ResourceOwnerRemember(owner, PointerGetDatum(tuple as *const c_void), &catcache_resowner_desc);
}
#[inline]
unsafe fn ResourceOwnerForgetCatCacheRef(owner: ResourceOwner, tuple: *mut HeapTupleData) {
    ResourceOwnerForget(owner, PointerGetDatum(tuple as *const c_void), &catcache_resowner_desc);
}
#[inline]
unsafe fn ResourceOwnerRememberCatCacheListRef(owner: ResourceOwner, list: *mut CatCList) {
    ResourceOwnerRemember(owner, PointerGetDatum(list as *const c_void), &catlistref_resowner_desc);
}
#[inline]
unsafe fn ResourceOwnerForgetCatCacheListRef(owner: ResourceOwner, list: *mut CatCList) {
    ResourceOwnerForget(owner, PointerGetDatum(list as *const c_void), &catlistref_resowner_desc);
}

// CurrentResourceOwner (TODO(pg-port): utils/resowner/resowner.c)
static mut CurrentResourceOwner: ResourceOwner = null_mut();

// Hash and equality functions for system types that are used as cache key
// fields.  In some cases, we just call the regular SQL-callable functions for
// the appropriate data type, but that tends to be a little slow, and the
// speed of these functions is performance-critical.  Therefore, for data
// types that frequently occur as catcache keys, we hard-code the logic here.

unsafe fn chareqfast(a: Datum, b: Datum) -> bool {
    DatumGetChar(a) == DatumGetChar(b)
}

unsafe fn charhashfast(datum: Datum) -> uint32 {
    murmurhash32(DatumGetChar(datum) as i32 as uint32)
}

unsafe fn nameeqfast(a: Datum, b: Datum) -> bool {
    let ca = NameStr(DatumGetName(a));
    let cb = NameStr(DatumGetName(b));

    strncmp(ca, cb, NAMEDATALEN as usize) == 0
}

unsafe fn namehashfast(datum: Datum) -> uint32 {
    let key = NameStr(DatumGetName(datum));

    hash_any(key as *const c_uchar, strlen(key) as c_int) as uint32
}

unsafe fn int2eqfast(a: Datum, b: Datum) -> bool {
    DatumGetInt16(a) == DatumGetInt16(b)
}

unsafe fn int2hashfast(datum: Datum) -> uint32 {
    murmurhash32(DatumGetInt16(datum) as i32 as uint32)
}

unsafe fn int4eqfast(a: Datum, b: Datum) -> bool {
    DatumGetInt32(a) == DatumGetInt32(b)
}

unsafe fn int4hashfast(datum: Datum) -> uint32 {
    murmurhash32(DatumGetInt32(datum) as uint32)
}

unsafe fn texteqfast(a: Datum, b: Datum) -> bool {
    // The use of DEFAULT_COLLATION_OID is fairly arbitrary here.  We just
    // want to take the fast "deterministic" path in texteq().
    DatumGetBool(DirectFunctionCall2Coll(texteq, DEFAULT_COLLATION_OID, a, b))
}

unsafe fn texthashfast(datum: Datum) -> uint32 {
    // analogously here as in texteqfast()
    DirectFunctionCall1Coll(hashtext, DEFAULT_COLLATION_OID, datum) as i32 as uint32
}

unsafe fn oidvectoreqfast(a: Datum, b: Datum) -> bool {
    DatumGetBool(DirectFunctionCall2(oidvectoreq, a, b))
}

unsafe fn oidvectorhashfast(datum: Datum) -> uint32 {
    DirectFunctionCall1(hashoidvector, datum) as i32 as uint32
}

// helper stubs for libc string fns
unsafe fn strncmp(a: *const c_char, b: *const c_char, n: usize) -> c_int {
    let sa = core::slice::from_raw_parts(a as *const u8, n);
    let sb = core::slice::from_raw_parts(b as *const u8, n);
    for i in 0..n {
        let (ca, cb) = (sa[i], sb[i]);
        if ca != cb {
            return ca as c_int - cb as c_int;
        }
        if ca == 0 {
            break;
        }
    }
    0
}
unsafe fn strlen(s: *const c_char) -> usize {
    CStr::from_ptr(s).to_bytes().len()
}

// Lookup support functions for a type.
unsafe fn GetCCHashEqFuncs(
    keytype: Oid,
    hashfunc: *mut Option<CCHashFN>,
    eqfunc: *mut RegProcedure,
    fasteqfunc: *mut Option<CCFastEqualFN>,
) {
    match keytype {
        BOOLOID => {
            *hashfunc = Some(charhashfast);
            *fasteqfunc = Some(chareqfast);
            *eqfunc = F_BOOLEQ;
        }
        CHAROID => {
            *hashfunc = Some(charhashfast);
            *fasteqfunc = Some(chareqfast);
            *eqfunc = F_CHAREQ;
        }
        NAMEOID => {
            *hashfunc = Some(namehashfast);
            *fasteqfunc = Some(nameeqfast);
            *eqfunc = F_NAMEEQ;
        }
        INT2OID => {
            *hashfunc = Some(int2hashfast);
            *fasteqfunc = Some(int2eqfast);
            *eqfunc = F_INT2EQ;
        }
        INT4OID => {
            *hashfunc = Some(int4hashfast);
            *fasteqfunc = Some(int4eqfast);
            *eqfunc = F_INT4EQ;
        }
        TEXTOID => {
            *hashfunc = Some(texthashfast);
            *fasteqfunc = Some(texteqfast);
            *eqfunc = F_TEXTEQ;
        }
        OIDOID | REGPROCOID | REGPROCEDUREOID | REGOPEROID | REGOPERATOROID | REGCLASSOID
        | REGTYPEOID | REGCOLLATIONOID | REGCONFIGOID | REGDICTIONARYOID | REGROLEOID
        | REGNAMESPACEOID => {
            *hashfunc = Some(int4hashfast);
            *fasteqfunc = Some(int4eqfast);
            *eqfunc = F_OIDEQ;
        }
        OIDVECTOROID => {
            *hashfunc = Some(oidvectorhashfast);
            *fasteqfunc = Some(oidvectoreqfast);
            *eqfunc = F_OIDVECTOREQ;
        }
        _ => {
            elog!(FATAL, "type {} not supported as catcache key", keytype);
            *hashfunc = None; // keep compiler quiet

            *eqfunc = InvalidOid;
        }
    }
}

//		CatalogCacheComputeHashValue
//
// Compute the hash value associated with a given set of lookup keys
unsafe fn CatalogCacheComputeHashValue(
    cache: *mut CatCache,
    nkeys: c_int,
    v1: Datum,
    v2: Datum,
    v3: Datum,
    v4: Datum,
) -> uint32 {
    let mut hashValue: uint32 = 0;
    let cc_hashfunc = &(*cache).cc_hashfunc;

    // CACHE_elog(DEBUG2, "CatalogCacheComputeHashValue %s %d %p", ...)

    // The C code falls through each case, accumulating the lower-numbered keys.
    match nkeys {
        4 => {
            hashValue ^= pg_rotate_left32((cc_hashfunc[3].unwrap())(v4), 24);
            hashValue ^= pg_rotate_left32((cc_hashfunc[2].unwrap())(v3), 16);
            hashValue ^= pg_rotate_left32((cc_hashfunc[1].unwrap())(v2), 8);
            hashValue ^= (cc_hashfunc[0].unwrap())(v1);
        }
        3 => {
            hashValue ^= pg_rotate_left32((cc_hashfunc[2].unwrap())(v3), 16);
            hashValue ^= pg_rotate_left32((cc_hashfunc[1].unwrap())(v2), 8);
            hashValue ^= (cc_hashfunc[0].unwrap())(v1);
        }
        2 => {
            hashValue ^= pg_rotate_left32((cc_hashfunc[1].unwrap())(v2), 8);
            hashValue ^= (cc_hashfunc[0].unwrap())(v1);
        }
        1 => {
            hashValue ^= (cc_hashfunc[0].unwrap())(v1);
        }
        _ => {
            elog!(FATAL, "wrong number of hash keys: {}", nkeys);
        }
    }

    hashValue
}

//		CatalogCacheComputeTupleHashValue
//
// Compute the hash value associated with a given tuple to be cached
unsafe fn CatalogCacheComputeTupleHashValue(
    cache: *mut CatCache,
    nkeys: c_int,
    tuple: HeapTuple,
) -> uint32 {
    let mut v1: Datum = 0;
    let mut v2: Datum = 0;
    let mut v3: Datum = 0;
    let mut v4: Datum = 0;
    let mut isNull: bool = false;
    let cc_keyno = &(*cache).cc_keyno;
    let cc_tupdesc = (*cache).cc_tupdesc;

    // Now extract key fields from tuple, insert into scankey
    match nkeys {
        4 => {
            v4 = fastgetattr(tuple, cc_keyno[3], cc_tupdesc, &mut isNull);
            Assert!(!isNull);
            v3 = fastgetattr(tuple, cc_keyno[2], cc_tupdesc, &mut isNull);
            Assert!(!isNull);
            v2 = fastgetattr(tuple, cc_keyno[1], cc_tupdesc, &mut isNull);
            Assert!(!isNull);
            v1 = fastgetattr(tuple, cc_keyno[0], cc_tupdesc, &mut isNull);
            Assert!(!isNull);
        }
        3 => {
            v3 = fastgetattr(tuple, cc_keyno[2], cc_tupdesc, &mut isNull);
            Assert!(!isNull);
            v2 = fastgetattr(tuple, cc_keyno[1], cc_tupdesc, &mut isNull);
            Assert!(!isNull);
            v1 = fastgetattr(tuple, cc_keyno[0], cc_tupdesc, &mut isNull);
            Assert!(!isNull);
        }
        2 => {
            v2 = fastgetattr(tuple, cc_keyno[1], cc_tupdesc, &mut isNull);
            Assert!(!isNull);
            v1 = fastgetattr(tuple, cc_keyno[0], cc_tupdesc, &mut isNull);
            Assert!(!isNull);
        }
        1 => {
            v1 = fastgetattr(tuple, cc_keyno[0], cc_tupdesc, &mut isNull);
            Assert!(!isNull);
        }
        _ => {
            elog!(FATAL, "wrong number of hash keys: {}", nkeys);
        }
    }

    CatalogCacheComputeHashValue(cache, nkeys, v1, v2, v3, v4)
}

//		CatalogCacheCompareTuple
//
// Compare a tuple to the passed arguments.
#[inline]
unsafe fn CatalogCacheCompareTuple(
    cache: *const CatCache,
    nkeys: c_int,
    cachekeys: *const Datum,
    searchkeys: *const Datum,
) -> bool {
    let cc_fastequal = &(*cache).cc_fastequal;

    for i in 0..nkeys as usize {
        if !(cc_fastequal[i].unwrap())(*cachekeys.add(i), *searchkeys.add(i)) {
            return false;
        }
    }
    true
}

//		CatCacheRemoveCTup
//
// Unlink and delete the given cache entry
//
// NB: if it is a member of a CatCList, the CatCList is deleted too.
// Both the cache entry and the list had better have zero refcount.
unsafe fn CatCacheRemoveCTup(cache: *mut CatCache, ct: *mut CatCTup) {
    Assert!((*ct).refcount == 0);
    Assert!((*ct).my_cache == cache);

    if !(*ct).c_list.is_null() {
        // The cleanest way to handle this is to call CatCacheRemoveCList,
        // which will recurse back to me, and the recursive call will do the
        // work.  Set the "dead" flag to make sure it does recurse.
        (*ct).dead = true;
        CatCacheRemoveCList(cache, (*ct).c_list);
        return; // nothing left to do
    }

    // delink from linked list
    dlist_delete(&mut (*ct).cache_elem);

    // Free keys when we're dealing with a negative entry, normal entries just
    // point into tuple, allocated together with the CatCTup.
    if (*ct).negative {
        CatCacheFreeKeys(
            (*cache).cc_tupdesc,
            (*cache).cc_nkeys,
            (*cache).cc_keyno.as_mut_ptr(),
            (*ct).keys.as_mut_ptr(),
        );
    }

    pfree(ct as *mut c_void);

    (*cache).cc_ntup -= 1;
    (*CacheHdr).ch_ntup -= 1;
}

//		CatCacheRemoveCList
//
// Unlink and delete the given cache list entry
//
// NB: any dead member entries that become unreferenced are deleted too.
unsafe fn CatCacheRemoveCList(cache: *mut CatCache, cl: *mut CatCList) {
    Assert!((*cl).refcount == 0);
    Assert!((*cl).my_cache == cache);

    // delink from member tuples
    let mut i = (*cl).n_members;
    while {
        i -= 1;
        i >= 0
    } {
        let ct = *(*cl).members.as_ptr().add(i as usize);

        Assert!((*ct).c_list == cl);
        (*ct).c_list = null_mut();
        // if the member is dead and now has no references, remove it
        if (*ct).dead && (*ct).refcount == 0 {
            CatCacheRemoveCTup(cache, ct);
        }
    }

    // delink from linked list
    dlist_delete(&mut (*cl).cache_elem);

    // free associated column data
    CatCacheFreeKeys(
        (*cache).cc_tupdesc,
        (*cl).nkeys as c_int,
        (*cache).cc_keyno.as_mut_ptr(),
        (*cl).keys.as_mut_ptr(),
    );

    pfree(cl as *mut c_void);

    (*cache).cc_nlist -= 1;
}

//	CatCacheInvalidate
//
//	Invalidate entries in the specified cache, given a hash value.
//
//	We delete cache entries that match the hash value, whether positive
//	or negative.  We don't care whether the invalidation is the result
//	of a tuple insertion or a deletion.
//
//	This routine is only quasi-public: it should only be used by inval.c.
pub unsafe fn CatCacheInvalidate(cache: *mut CatCache, hashValue: uint32) {
    let hashIndex: Index;
    let mut iter = core::mem::zeroed::<dlist_mutable_iter>();

    // CACHE_elog(DEBUG2, "CatCacheInvalidate: called");

    // We don't bother to check whether the cache has finished initialization
    // yet; if not, there will be no entries in it so no problem.

    // Invalidate *all* CatCLists in this cache; it's too hard to tell which
    // searches might still be correct, so just zap 'em all.
    for i in 0..(*cache).cc_nlbuckets {
        let bucket = (*cache).cc_lbucket.add(i as usize);

        dlist_foreach_modify!(iter, bucket, {
            let cl = dlist_container!(CatCList, cache_elem, iter.cur);

            if (*cl).refcount > 0 {
                (*cl).dead = true;
            } else {
                CatCacheRemoveCList(cache, cl);
            }
        });
    }

    // inspect the proper hash bucket for tuple matches
    hashIndex = HASH_INDEX(hashValue, (*cache).cc_nbuckets);
    dlist_foreach_modify!(iter, (*cache).cc_bucket.add(hashIndex as usize), {
        let ct = dlist_container!(CatCTup, cache_elem, iter.cur);

        if hashValue == (*ct).hash_value {
            if (*ct).refcount > 0 || (!(*ct).c_list.is_null() && (*(*ct).c_list).refcount > 0) {
                (*ct).dead = true;
                // list, if any, was marked dead above
                Assert!((*ct).c_list.is_null() || (*(*ct).c_list).dead);
            } else {
                CatCacheRemoveCTup(cache, ct);
            }
            // CACHE_elog(DEBUG2, "CatCacheInvalidate: invalidated");
            (*cache).cc_invals += 1;
            // could be multiple matches, so keep looking!
        }
    });

    // Also invalidate any entries that are being built
    let mut e = catcache_in_progress_stack;
    while !e.is_null() {
        if (*e).cache == cache {
            if (*e).list || (*e).hash_value == hashValue {
                (*e).dead = true;
            }
        }
        e = (*e).next;
    }
}

// ----------------------------------------------------------------
//					   public functions
// ----------------------------------------------------------------

// Standard routine for creating cache context if it doesn't exist yet
//
// There are a lot of places (probably far more than necessary) that check
// whether CacheMemoryContext exists yet and want to create it if not.
// We centralize knowledge of exactly how to create it here.
pub unsafe fn CreateCacheMemoryContext() {
    // Purely for paranoia, check that context doesn't exist; caller probably
    // did so already.
    if CacheMemoryContext.is_null() {
        CacheMemoryContext = AllocSetContextCreate!(
            TopMemoryContext,
            c"CacheMemoryContext".as_ptr(),
            ALLOCSET_DEFAULT_SIZES
        );
    }
}

//		ResetCatalogCache
//
// Reset one catalog cache to empty.
unsafe fn ResetCatalogCache(cache: *mut CatCache, debug_discard: bool) {
    let mut iter = core::mem::zeroed::<dlist_mutable_iter>();

    // Remove each list in this cache, or at least mark it dead
    for i in 0..(*cache).cc_nlbuckets {
        let bucket = (*cache).cc_lbucket.add(i as usize);

        dlist_foreach_modify!(iter, bucket, {
            let cl = dlist_container!(CatCList, cache_elem, iter.cur);

            if (*cl).refcount > 0 {
                (*cl).dead = true;
            } else {
                CatCacheRemoveCList(cache, cl);
            }
        });
    }

    // Remove each tuple in this cache, or at least mark it dead
    for i in 0..(*cache).cc_nbuckets {
        let bucket = (*cache).cc_bucket.add(i as usize);

        dlist_foreach_modify!(iter, bucket, {
            let ct = dlist_container!(CatCTup, cache_elem, iter.cur);

            if (*ct).refcount > 0 || (!(*ct).c_list.is_null() && (*(*ct).c_list).refcount > 0) {
                (*ct).dead = true;
                // list, if any, was marked dead above
                Assert!((*ct).c_list.is_null() || (*(*ct).c_list).dead);
            } else {
                CatCacheRemoveCTup(cache, ct);
            }
            (*cache).cc_invals += 1;
        });
    }

    // Also invalidate any entries that are being built
    if !debug_discard {
        let mut e = catcache_in_progress_stack;
        while !e.is_null() {
            if (*e).cache == cache {
                (*e).dead = true;
            }
            e = (*e).next;
        }
    }
}

//		ResetCatalogCaches
//
// Reset all caches when a shared cache inval event forces it
pub unsafe fn ResetCatalogCaches() {
    ResetCatalogCachesExt(false);
}

pub unsafe fn ResetCatalogCachesExt(debug_discard: bool) {
    let mut iter = core::mem::zeroed::<slist_iter>();

    // CACHE_elog(DEBUG2, "ResetCatalogCaches called");

    slist_foreach!(iter, &mut (*CacheHdr).ch_caches, {
        let cache = slist_container!(CatCache, cc_next, iter.cur);

        ResetCatalogCache(cache, debug_discard);
    });

    // CACHE_elog(DEBUG2, "end of ResetCatalogCaches call");
}

//		CatalogCacheFlushCatalog
//
//	Flush all catcache entries that came from the specified system catalog.
//	This is needed after VACUUM FULL/CLUSTER on the catalog, since the
//	tuples very likely now have different TIDs than before.
pub unsafe fn CatalogCacheFlushCatalog(catId: Oid) {
    let mut iter = core::mem::zeroed::<slist_iter>();

    // CACHE_elog(DEBUG2, "CatalogCacheFlushCatalog called for %u", catId);

    slist_foreach!(iter, &mut (*CacheHdr).ch_caches, {
        let cache = slist_container!(CatCache, cc_next, iter.cur);

        // Does this cache store tuples of the target catalog?
        if (*cache).cc_reloid == catId {
            // Yes, so flush all its contents
            ResetCatalogCache(cache, false);

            // Tell inval.c to call syscache callbacks for this cache
            CallSyscacheCallbacks((*cache).id, 0);
        }
    });

    // CACHE_elog(DEBUG2, "end of CatalogCacheFlushCatalog call");
}

//		InitCatCache
//
//	This allocates and initializes a cache for a system catalog relation.
//	Actually, the cache is only partially initialized to avoid opening the
//	relation.  The relation will be opened and the rest of the cache
//	structure initialized on the first access.
pub unsafe fn InitCatCache(
    id: c_int,
    reloid: Oid,
    indexoid: Oid,
    nkeys: c_int,
    key: *const c_int,
    nbuckets: c_int,
) -> *mut CatCache {
    let cp: *mut CatCache;
    let oldcxt: MemoryContext;

    // nbuckets is the initial number of hash buckets to use in this catcache.
    // It will be enlarged later if it becomes too full.
    //
    // nbuckets must be a power of two.  We check this via Assert rather than
    // a full runtime check because the values will be coming from constant
    // tables.
    Assert!(nbuckets > 0 && (nbuckets & -nbuckets) == nbuckets);

    // first switch to the cache context so our allocations do not vanish at
    // the end of a transaction
    if CacheMemoryContext.is_null() {
        CreateCacheMemoryContext();
    }

    oldcxt = MemoryContextSwitchTo(CacheMemoryContext);

    // if first time through, initialize the cache group header
    if CacheHdr.is_null() {
        CacheHdr = palloc(core::mem::size_of::<CatCacheHeader>()) as *mut CatCacheHeader;
        slist_init(&mut (*CacheHdr).ch_caches);
        (*CacheHdr).ch_ntup = 0;
        // CATCACHE_STATS: on_proc_exit(CatCachePrintStats, 0);
    }

    // Allocate a new cache structure, aligning to a cacheline boundary
    //
    // Note: we rely on zeroing to initialize all the dlist headers correctly
    cp = palloc_aligned(
        core::mem::size_of::<CatCache>(),
        PG_CACHE_LINE_SIZE,
        MCXT_ALLOC_ZERO,
    ) as *mut CatCache;
    (*cp).cc_bucket =
        palloc0(nbuckets as usize * core::mem::size_of::<dlist_head>()) as *mut dlist_head;

    // Many catcaches never receive any list searches.  Therefore, we don't
    // allocate the cc_lbuckets till we get a list search.
    (*cp).cc_lbucket = null_mut();

    // initialize the cache's relation information for the relation
    // corresponding to this cache, and initialize some of the new cache's
    // other internal fields.  But don't open the relation yet.
    (*cp).id = id;
    (*cp).cc_relname = c"(not known yet)".as_ptr();
    (*cp).cc_reloid = reloid;
    (*cp).cc_indexoid = indexoid;
    (*cp).cc_relisshared = false; // temporary
    (*cp).cc_tupdesc = null_mut() as TupleDesc;
    (*cp).cc_ntup = 0;
    (*cp).cc_nlist = 0;
    (*cp).cc_nbuckets = nbuckets;
    (*cp).cc_nlbuckets = 0;
    (*cp).cc_nkeys = nkeys;
    for i in 0..nkeys as usize {
        Assert!(AttributeNumberIsValid(*key.add(i)));
        (*cp).cc_keyno[i] = *key.add(i);
    }

    // new cache is initialized as far as we can go for now. print some
    // debugging information, if appropriate.
    // InitCatCache_DEBUG2;

    // add completed cache to top of group header's list
    slist_push_head(&mut (*CacheHdr).ch_caches, &mut (*cp).cc_next);

    // back to the old context before we return...
    MemoryContextSwitchTo(oldcxt);

    cp
}

// Enlarge a catcache, doubling the number of buckets.
unsafe fn RehashCatCache(cp: *mut CatCache) {
    let newbucket: *mut dlist_head;
    let newnbuckets: c_int;

    elog!(
        DEBUG1,
        "rehashing catalog cache id {} for {}; {} tups, {} buckets",
        (*cp).id,
        CStr::from_ptr((*cp).cc_relname).to_string_lossy(),
        (*cp).cc_ntup,
        (*cp).cc_nbuckets
    );

    // Allocate a new, larger, hash table.
    newnbuckets = (*cp).cc_nbuckets * 2;
    newbucket = MemoryContextAllocZero(
        CacheMemoryContext,
        newnbuckets as usize * core::mem::size_of::<dlist_head>(),
    ) as *mut dlist_head;

    // Move all entries from old hash table to new.
    for i in 0..(*cp).cc_nbuckets {
        let mut iter = core::mem::zeroed::<dlist_mutable_iter>();

        dlist_foreach_modify!(iter, (*cp).cc_bucket.add(i as usize), {
            let ct = dlist_container!(CatCTup, cache_elem, iter.cur);
            let hashIndex = HASH_INDEX((*ct).hash_value, newnbuckets);

            dlist_delete(iter.cur);
            dlist_push_head(newbucket.add(hashIndex as usize), &mut (*ct).cache_elem);
        });
    }

    // Switch to the new array.
    pfree((*cp).cc_bucket as *mut c_void);
    (*cp).cc_nbuckets = newnbuckets;
    (*cp).cc_bucket = newbucket;
}

// Enlarge a catcache's list storage, doubling the number of buckets.
unsafe fn RehashCatCacheLists(cp: *mut CatCache) {
    let newbucket: *mut dlist_head;
    let newnbuckets: c_int;

    elog!(
        DEBUG1,
        "rehashing catalog cache id {} for {}; {} lists, {} buckets",
        (*cp).id,
        CStr::from_ptr((*cp).cc_relname).to_string_lossy(),
        (*cp).cc_nlist,
        (*cp).cc_nlbuckets
    );

    // Allocate a new, larger, hash table.
    newnbuckets = (*cp).cc_nlbuckets * 2;
    newbucket = MemoryContextAllocZero(
        CacheMemoryContext,
        newnbuckets as usize * core::mem::size_of::<dlist_head>(),
    ) as *mut dlist_head;

    // Move all entries from old hash table to new.
    for i in 0..(*cp).cc_nlbuckets {
        let mut iter = core::mem::zeroed::<dlist_mutable_iter>();

        dlist_foreach_modify!(iter, (*cp).cc_lbucket.add(i as usize), {
            let cl = dlist_container!(CatCList, cache_elem, iter.cur);
            let hashIndex = HASH_INDEX((*cl).hash_value, newnbuckets);

            dlist_delete(iter.cur);
            dlist_push_head(newbucket.add(hashIndex as usize), &mut (*cl).cache_elem);
        });
    }

    // Switch to the new array.
    pfree((*cp).cc_lbucket as *mut c_void);
    (*cp).cc_nlbuckets = newnbuckets;
    (*cp).cc_lbucket = newbucket;
}

//		ConditionalCatalogCacheInitializeCache
//
// Call CatalogCacheInitializeCache() if not yet done.
#[inline(always)]
unsafe fn ConditionalCatalogCacheInitializeCache(cache: *mut CatCache) {
    // USE_ASSERT_CHECKING block:
    // TypeCacheRelCallback() runs outside transactions and relies on TYPEOID
    // for hashing.  InvalidateAttoptCacheCallback() runs outside transactions
    // and likewise relies on ATTNUM.
    if !((*cache).id == TYPEOID || (*cache).id == ATTNUM) || IsTransactionState() {
        // AssertCouldGetRelation();
    } else {
        Assert!(!(*cache).cc_tupdesc.is_null());
    }

    if (*cache).cc_tupdesc.is_null() {
        CatalogCacheInitializeCache(cache);
    }
}

//		CatalogCacheInitializeCache
//
// This function does final initialization of a catcache: obtain the tuple
// descriptor and set up the hash and equality function links.
unsafe fn CatalogCacheInitializeCache(cache: *mut CatCache) {
    let relation: Relation;
    let oldcxt: MemoryContext;
    let tupdesc: TupleDesc;

    // CatalogCacheInitializeCache_DEBUG1;

    relation = table_open((*cache).cc_reloid, AccessShareLock);

    // switch to the cache context so our allocations do not vanish at the end
    // of a transaction
    Assert!(!CacheMemoryContext.is_null());

    oldcxt = MemoryContextSwitchTo(CacheMemoryContext);

    // copy the relcache's tuple descriptor to permanent cache storage
    tupdesc = CreateTupleDescCopyConstr(RelationGetDescr(relation));

    // save the relation's name and relisshared flag, too (cc_relname is used
    // only for debugging purposes)
    (*cache).cc_relname = pstrdup(RelationGetRelationName(relation));
    (*cache).cc_relisshared = RelationGetForm_relisshared(relation);

    // return to the caller's memory context and close the rel
    MemoryContextSwitchTo(oldcxt);

    table_close(relation, AccessShareLock);

    // CACHE_elog(DEBUG2, "CatalogCacheInitializeCache: %s, %d keys", ...)

    // initialize cache's key information
    for i in 0..(*cache).cc_nkeys as usize {
        let keytype: Oid;
        let mut eqfunc: RegProcedure = InvalidOid;

        // CatalogCacheInitializeCache_DEBUG2;

        if (*cache).cc_keyno[i] > 0 {
            let attr = TupleDescAttr(tupdesc, (*cache).cc_keyno[i] - 1);

            keytype = (*attr).atttypid;
            // cache key columns should always be NOT NULL
            Assert!((*attr).attnotnull);
        } else {
            if (*cache).cc_keyno[i] < 0 {
                elog!(FATAL, "sys attributes are not supported in caches");
            }
            keytype = OIDOID;
        }

        GetCCHashEqFuncs(
            keytype,
            &mut (*cache).cc_hashfunc[i],
            &mut eqfunc,
            &mut (*cache).cc_fastequal[i],
        );

        // Do equality-function lookup (we assume this won't need a catalog
        // lookup for any supported type)
        fmgr_info_cxt(
            eqfunc,
            &mut (*cache).cc_skey[i].sk_func as *mut _ as *mut c_void,
            CacheMemoryContext,
        );

        // Initialize sk_attno suitably for HeapKeyTest() and heap scans
        (*cache).cc_skey[i].sk_attno = (*cache).cc_keyno[i] as AttrNumber;

        // Fill in sk_strategy as well --- always standard equality
        (*cache).cc_skey[i].sk_strategy = BTEqualStrategyNumber as StrategyNumber;
        (*cache).cc_skey[i].sk_subtype = InvalidOid;
        // If a catcache key requires a collation, it must be C collation
        (*cache).cc_skey[i].sk_collation = C_COLLATION_OID;

        // CACHE_elog(DEBUG2, "CatalogCacheInitializeCache %s %d %p", ...)
    }

    // mark this cache fully initialized
    (*cache).cc_tupdesc = tupdesc;
}

// InitCatCachePhase2 -- external interface for CatalogCacheInitializeCache
//
// One reason to call this routine is to ensure that the relcache has
// created entries for all the catalogs and indexes referenced by catcaches.
pub unsafe fn InitCatCachePhase2(cache: *mut CatCache, touch_index: bool) {
    ConditionalCatalogCacheInitializeCache(cache);

    if touch_index && (*cache).id != AMOID && (*cache).id != AMNAME {
        let idesc: Relation;

        // We must lock the underlying catalog before opening the index to
        // avoid deadlock, since index_open could possibly result in reading
        // this same catalog, and if anyone else is exclusive-locking this
        // catalog and index they'll be doing it in that order.
        LockRelationOid((*cache).cc_reloid, AccessShareLock);
        idesc = index_open((*cache).cc_indexoid, AccessShareLock);

        // While we've got the index open, let's check that it's unique (and
        // not just deferrable-unique, thank you very much).
        Assert!(rd_index_indisunique(idesc) && rd_index_indimmediate(idesc));

        index_close(idesc, AccessShareLock);
        UnlockRelationOid((*cache).cc_reloid, AccessShareLock);
    }
}

//		IndexScanOK
//
//		This function checks for tuples that will be fetched by
//		IndexSupportInitialize() during relcache initialization for
//		certain system indexes that support critical syscaches.
unsafe fn IndexScanOK(cache: *mut CatCache) -> bool {
    match (*cache).id {
        x if x == INDEXRELID => {
            // Rather than tracking exactly which indexes have to be loaded
            // before we can use indexscans (which changes from time to time),
            // just force all pg_index searches to be heap scans until we've
            // built the critical relcaches.
            if !criticalRelcachesBuilt {
                return false;
            }
        }

        x if x == AMOID || x == AMNAME => {
            // Always do heap scans in pg_am, because it's so small there's
            // not much point in an indexscan anyway.
            return false;
        }

        x if x == AUTHNAME || x == AUTHOID || x == AUTHMEMMEMROLE || x == DATABASEOID => {
            // Protect authentication lookups occurring before relcache has
            // collected entries for shared indexes.
            if !criticalSharedRelcachesBuilt {
                return false;
            }
        }

        _ => {}
    }

    // Normal case, allow index scan
    true
}

const AccessShareLock: c_int = 1;

//	SearchCatCache
//
//		This call searches a system cache for a tuple, opening the relation
//		if necessary (on the first access to a particular cache).
//
//		The result is NULL if not found, or a pointer to a HeapTuple in
//		the cache.  The caller must not modify the tuple, and must call
//		ReleaseCatCache() when done with it.
pub unsafe fn SearchCatCache(
    cache: *mut CatCache,
    v1: Datum,
    v2: Datum,
    v3: Datum,
    v4: Datum,
) -> HeapTuple {
    SearchCatCacheInternal(cache, (*cache).cc_nkeys, v1, v2, v3, v4)
}

// SearchCatCacheN() are SearchCatCache() versions for a specific number of
// arguments.
pub unsafe fn SearchCatCache1(cache: *mut CatCache, v1: Datum) -> HeapTuple {
    SearchCatCacheInternal(cache, 1, v1, 0, 0, 0)
}

pub unsafe fn SearchCatCache2(cache: *mut CatCache, v1: Datum, v2: Datum) -> HeapTuple {
    SearchCatCacheInternal(cache, 2, v1, v2, 0, 0)
}

pub unsafe fn SearchCatCache3(cache: *mut CatCache, v1: Datum, v2: Datum, v3: Datum) -> HeapTuple {
    SearchCatCacheInternal(cache, 3, v1, v2, v3, 0)
}

pub unsafe fn SearchCatCache4(
    cache: *mut CatCache,
    v1: Datum,
    v2: Datum,
    v3: Datum,
    v4: Datum,
) -> HeapTuple {
    SearchCatCacheInternal(cache, 4, v1, v2, v3, v4)
}

// Work-horse for SearchCatCache/SearchCatCacheN.
#[inline]
unsafe fn SearchCatCacheInternal(
    cache: *mut CatCache,
    nkeys: c_int,
    v1: Datum,
    v2: Datum,
    v3: Datum,
    v4: Datum,
) -> HeapTuple {
    let mut arguments: [Datum; CATCACHE_MAXKEYS] = [0; CATCACHE_MAXKEYS];
    let hashValue: uint32;
    let hashIndex: Index;
    let mut iter = core::mem::zeroed::<dlist_iter>();
    let bucket: *mut dlist_head;
    let mut ct: *mut CatCTup;

    Assert!((*cache).cc_nkeys == nkeys);

    // one-time startup overhead for each cache
    ConditionalCatalogCacheInitializeCache(cache);

    (*cache).cc_searches += 1;

    // Initialize local parameter array
    arguments[0] = v1;
    arguments[1] = v2;
    arguments[2] = v3;
    arguments[3] = v4;

    // find the hash bucket in which to look for the tuple
    hashValue = CatalogCacheComputeHashValue(cache, nkeys, v1, v2, v3, v4);
    hashIndex = HASH_INDEX(hashValue, (*cache).cc_nbuckets);

    // scan the hash bucket until we find a match or exhaust our tuples
    //
    // Note: it's okay to use dlist_foreach here, even though we modify the
    // dlist within the loop, because we don't continue the loop afterwards.
    bucket = (*cache).cc_bucket.add(hashIndex as usize);
    dlist_foreach!(iter, bucket, {
        ct = dlist_container!(CatCTup, cache_elem, iter.cur);

        if (*ct).dead {
            continue; // ignore dead entries
        }

        if (*ct).hash_value != hashValue {
            continue; // quickly skip entry if wrong hash val
        }

        if !CatalogCacheCompareTuple(cache, nkeys, (*ct).keys.as_ptr(), arguments.as_ptr()) {
            continue;
        }

        // We found a match in the cache.  Move it to the front of the list
        // for its hashbucket, in order to speed subsequent searches.
        dlist_move_head(bucket, &mut (*ct).cache_elem);

        // If it's a positive entry, bump its refcount and return it. If it's
        // negative, we can report failure to the caller.
        if !(*ct).negative {
            ResourceOwnerEnlarge(CurrentResourceOwner);
            (*ct).refcount += 1;
            ResourceOwnerRememberCatCacheRef(CurrentResourceOwner, &mut (*ct).tuple);

            // CACHE_elog(DEBUG2, "SearchCatCache(%s): found in bucket %d", ...)

            (*cache).cc_hits += 1;

            return &mut (*ct).tuple;
        } else {
            // CACHE_elog(DEBUG2, "SearchCatCache(%s): found neg entry in bucket %d", ...)

            (*cache).cc_neg_hits += 1;

            return null_mut();
        }
    });

    SearchCatCacheMiss(cache, nkeys, hashValue, hashIndex, v1, v2, v3, v4)
}

// Search the actual catalogs, rather than the cache.
//
// This is kept separate from SearchCatCacheInternal() to keep the fast-path
// as small as possible.
unsafe fn SearchCatCacheMiss(
    cache: *mut CatCache,
    nkeys: c_int,
    hashValue: uint32,
    hashIndex: Index,
    v1: Datum,
    v2: Datum,
    v3: Datum,
    v4: Datum,
) -> HeapTuple {
    let mut cur_skey: [ScanKeyData; CATCACHE_MAXKEYS] = core::mem::zeroed();
    let relation: Relation;
    let mut scandesc: SysScanDesc;
    let mut ntp: HeapTuple;
    let mut ct: *mut CatCTup;
    let mut stale: bool;
    let mut arguments: [Datum; CATCACHE_MAXKEYS] = [0; CATCACHE_MAXKEYS];

    // Initialize local parameter array
    arguments[0] = v1;
    arguments[1] = v2;
    arguments[2] = v3;
    arguments[3] = v4;

    // Tuple was not found in cache, so we have to try to retrieve it directly
    // from the relation.  If found, we will add it to the cache; if not
    // found, we will add a negative cache entry instead.
    relation = table_open((*cache).cc_reloid, AccessShareLock);

    // Ok, need to make a lookup in the relation, copy the scankey and fill
    // out any per-call fields.
    core::ptr::copy_nonoverlapping(
        (*cache).cc_skey.as_ptr(),
        cur_skey.as_mut_ptr(),
        nkeys as usize,
    );
    cur_skey[0].sk_argument = v1;
    cur_skey[1].sk_argument = v2;
    cur_skey[2].sk_argument = v3;
    cur_skey[3].sk_argument = v4;

    loop {
        scandesc = systable_beginscan(
            relation,
            (*cache).cc_indexoid,
            IndexScanOK(cache),
            null_mut(),
            nkeys,
            cur_skey.as_mut_ptr(),
        );

        ct = null_mut();
        stale = false;

        loop {
            ntp = systable_getnext(scandesc);
            if !HeapTupleIsValid(ntp) {
                break;
            }
            ct = CatalogCacheCreateEntry(cache, ntp, null_mut(), hashValue, hashIndex);
            // upon failure, we must start the scan over
            if ct.is_null() {
                stale = true;
                break;
            }
            // immediately set the refcount to 1
            ResourceOwnerEnlarge(CurrentResourceOwner);
            (*ct).refcount += 1;
            ResourceOwnerRememberCatCacheRef(CurrentResourceOwner, &mut (*ct).tuple);
            break; // assume only one match
        }

        systable_endscan(scandesc);

        if !stale {
            break;
        }
    }

    table_close(relation, AccessShareLock);

    // If tuple was not found, we need to build a negative cache entry
    // containing a fake tuple.  The fake tuple has the correct key columns,
    // but nulls everywhere else.
    if ct.is_null() {
        if IsBootstrapProcessingMode() {
            return null_mut();
        }

        ct = CatalogCacheCreateEntry(cache, null_mut(), arguments.as_mut_ptr(), hashValue, hashIndex);

        // Creating a negative cache entry shouldn't fail
        Assert!(!ct.is_null());

        // CACHE_elog(DEBUG2, ...)

        // We are not returning the negative entry to the caller, so leave its
        // refcount zero.

        return null_mut();
    }

    // CACHE_elog(DEBUG2, "SearchCatCache(%s): put in bucket %d", ...)

    (*cache).cc_newloads += 1;

    &mut (*ct).tuple
}

//	ReleaseCatCache
//
//	Decrement the reference count of a catcache entry (releasing the
//	hold grabbed by a successful SearchCatCache).
pub unsafe fn ReleaseCatCache(tuple: HeapTuple) {
    ReleaseCatCacheWithOwner(tuple, CurrentResourceOwner);
}

unsafe fn ReleaseCatCacheWithOwner(tuple: HeapTuple, resowner: ResourceOwner) {
    let ct = (tuple as *mut c_char).sub(core::mem::offset_of!(CatCTup, tuple)) as *mut CatCTup;

    // Safety checks to ensure we were handed a cache entry
    Assert!((*ct).ct_magic == CT_MAGIC);
    Assert!((*ct).refcount > 0);

    (*ct).refcount -= 1;
    if !resowner.is_null() {
        ResourceOwnerForgetCatCacheRef(resowner, &mut (*ct).tuple);
    }

    if (*ct).dead
        && (*ct).refcount == 0
        && ((*ct).c_list.is_null() || (*(*ct).c_list).refcount == 0)
    {
        CatCacheRemoveCTup((*ct).my_cache, ct);
    }
}

//	GetCatCacheHashValue
//
//		Compute the hash value for a given set of search keys.
pub unsafe fn GetCatCacheHashValue(
    cache: *mut CatCache,
    v1: Datum,
    v2: Datum,
    v3: Datum,
    v4: Datum,
) -> uint32 {
    // one-time startup overhead for each cache
    ConditionalCatalogCacheInitializeCache(cache);

    // calculate the hash value
    CatalogCacheComputeHashValue(cache, (*cache).cc_nkeys, v1, v2, v3, v4)
}

//	SearchCatCacheList
//
//		Generate a list of all tuples matching a partial key (that is,
//		a key specifying just the first K of the cache's N key columns).
pub unsafe fn SearchCatCacheList(
    cache: *mut CatCache,
    nkeys: c_int,
    v1: Datum,
    v2: Datum,
    v3: Datum,
) -> *mut CatCList {
    let v4: Datum = 0; // dummy last-column value
    let mut arguments: [Datum; CATCACHE_MAXKEYS] = [0; CATCACHE_MAXKEYS];
    let lHashValue: uint32;
    let lHashIndex: Index;
    let mut iter = core::mem::zeroed::<dlist_iter>();
    let lbucket: *mut dlist_head;
    let mut cl: *mut CatCList;
    let mut ct: *mut CatCTup;
    let mut ctlist: *mut List;
    let mut nmembers: c_int = 0;
    let mut ordered: bool = false;
    let mut ntp: HeapTuple;
    let oldcxt: MemoryContext;
    let mut i: c_int;
    let save_in_progress: *mut CatCInProgress;
    let mut in_progress_ent: CatCInProgress = core::mem::zeroed();

    // one-time startup overhead for each cache
    ConditionalCatalogCacheInitializeCache(cache);

    Assert!(nkeys > 0 && nkeys < (*cache).cc_nkeys);

    (*cache).cc_lsearches += 1;

    // Initialize local parameter array
    arguments[0] = v1;
    arguments[1] = v2;
    arguments[2] = v3;
    arguments[3] = v4;

    // If we haven't previously done a list search in this cache, create the
    // bucket header array; otherwise, consider whether it's time to enlarge it.
    if (*cache).cc_lbucket.is_null() {
        // Arbitrary initial size --- must be a power of 2
        let nbuckets: c_int = 16;

        (*cache).cc_lbucket = MemoryContextAllocZero(
            CacheMemoryContext,
            nbuckets as usize * core::mem::size_of::<dlist_head>(),
        ) as *mut dlist_head;
        // Don't set cc_nlbuckets if we get OOM allocating cc_lbucket
        (*cache).cc_nlbuckets = nbuckets;
    } else {
        // If the hash table has become too full, enlarge the buckets array.
        // Quite arbitrarily, we enlarge when fill factor > 2.
        if (*cache).cc_nlist > (*cache).cc_nlbuckets * 2 {
            RehashCatCacheLists(cache);
        }
    }

    // Find the hash bucket in which to look for the CatCList.
    lHashValue = CatalogCacheComputeHashValue(cache, nkeys, v1, v2, v3, v4);
    lHashIndex = HASH_INDEX(lHashValue, (*cache).cc_nlbuckets);

    // scan the items until we find a match or exhaust our list
    lbucket = (*cache).cc_lbucket.add(lHashIndex as usize);
    dlist_foreach!(iter, lbucket, {
        cl = dlist_container!(CatCList, cache_elem, iter.cur);

        if (*cl).dead {
            continue; // ignore dead entries
        }

        if (*cl).hash_value != lHashValue {
            continue; // quickly skip entry if wrong hash val
        }

        // see if the cached list matches our key.
        if (*cl).nkeys as c_int != nkeys {
            continue;
        }

        if !CatalogCacheCompareTuple(cache, nkeys, (*cl).keys.as_ptr(), arguments.as_ptr()) {
            continue;
        }

        // We found a matching list.  Move the list to the front of the list
        // for its hashbucket, so as to speed subsequent searches.
        dlist_move_head(lbucket, &mut (*cl).cache_elem);

        // Bump the list's refcount and return it
        ResourceOwnerEnlarge(CurrentResourceOwner);
        (*cl).refcount += 1;
        ResourceOwnerRememberCatCacheListRef(CurrentResourceOwner, cl);

        // CACHE_elog(DEBUG2, "SearchCatCacheList(%s): found list", ...)

        (*cache).cc_lhits += 1;

        return cl;
    });

    // List was not found in cache, so we have to build it by reading the
    // relation.  For each matching tuple found in the relation, use an
    // existing cache entry if possible, else build a new one.
    ctlist = NIL;

    // Cache invalidation can happen while we're building the list.
    // Register an "in-progress" entry that will receive the invalidation,
    // until we have built the final list entry.
    save_in_progress = catcache_in_progress_stack;
    in_progress_ent.next = catcache_in_progress_stack;
    in_progress_ent.cache = cache;
    in_progress_ent.hash_value = lHashValue;
    in_progress_ent.list = true;
    in_progress_ent.dead = false;
    catcache_in_progress_stack = &mut in_progress_ent;

    // PG_TRY()
    {
        let mut cur_skey: [ScanKeyData; CATCACHE_MAXKEYS] = core::mem::zeroed();
        let relation: Relation;
        let mut scandesc: SysScanDesc;
        let mut first_iter = true;

        relation = table_open((*cache).cc_reloid, AccessShareLock);

        // Ok, need to make a lookup in the relation, copy the scankey and
        // fill out any per-call fields.
        core::ptr::copy_nonoverlapping(
            (*cache).cc_skey.as_ptr(),
            cur_skey.as_mut_ptr(),
            (*cache).cc_nkeys as usize,
        );
        cur_skey[0].sk_argument = v1;
        cur_skey[1].sk_argument = v2;
        cur_skey[2].sk_argument = v3;
        cur_skey[3].sk_argument = v4;

        // Scan the table for matching entries.  If an invalidation arrives
        // mid-build, we will loop back here to retry.
        loop {
            // If we are retrying, release refcounts on any items created on
            // the previous iteration.
            foreach!(ctlist_item, ctlist, {
                ct = lfirst(current_cell!(ctlist_item)) as *mut CatCTup;
                Assert!((*ct).c_list.is_null());
                Assert!((*ct).refcount > 0);
                (*ct).refcount -= 1;
            });
            // Reset ctlist in preparation for new try
            ctlist = NIL;
            in_progress_ent.dead = false;

            scandesc = systable_beginscan(
                relation,
                (*cache).cc_indexoid,
                IndexScanOK(cache),
                null_mut(),
                nkeys,
                cur_skey.as_mut_ptr(),
            );

            // The list will be ordered iff we are doing an index scan
            ordered = !(*scandesc).irel.is_null();

            // Injection point to help testing the recursive invalidation case
            if first_iter {
                // INJECTION_POINT("catcache-list-miss-systable-scan-started", NULL);
                first_iter = false;
            }

            loop {
                ntp = systable_getnext(scandesc);
                if !(HeapTupleIsValid(ntp) && !in_progress_ent.dead) {
                    break;
                }

                let hashValue: uint32;
                let hashIndex: Index;
                let mut found = false;
                let bucket: *mut dlist_head;

                // See if there's an entry for this tuple already.
                ct = null_mut();
                hashValue = CatalogCacheComputeTupleHashValue(cache, (*cache).cc_nkeys, ntp);
                hashIndex = HASH_INDEX(hashValue, (*cache).cc_nbuckets);

                bucket = (*cache).cc_bucket.add(hashIndex as usize);
                dlist_foreach!(iter, bucket, {
                    ct = dlist_container!(CatCTup, cache_elem, iter.cur);

                    if (*ct).dead || (*ct).negative {
                        continue; // ignore dead and negative entries
                    }

                    if (*ct).hash_value != hashValue {
                        continue; // quickly skip entry if wrong hash val
                    }

                    if !ItemPointerEquals(
                        &(*ct).tuple.t_self as *const _ as *const c_void,
                        &(*ntp).t_self as *const _ as *const c_void,
                    ) {
                        continue; // not same tuple
                    }

                    // Found a match, but can't use it if it belongs to
                    // another list already
                    if !(*ct).c_list.is_null() {
                        continue;
                    }

                    found = true;
                    break; // A-OK
                });

                if !found {
                    // We didn't find a usable entry, so make a new one
                    ct = CatalogCacheCreateEntry(cache, ntp, null_mut(), hashValue, hashIndex);

                    // upon failure, we must start the scan over
                    if ct.is_null() {
                        in_progress_ent.dead = true;
                        break;
                    }
                }

                // Careful here: add entry to ctlist, then bump its refcount
                // This way leaves state correct if lappend runs out of memory
                ctlist = lappend(ctlist, ct as *mut c_void);
                (*ct).refcount += 1;
            }

            systable_endscan(scandesc);

            if !in_progress_ent.dead {
                break;
            }
        }

        table_close(relation, AccessShareLock);

        // Make sure the resource owner has room to remember this entry.
        ResourceOwnerEnlarge(CurrentResourceOwner);

        // Now we can build the CatCList entry.
        oldcxt = MemoryContextSwitchTo(CacheMemoryContext);
        nmembers = list_length(ctlist);
        cl = palloc(
            core::mem::offset_of!(CatCList, members) + nmembers as usize * core::mem::size_of::<*mut CatCTup>(),
        ) as *mut CatCList;

        // Extract key values
        CatCacheCopyKeys(
            (*cache).cc_tupdesc,
            nkeys,
            (*cache).cc_keyno.as_mut_ptr(),
            arguments.as_mut_ptr(),
            (*cl).keys.as_mut_ptr(),
        );
        MemoryContextSwitchTo(oldcxt);

        // We are now past the last thing that could trigger an elog before we
        // have finished building the CatCList and remembering it in the
        // resource owner.
    }
    // PG_CATCH() handling is folded into the normal path; on error the C code
    // would undo refcounts and re-throw. Rust port relies on unwinding/abort.

    Assert!(catcache_in_progress_stack == &mut in_progress_ent as *mut CatCInProgress);
    catcache_in_progress_stack = save_in_progress;

    (*cl).cl_magic = CL_MAGIC;
    (*cl).my_cache = cache;
    (*cl).refcount = 0; // for the moment
    (*cl).dead = false;
    (*cl).ordered = ordered;
    (*cl).nkeys = nkeys as c_short;
    (*cl).hash_value = lHashValue;
    (*cl).n_members = nmembers;

    i = 0;
    foreach!(ctlist_item, ctlist, {
        ct = lfirst(current_cell!(ctlist_item)) as *mut CatCTup;
        *(*cl).members.as_mut_ptr().add(i as usize) = ct;
        i += 1;
        Assert!((*ct).c_list.is_null());
        (*ct).c_list = cl;
        // release the temporary refcount on the member
        Assert!((*ct).refcount > 0);
        (*ct).refcount -= 1;
        // mark list dead if any members already dead
        if (*ct).dead {
            (*cl).dead = true;
        }
    });
    Assert!(i == nmembers);

    // Add the CatCList to the appropriate bucket, and count it.
    dlist_push_head(lbucket, &mut (*cl).cache_elem);

    (*cache).cc_nlist += 1;

    // Finally, bump the list's refcount and return it
    (*cl).refcount += 1;
    ResourceOwnerRememberCatCacheListRef(CurrentResourceOwner, cl);

    // CACHE_elog(DEBUG2, "SearchCatCacheList(%s): made list of %d members", ...)

    cl
}

//	ReleaseCatCacheList
//
//	Decrement the reference count of a catcache list.
pub unsafe fn ReleaseCatCacheList(list: *mut CatCList) {
    ReleaseCatCacheListWithOwner(list, CurrentResourceOwner);
}

unsafe fn ReleaseCatCacheListWithOwner(list: *mut CatCList, resowner: ResourceOwner) {
    // Safety checks to ensure we were handed a cache entry
    Assert!((*list).cl_magic == CL_MAGIC);
    Assert!((*list).refcount > 0);
    (*list).refcount -= 1;
    if !resowner.is_null() {
        ResourceOwnerForgetCatCacheListRef(resowner, list);
    }

    if (*list).dead && (*list).refcount == 0 {
        CatCacheRemoveCList((*list).my_cache, list);
    }
}

// CatalogCacheCreateEntry
//		Create a new CatCTup entry, copying the given HeapTuple and other
//		supplied data into it.  The new entry initially has refcount 0.
unsafe fn CatalogCacheCreateEntry(
    cache: *mut CatCache,
    ntp: HeapTuple,
    arguments: *mut Datum,
    hashValue: uint32,
    hashIndex: Index,
) -> *mut CatCTup {
    let ct: *mut CatCTup;
    let oldcxt: MemoryContext;

    if !ntp.is_null() {
        let mut dtp: HeapTuple = null_mut();

        // To ensure we have test coverage for the retry paths in our callers,
        // make debug builds randomly fail about 0.1% of the times through this
        // code path, even when there's no toasted fields. (USE_ASSERT_CHECKING)
        if pg_prng_uint32(core::ptr::addr_of_mut!(pg_global_prng_state)) <= (PG_UINT32_MAX / 1000) {
            return null_mut();
        }

        // If there are any out-of-line toasted fields in the tuple, expand
        // them in-line.
        if HeapTupleHasExternal(ntp) {
            let save_in_progress: *mut CatCInProgress;
            let mut in_progress_ent: CatCInProgress = core::mem::zeroed();

            // The tuple could become stale while we are doing toast table
            // access (since AcceptInvalidationMessages can run then).  The
            // invalidation will mark our in-progress entry as dead.
            save_in_progress = catcache_in_progress_stack;
            in_progress_ent.next = catcache_in_progress_stack;
            in_progress_ent.cache = cache;
            in_progress_ent.hash_value = hashValue;
            in_progress_ent.list = false;
            in_progress_ent.dead = false;
            catcache_in_progress_stack = &mut in_progress_ent;

            // PG_TRY()
            dtp = toast_flatten_tuple(ntp, (*cache).cc_tupdesc);
            // PG_FINALLY()
            Assert!(catcache_in_progress_stack == &mut in_progress_ent as *mut CatCInProgress);
            catcache_in_progress_stack = save_in_progress;

            if in_progress_ent.dead {
                heap_freetuple(dtp);
                return null_mut();
            }
        } else {
            dtp = ntp;
        }

        // Allocate memory for CatCTup and the cached tuple in one go
        oldcxt = MemoryContextSwitchTo(CacheMemoryContext);

        ct = palloc(core::mem::size_of::<CatCTup>() + MAXIMUM_ALIGNOF + (*dtp).t_len as usize)
            as *mut CatCTup;
        (*ct).tuple.t_len = (*dtp).t_len;
        (*ct).tuple.t_self = (*dtp).t_self;
        (*ct).tuple.t_tableOid = (*dtp).t_tableOid;
        (*ct).tuple.t_data =
            MAXALIGN((ct as *mut c_char).add(core::mem::size_of::<CatCTup>()) as usize)
                as HeapTupleHeader;
        // copy tuple contents
        core::ptr::copy_nonoverlapping(
            (*dtp).t_data as *const c_char,
            (*ct).tuple.t_data as *mut c_char,
            (*dtp).t_len as usize,
        );
        MemoryContextSwitchTo(oldcxt);

        if dtp != ntp {
            heap_freetuple(dtp);
        }

        // extract keys - they'll point into the tuple if not by-value
        for i in 0..(*cache).cc_nkeys as usize {
            let atp: Datum;
            let mut isnull: bool = false;

            atp = heap_getattr(
                &mut (*ct).tuple,
                (*cache).cc_keyno[i],
                (*cache).cc_tupdesc,
                &mut isnull,
            );
            Assert!(!isnull);
            (*ct).keys[i] = atp;
        }
    } else {
        // Set up keys for a negative cache entry
        oldcxt = MemoryContextSwitchTo(CacheMemoryContext);
        ct = palloc(core::mem::size_of::<CatCTup>()) as *mut CatCTup;

        // Store keys - they'll point into separately allocated memory if not
        // by-value.
        CatCacheCopyKeys(
            (*cache).cc_tupdesc,
            (*cache).cc_nkeys,
            (*cache).cc_keyno.as_mut_ptr(),
            arguments,
            (*ct).keys.as_mut_ptr(),
        );
        MemoryContextSwitchTo(oldcxt);
    }

    // Finish initializing the CatCTup header, and add it to the cache's
    // linked list and counts.
    (*ct).ct_magic = CT_MAGIC;
    (*ct).my_cache = cache;
    (*ct).c_list = null_mut();
    (*ct).refcount = 0; // for the moment
    (*ct).dead = false;
    (*ct).negative = ntp.is_null();
    (*ct).hash_value = hashValue;

    dlist_push_head((*cache).cc_bucket.add(hashIndex as usize), &mut (*ct).cache_elem);

    (*cache).cc_ntup += 1;
    (*CacheHdr).ch_ntup += 1;

    // If the hash table has become too full, enlarge the buckets array. Quite
    // arbitrarily, we enlarge when fill factor > 2.
    if (*cache).cc_ntup > (*cache).cc_nbuckets * 2 {
        RehashCatCache(cache);
    }

    ct
}

// Helper routine that frees keys stored in the keys array.
unsafe fn CatCacheFreeKeys(tupdesc: TupleDesc, nkeys: c_int, attnos: *mut c_int, keys: *mut Datum) {
    for i in 0..nkeys as usize {
        let attnum = *attnos.add(i);
        let att: Form_pg_attribute;

        // system attribute are not supported in caches
        Assert!(attnum > 0);

        att = TupleDescAttr(tupdesc, attnum - 1);

        if !(*att).attbyval {
            pfree(DatumGetPointer(*keys.add(i)));
        }
    }
}

// Helper routine that copies the keys in the srckeys array into the dstkeys
// one, guaranteeing that the datums are fully allocated in the current memory
// context.
unsafe fn CatCacheCopyKeys(
    tupdesc: TupleDesc,
    nkeys: c_int,
    attnos: *mut c_int,
    srckeys: *mut Datum,
    dstkeys: *mut Datum,
) {
    // XXX: memory and lookup performance could possibly be improved by
    // storing all keys in one allocation.

    for i in 0..nkeys as usize {
        let attnum = *attnos.add(i);
        let att = TupleDescAttr(tupdesc, attnum - 1);
        let mut src = *srckeys.add(i);
        let mut srcname: NameData = core::mem::zeroed();

        // Must be careful in case the caller passed a C string where a NAME
        // is wanted: convert the given argument to a correctly padded NAME.
        if (*att).atttypid == NAMEOID {
            namestrcpy(&mut srcname, DatumGetCString(src));
            src = NameGetDatum(&srcname);
        }

        *dstkeys.add(i) = datumCopy(src, (*att).attbyval, (*att).attlen as c_int);
    }
}

//	PrepareToInvalidateCacheTuple()
//
//	Given a tuple belonging to the specified relation, find all catcaches it
//	could be in, compute the correct hash value for each such catcache, and
//	call the specified function to record the cache id and hash value in
//	inval.c's lists.
pub unsafe fn PrepareToInvalidateCacheTuple(
    relation: Relation,
    tuple: HeapTuple,
    newtuple: HeapTuple,
    function: Option<unsafe fn(c_int, uint32, Oid, *mut c_void)>,
    context: *mut c_void,
) {
    let mut iter = core::mem::zeroed::<slist_iter>();
    let reloid: Oid;

    // CACHE_elog(DEBUG2, "PrepareToInvalidateCacheTuple: called");

    // sanity checks
    Assert!(RelationIsValid(relation));
    Assert!(HeapTupleIsValid(tuple));
    Assert!(PointerIsValid(function.map_or(null(), |f| f as *const c_void)));
    Assert!(!CacheHdr.is_null());

    reloid = RelationGetRelid(relation);

    // for each cache
    //    if the cache contains tuples from the specified relation
    //        compute the tuple's hash value(s) in this cache,
    //        and call the passed function to register the information.
    slist_foreach!(iter, &mut (*CacheHdr).ch_caches, {
        let ccp = slist_container!(CatCache, cc_next, iter.cur);
        let hashvalue: uint32;
        let dbid: Oid;

        if (*ccp).cc_reloid != reloid {
            continue;
        }

        // Just in case cache hasn't finished initialization yet...
        ConditionalCatalogCacheInitializeCache(ccp);

        hashvalue = CatalogCacheComputeTupleHashValue(ccp, (*ccp).cc_nkeys, tuple);
        dbid = if (*ccp).cc_relisshared { 0 as Oid } else { MyDatabaseId };

        (function.unwrap())((*ccp).id, hashvalue, dbid, context);

        if !newtuple.is_null() {
            let newhashvalue: uint32;

            newhashvalue = CatalogCacheComputeTupleHashValue(ccp, (*ccp).cc_nkeys, newtuple);

            if newhashvalue != hashvalue {
                (function.unwrap())((*ccp).id, newhashvalue, dbid, context);
            }
        }
    });
}

// ResourceOwner callbacks

unsafe fn ResOwnerReleaseCatCache(res: Datum) {
    ReleaseCatCacheWithOwner(DatumGetPointer(res) as HeapTuple, null_mut());
}

unsafe fn ResOwnerPrintCatCache(res: Datum) -> *mut c_char {
    let tuple = DatumGetPointer(res) as HeapTuple;
    let ct = (tuple as *mut c_char).sub(core::mem::offset_of!(CatCTup, tuple)) as *mut CatCTup;

    // Safety check to ensure we were handed a cache entry
    Assert!((*ct).ct_magic == CT_MAGIC);

    // psprintf("cache %s (%d), tuple %u/%u has count %d", ...)
    let _ = (
        (*(*ct).my_cache).cc_relname,
        (*(*ct).my_cache).id,
        ItemPointerGetBlockNumber(&(*tuple).t_self as *const _ as *const c_void),
        ItemPointerGetOffsetNumber(&(*tuple).t_self as *const _ as *const c_void),
        (*ct).refcount,
    );
    psprintf_2(c"cache".as_ptr())
}

unsafe fn ResOwnerReleaseCatCacheList(res: Datum) {
    ReleaseCatCacheListWithOwner(DatumGetPointer(res) as *mut CatCList, null_mut());
}

unsafe fn ResOwnerPrintCatCacheList(res: Datum) -> *mut c_char {
    let list = DatumGetPointer(res) as *mut CatCList;

    // psprintf("cache %s (%d), list %p has count %d", ...)
    let _ = (
        (*(*list).my_cache).cc_relname,
        (*(*list).my_cache).id,
        list,
        (*list).refcount,
    );
    psprintf_2(c"cache".as_ptr())
}

// lfirst helper (pg_list.h)
unsafe fn lfirst(cell: *mut ListCell) -> *mut c_void {
    (*cell).ptr_value
}
