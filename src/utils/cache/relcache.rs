//! relcache.rs
//!   POSTGRES relation descriptor cache code
//!
//! Translated 1:1 from postgres/src/backend/utils/cache/relcache.c
//!
//! INTERFACE ROUTINES
//!   RelationCacheInitialize          - initialize relcache (to empty)
//!   RelationCacheInitializePhase2    - initialize shared-catalog entries
//!   RelationCacheInitializePhase3    - finish initializing relcache
//!   RelationIdGetRelation            - get a reldesc by relation id
//!   RelationClose                    - close an open relation
//!
//! NOTES
//!   The following code contains many undocumented hacks.  Please be
//!   careful....
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/utils/cache/relcache.c

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(unreachable_patterns)]

use crate::prelude::*;
use IndexAttrBitmapKind::*;

// Real types from their canonical homes.
use crate::utils::rel::{LockInfoData, LockRelId, Relation, RelationData};
use crate::access::common::tupdesc::{TupleDesc, TupleDescData};
use crate::catalog::pg_class::{Form_pg_class, FormData_pg_class};
use crate::utils::hash::dynahash::{HASHCTL, HTAB, HASH_SEQ_STATUS};
use crate::utils::mmgr::mcxt::CacheMemoryContext;
use crate::access::htup_details::{HeapTuple, HeapTupleData};
use crate::nodes::pg_list::List;
use crate::nodes::bitmapset::Bitmapset;
use crate::storage::lockdefs::AccessShareLock;
use crate::miscadmin::IsBootstrapProcessingMode;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const RELCACHE_INIT_FILEMAGIC: c_int = 0x573266; /* version ID value */

/// Maximum size for eoxact_list[] before we fall back to full scan.
const MAX_EOXACT_LIST: usize = 32;

/// Initial size of the RelationIdCache hashtable.
const INITRELCACHESIZE: c_int = 400;

// ---------------------------------------------------------------------------
// Local stub types (not-yet-ported pointee types kept as c_void pointers)
// ---------------------------------------------------------------------------

/// access/genam.h: SysScanDesc
// TODO(pg-port): access/index/genam.c not fully ported.
pub type SysScanDesc = *mut c_void;

/// utils/catcache.h: Snapshot
// TODO(pg-port): utils/snapmgr.c not ported.
pub type Snapshot = *mut c_void;

/// access/sdir.h: StrategyNumber
pub type StrategyNumber = uint16;

/// catalog/pg_am.h: Form_pg_am
// TODO(pg-port): catalog/pg_am.rs form pointer.
pub type Form_pg_am = *mut c_void;

/// catalog/pg_index.h: Form_pg_index (real type from canonical home)
pub use crate::catalog::pg_index::Form_pg_index;

/// catalog/pg_opclass.h: Form_pg_opclass
// TODO(pg-port): catalog/pg_opclass.c not ported.
pub type Form_pg_opclass = *mut c_void;

/// catalog/pg_amproc.h: Form_pg_amproc
// TODO(pg-port): catalog/pg_amproc.c not ported.
pub type Form_pg_amproc = *mut c_void;

/// catalog/pg_rewrite.h: Form_pg_rewrite
// TODO(pg-port): catalog/pg_rewrite.c not ported.
pub type Form_pg_rewrite = *mut c_void;

/// catalog/pg_attrdef.h: Form_pg_attrdef
// TODO(pg-port): catalog/pg_attrdef.c not ported.
pub type Form_pg_attrdef = *mut c_void;

/// catalog/pg_constraint.h: Form_pg_constraint
// TODO(pg-port): catalog/pg_constraint.c not ported.
pub type Form_pg_constraint = *mut c_void;

/// catalog/pg_publication.h: Form_pg_publication
// TODO(pg-port): catalog/pg_publication.c not ported.
pub type Form_pg_publication = *mut c_void;

/// catalog/pg_statistic_ext.h: Form_pg_statistic_ext
// TODO(pg-port): catalog/pg_statistic_ext.c not ported.
pub type Form_pg_statistic_ext = *mut c_void;

/// utils/array.h: ArrayType
// TODO(pg-port): utils/adt/array.c not ported.
pub type ArrayType = *mut c_void;

/// rewrite/rewriteSupport.h: RuleLock / RewriteRule
// TODO(pg-port): rewrite/rewriteDefine.c not ported.
pub type RuleLock = c_void;
pub type RewriteRule = c_void;

/// commands/policy.h: RowSecurityPolicy
// TODO(pg-port): commands/policy.c not ported.
pub type RowSecurityPolicy = c_void;
// RowSecurityDesc has a real struct def below (in Part 8).

/// access/tupdesc.h: TupleConstr / AttrMissing / CompactAttribute
// TODO(pg-port): access/common/tupdesc.rs partially ported.
pub type TupleConstr = c_void;
pub type AttrMissing = c_void;
pub type CompactAttribute = c_void;
// AttrDefault and ConstrCheck have real struct defs below (in Part 8).

/// catalog/pg_index.h: oidvector / int2vector
// TODO(pg-port): catalog/pg_type.c not ported.
pub type oidvector = c_void;
pub type int2vector = c_void;

/// access/index/amapi.h: IndexAmRoutine (opaque here; real home is utils/rel.rs dep)
// TODO(pg-port): we use *mut c_void for the opaque field pointer here.
// The real IndexAmRoutine is in crate::access::index::amapi::IndexAmRoutine.
pub type SMgrRelation = *mut c_void;

/// access/tableam.h: TableAmRoutine
// TODO(pg-port): access/table/tableam.c not ported.
pub type TableAmRoutine = *mut c_void;

/// nodes/primnodes.h: Datum
pub type Datum = usize;

/// nodes/primnodes.h: Node
pub type Node = c_void;

// PublicationDesc has a real struct def below (in Part 8).

/// utils/resowner.h: ResourceOwner
// TODO(pg-port): utils/resowner.c not ported.
pub type ResourceOwner = *mut c_void;

/// utils/resowner.h: ResourceOwnerDesc
// TODO(pg-port): stub struct.
pub type ResourceOwnerDesc = c_void;

/// storage/lwlock.h: LWLock mode
// TODO(pg-port): storage/lwlock.c not ported.
pub type LWLockMode = c_int;
pub const LW_EXCLUSIVE: LWLockMode = 1;

/// access/xact.h: SubTransactionId
pub type SubTransactionId = uint32;
pub const InvalidSubTransactionId: SubTransactionId = 0;

/// access/transam.h: TransactionId
pub type TransactionId = uint32;
pub const InvalidTransactionId: TransactionId = 0;

/// catalog/pg_proc.h: RegProcedure
pub type RegProcedure = Oid;

/// access/stratnum.h: InvalidStrategy
pub const InvalidStrategy: StrategyNumber = 0;

/// access/sysattr.h: FirstLowInvalidHeapAttributeNumber
// TODO(pg-port): access/sysattr.c not ported.
pub const FirstLowInvalidHeapAttributeNumber: c_int = -8;

/// pg_config_manual.h: MAXPGPATH
pub const MAXPGPATH: usize = 1024;

/// catalog/indexing.h: various index OIDs used during bootstrapping
// TODO(pg-port): replace with real constants once catalog/indexing.c is ported.
pub const ClassOidIndexId: Oid = 2662;
pub const AttributeRelidNumIndexId: Oid = 2677;
pub const IndexRelidIndexId: Oid = 2678;
pub const OpclassOidIndexId: Oid = 2673;
pub const AccessMethodProcedureIndexId: Oid = 2655;
pub const RewriteRelRulenameIndexId: Oid = 2693;
pub const TriggerRelidNameIndexId: Oid = 2694;
pub const DatabaseNameIndexId: Oid = 2671;
pub const DatabaseOidIndexId: Oid = 2672;
pub const AuthIdRolnameIndexId: Oid = 2676;
pub const AuthIdOidIndexId: Oid = 2696;
pub const AuthMemMemRoleIndexId: Oid = 2694; // placeholder
pub const SharedSecLabelObjectIndexId: Oid = 3593;
pub const AttrDefaultIndexId: Oid = 2656;
pub const ConstraintRelidTypidNameIndexId: Oid = 2664;
pub const StatisticExtRelidIndexId: Oid = 3380;
pub const IndexIndrelidIndexId: Oid = 2678;
pub const OpclassOidIndexId2: Oid = 2673;
pub const AccessMethodProcedureIndexId2: Oid = 2655;
pub const INDEXRELID: c_int = 41;
pub const AMOID: c_int = 42;
pub const RELOID: c_int = 43;
pub const PUBLICATIONOID: c_int = 44;

/// catalog/pg_class.h: RelationRelationId etc.
// TODO(pg-port): replace with real OIDs from catalog/pg_class.rs.
pub const RelationRelationId: Oid = 1259;
pub const AttributeRelationId: Oid = 1249;
pub const IndexRelationId: Oid = 2610;
pub const RewriteRelationId: Oid = 2618;
pub const TriggerRelationId: Oid = 2620;
pub const ConstraintRelationId: Oid = 2606;
pub const AttrDefaultRelationId: Oid = 2604;
pub const OperatorClassRelationId: Oid = 2616;
pub const AccessMethodProcedureRelationId: Oid = 2603;
pub const StatisticExtRelationId: Oid = 3381;
pub const DatabaseRelationId: Oid = 1262;
pub const AuthIdRelationId: Oid = 1260;
pub const AuthMemRelationId: Oid = 1261;
pub const TypeRelationId: Oid = 1247;
pub const ProcedureRelationId: Oid = 1255;
pub const SharedSecLabelRelationId: Oid = 3592;

pub const DatabaseRelation_Rowtype_Id: Oid = 0; // placeholder
pub const AuthIdRelation_Rowtype_Id: Oid = 0;
pub const AuthMemRelation_Rowtype_Id: Oid = 0;
pub const SharedSecLabelRelation_Rowtype_Id: Oid = 0;
pub const SubscriptionRelation_Rowtype_Id: Oid = 0;
pub const RelationRelation_Rowtype_Id: Oid = 0;
pub const AttributeRelation_Rowtype_Id: Oid = 0;
pub const ProcedureRelation_Rowtype_Id: Oid = 0;
pub const TypeRelation_Rowtype_Id: Oid = 0;

pub const Natts_pg_database: c_int = 0;
pub const Natts_pg_authid: c_int = 0;
pub const Natts_pg_auth_members: c_int = 0;
pub const Natts_pg_shseclabel: c_int = 0;
pub const Natts_pg_subscription: c_int = 0;
pub const Natts_pg_class: c_int = 0;
pub const Natts_pg_attribute: c_int = 0;
pub const Natts_pg_proc: c_int = 0;
pub const Natts_pg_type: c_int = 0;
pub const Natts_pg_index: c_int = 0;

/// catalog/pg_am.h constants
// TODO(pg-port): catalog/pg_am.rs not ported.
pub const HEAP_TABLE_AM_OID: Oid = 2;
pub const F_HEAP_TABLEAM_HANDLER: RegProcedure = 0; // placeholder

/// catalog/namespace.h
// TODO(pg-port): catalog/namespace.c not ported.
pub const PG_CATALOG_NAMESPACE: Oid = 11;
pub const GLOBALTABLESPACE_OID: Oid = 1664;
pub const RECORDOID: Oid = 2249;

/// pg_type.h
pub const OIDOID: Oid = 26;

/// binary_upgrade.h
// TODO(pg-port): catalog/binary_upgrade.c not ported.
pub static mut IsBinaryUpgrade: bool = false;
pub static mut binary_upgrade_next_index_pg_class_relfilenumber: Oid = 0;
pub static mut binary_upgrade_next_heap_pg_class_relfilenumber: Oid = 0;

/// ScanKeyData (access/skey.h)
// TODO(pg-port): access/common/scankey.rs not ported.
pub type ScanKeyData = c_void;

/// FormData_pg_attribute (access/tupdesc.h)
// TODO(pg-port): access/common/tupdesc.rs partially ported.
pub type FormData_pg_attribute = c_void;
pub type Form_pg_attribute = *mut c_void;

/// AttrNumber
pub type AttrNumber = int16;

/// Size (postgres.h)
pub type Size = usize;

/// pg_cmp_s16 (common/int.h)
#[inline]
fn pg_cmp_s16(a: int16, b: int16) -> c_int {
    (a as c_int) - (b as c_int)
}

// ---------------------------------------------------------------------------
// Hardcoded tuple descriptors (contents generated by genbki.pl)
// In C these are static arrays.  Here we use zero-length placeholders since
// the actual genbki output would be generated code; real port must fill them.
// ---------------------------------------------------------------------------

/// Placeholder descriptor arrays.  TODO(pg-port): replace with genbki output.
// Use *const c_void null pointers to avoid Sync bound on [*const FormData_pg_attribute; 0].
const Desc_pg_class: *const FormData_pg_attribute = core::ptr::null();
const Desc_pg_attribute: *const FormData_pg_attribute = core::ptr::null();
const Desc_pg_proc: *const FormData_pg_attribute = core::ptr::null();
const Desc_pg_type: *const FormData_pg_attribute = core::ptr::null();
const Desc_pg_database: *const FormData_pg_attribute = core::ptr::null();
const Desc_pg_authid: *const FormData_pg_attribute = core::ptr::null();
const Desc_pg_auth_members: *const FormData_pg_attribute = core::ptr::null();
const Desc_pg_index: *const FormData_pg_attribute = core::ptr::null();
const Desc_pg_shseclabel: *const FormData_pg_attribute = core::ptr::null();
const Desc_pg_subscription: *const FormData_pg_attribute = core::ptr::null();

// ---------------------------------------------------------------------------
// Hash tables that index the relation cache
//
// We used to index the cache by both name and OID, but now there
// is only an index by OID.
// ---------------------------------------------------------------------------

/// Entry type for RelationIdCache hashtable.
#[repr(C)]
pub struct RelIdCacheEnt {
    pub reloid: Oid,
    pub reldesc: Relation,
}

static mut RelationIdCache: *mut HTAB = core::ptr::null_mut();

// ---------------------------------------------------------------------------
// Global flags
// ---------------------------------------------------------------------------

/// This flag is false until we have prepared the critical relcache entries
/// that are needed to do indexscans on the tables read by relcache building.
pub static mut criticalRelcachesBuilt: bool = false;

/// This flag is false until we have prepared the critical relcache entries
/// for shared catalogs (which are the tables needed for login).
pub static mut criticalSharedRelcachesBuilt: bool = false;

/// This counter counts relcache inval events received since backend startup
/// (but only for rels that are actually in cache).  Presently, we use it only
/// to detect whether data about to be written by write_relcache_init_file()
/// might already be obsolete.
static mut relcacheInvalsReceived: c_long = 0;

// ---------------------------------------------------------------------------
// in_progress_list -- stack of ongoing RelationBuildDesc() calls.
// CREATE INDEX CONCURRENTLY makes catalog changes under ShareUpdateExclusiveLock.
// It critically relies on each backend absorbing those changes no later than
// next transaction start.  Hence, RelationBuildDesc() loops until it finishes
// without accepting a relevant invalidation.
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct InProgressEnt {
    pub reloid: Oid,      /* OID of relation being built */
    pub invalidated: bool, /* whether an invalidation arrived for it */
}

static mut in_progress_list: *mut InProgressEnt = core::ptr::null_mut();
static mut in_progress_list_len: c_int = 0;
static mut in_progress_list_maxlen: c_int = 0;

// ---------------------------------------------------------------------------
// eoxact_list[] stores the OIDs of relations that (might) need AtEOXact
// cleanup work.  This list intentionally has limited size; if it overflows,
// we fall back to scanning the whole hashtable.
// EOXactListAdd() does not bother to prevent duplicate list entries, so the
// cleanup processing must be idempotent.
// ---------------------------------------------------------------------------

static mut eoxact_list: [Oid; MAX_EOXACT_LIST] = [0; MAX_EOXACT_LIST];
static mut eoxact_list_len: c_int = 0;
static mut eoxact_list_overflowed: bool = false;

#[inline]
unsafe fn EOXactListAdd(rel: Relation) {
    if eoxact_list_len < MAX_EOXACT_LIST as c_int {
        eoxact_list[eoxact_list_len as usize] = (*rel).rd_id;
        eoxact_list_len += 1;
    } else {
        eoxact_list_overflowed = true;
    }
}

// ---------------------------------------------------------------------------
// EOXactTupleDescArray stores TupleDescs that (might) need AtEOXact cleanup
// ---------------------------------------------------------------------------

static mut EOXactTupleDescArray: *mut TupleDesc = core::ptr::null_mut();
static mut NextEOXactTupleDescNum: c_int = 0;
static mut EOXactTupleDescArrayLen: c_int = 0;

// ---------------------------------------------------------------------------
// Special cache for opclass-related information
//
// Note: only default support procs get cached, ie, those with
// lefttype = righttype = opcintype.
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct OpClassCacheEnt {
    pub opclassoid: Oid,           /* lookup key: OID of opclass */
    pub valid: bool,               /* set true after successful fill-in */
    pub numSupport: StrategyNumber, /* max # of support procs (from pg_am) */
    pub opcfamily: Oid,            /* OID of opclass's family */
    pub opcintype: Oid,            /* OID of opclass's declared input type */
    pub supportProcs: *mut RegProcedure, /* OIDs of support procedures */
}

static mut OpClassCache: *mut HTAB = core::ptr::null_mut();

// ---------------------------------------------------------------------------
// ResourceOwner support for tracking relcache references
// ---------------------------------------------------------------------------

// Stubs for ResourceOwner callbacks (not yet ported).
// TODO(pg-port): utils/resowner.c not ported.
unsafe fn ResourceOwnerEnlarge(_owner: ResourceOwner) {
    // TODO(pg-port): utils/resowner.c not ported.
}
unsafe fn ResourceOwnerRemember(_owner: ResourceOwner, _res: Datum, _desc: *const ResourceOwnerDesc) {
    // TODO(pg-port): utils/resowner.c not ported.
}
unsafe fn ResourceOwnerForget(_owner: ResourceOwner, _res: Datum, _desc: *const ResourceOwnerDesc) {
    // TODO(pg-port): utils/resowner.c not ported.
}
static mut CurrentResourceOwner: ResourceOwner = core::ptr::null_mut();

// relref_resowner_desc is defined in C as a static struct; we reference it
// by address, so declare a zero-sized placeholder here.
// TODO(pg-port): replace with real ResourceOwnerDesc once resowner.c is ported.
// ResourceOwnerDesc is c_void so we use a raw extern static trick.
static mut relref_resowner_desc_storage: u8 = 0;
// SAFETY: relref_resowner_desc is only used by address, never dereferenced.
#[inline]
unsafe fn relref_resowner_desc_ptr() -> *const ResourceOwnerDesc {
    &raw const relref_resowner_desc_storage as *const ResourceOwnerDesc
}

#[inline]
unsafe fn ResourceOwnerRememberRelationRef(owner: ResourceOwner, rel: Relation) {
    ResourceOwnerRemember(owner, rel as Datum, relref_resowner_desc_ptr());
}

#[inline]
unsafe fn ResourceOwnerForgetRelationRef(owner: ResourceOwner, rel: Relation) {
    ResourceOwnerForget(owner, rel as Datum, relref_resowner_desc_ptr());
}

// ---------------------------------------------------------------------------
// Lookup hashtable helper macros (translated to inline functions)
// ---------------------------------------------------------------------------

/// Insert RELATION into RelationIdCache.  If an entry already exists for its
/// OID, replace_allowed must be true, and the old entry will be destroyed if
/// its refcount is zero, or a WARNING will be emitted if it is not.
unsafe fn RelationCacheInsert(relation: Relation, replace_allowed: bool) {
    let mut found: bool = false;
    // TODO(pg-port): hash_search is in utils/hash/dynahash.c -- use stub.
    let hentry = hash_search(
        RelationIdCache,
        &(*relation).rd_id as *const Oid as *const c_void,
        HASH_ENTER,
        &mut found,
    ) as *mut RelIdCacheEnt;
    if found {
        /* see comments in RelationBuildDesc and RelationBuildLocalRelation */
        let old_rel: Relation = (*hentry).reldesc;
        assert!(replace_allowed, "RelationCacheInsert: replace not allowed");
        (*hentry).reldesc = relation;
        if RelationHasReferenceCountZero(old_rel) {
            RelationDestroyRelation(old_rel, false);
        } else if !IsBootstrapProcessingMode() {
            // elog(WARNING, "leaking still-referenced relcache entry for \"%s\"",
            //      RelationGetRelationName(old_rel));
            eprintln!(
                "WARNING: leaking still-referenced relcache entry for \"{}\"",
                relation_get_name_str(old_rel)
            );
        }
    } else {
        (*hentry).reldesc = relation;
    }
}

unsafe fn RelationIdCacheLookup(id: Oid, relation: &mut Relation) {
    let hentry = hash_search(
        RelationIdCache,
        &id as *const Oid as *const c_void,
        HASH_FIND,
        core::ptr::null_mut(),
    ) as *mut RelIdCacheEnt;
    if !hentry.is_null() {
        *relation = (*hentry).reldesc;
    } else {
        *relation = core::ptr::null_mut();
    }
}

unsafe fn RelationCacheDelete(relation: Relation) {
    let hentry = hash_search(
        RelationIdCache,
        &(*relation).rd_id as *const Oid as *const c_void,
        HASH_REMOVE,
        core::ptr::null_mut(),
    ) as *mut RelIdCacheEnt;
    if hentry.is_null() {
        // elog(WARNING, "failed to delete relcache entry for OID %u", relation->rd_id);
        eprintln!(
            "WARNING: failed to delete relcache entry for OID {}",
            (*relation).rd_id
        );
    }
}

// ---------------------------------------------------------------------------
// Helper: get relation name as Rust string (for error messages).
// ---------------------------------------------------------------------------
unsafe fn relation_get_name_str(rel: Relation) -> String {
    if rel.is_null() || (*rel).rd_rel.is_null() {
        return "<null>".to_string();
    }
    // rd_rel->relname is a NameData [64]byte C-string.
    // TODO(pg-port): real NameData access via FormData_pg_class.relname field.
    "<relation>".to_string()
}

// ---------------------------------------------------------------------------
// Stubs for not-yet-ported callees
// ---------------------------------------------------------------------------

// -- memory management (utils/mmgr) --

/// TODO(pg-port): utils/mmgr/mcxt.c not fully ported.
unsafe fn MemoryContextSwitchTo(cxt: *mut c_void) -> *mut c_void {
    unimplemented!("TODO(pg-port): MemoryContextSwitchTo")
}
unsafe fn MemoryContextDelete(cxt: *mut c_void) {
    // TODO(pg-port): utils/mmgr/mcxt.c not ported.
}
unsafe fn MemoryContextDeleteChildren(cxt: *mut c_void) {
    // TODO(pg-port): utils/mmgr/mcxt.c not ported.
}
unsafe fn MemoryContextSetParent(cxt: *mut c_void, new_parent: *mut c_void) {
    // TODO(pg-port): utils/mmgr/mcxt.c not ported.
}
unsafe fn MemoryContextCopyAndSetIdentifier(_cxt: *mut c_void, _name: *const c_char) {
    // TODO(pg-port): utils/mmgr/mcxt.c not ported.
}
unsafe fn MemoryContextAlloc(cxt: *mut c_void, size: Size) -> *mut c_void {
    unimplemented!("TODO(pg-port): MemoryContextAlloc")
}
unsafe fn MemoryContextAllocZero(cxt: *mut c_void, size: Size) -> *mut c_void {
    unimplemented!("TODO(pg-port): MemoryContextAllocZero")
}
unsafe fn AllocSetContextCreate(
    _parent: *mut c_void,
    _name: *const c_char,
    _sizes: c_int,
) -> *mut c_void {
    unimplemented!("TODO(pg-port): AllocSetContextCreate")
}
unsafe fn MemoryContextStrdup(cxt: *mut c_void, s: *const c_char) -> *mut c_char {
    unimplemented!("TODO(pg-port): MemoryContextStrdup")
}
/// TODO(pg-port): utils/mmgr/mcxt.c not ported.
unsafe fn CreateCacheMemoryContext() {
    // TODO(pg-port): utils/mmgr/mcxt.c not ported.
}
const ALLOCSET_SMALL_SIZES: c_int = 1;
const ALLOCSET_DEFAULT_SIZES: c_int = 2;

// palloc / pfree
unsafe fn palloc(size: Size) -> *mut c_void {
    unimplemented!("TODO(pg-port): palloc")
}
unsafe fn palloc0(size: Size) -> *mut c_void {
    unimplemented!("TODO(pg-port): palloc0")
}
unsafe fn pfree(ptr: *mut c_void) {
    // TODO(pg-port): palloc.c not ported.
}
unsafe fn repalloc(ptr: *mut c_void, size: Size) -> *mut c_void {
    unimplemented!("TODO(pg-port): repalloc")
}

// -- catalog access (access/index/genam.h) --

/// TODO(pg-port): access/index/genam.c ScanKeyInit.
unsafe fn ScanKeyInit(
    _skey: *mut ScanKeyData,
    _attnum: c_int,
    _strategy: StrategyNumber,
    _func: RegProcedure,
    _argument: Datum,
) {
    // TODO(pg-port): access/common/scankey.c not ported.
}
/// TODO(pg-port): access/index/genam.c systable_beginscan.
unsafe fn systable_beginscan(
    _heapRelation: Relation,
    _indexId: Oid,
    _indexOK: bool,
    _snapshot: Snapshot,
    _nkeys: c_int,
    _key: *mut ScanKeyData,
) -> SysScanDesc {
    unimplemented!("TODO(pg-port): systable_beginscan")
}
/// TODO(pg-port): access/index/genam.c systable_getnext.
unsafe fn systable_getnext(_scan: SysScanDesc) -> HeapTuple {
    unimplemented!("TODO(pg-port): systable_getnext")
}
/// TODO(pg-port): access/index/genam.c systable_endscan.
unsafe fn systable_endscan(_scan: SysScanDesc) {
    // TODO(pg-port): access/index/genam.c not ported.
}

// -- table access (access/table/table.h) --

/// TODO(pg-port): access/table/table.c not ported.
unsafe fn table_open(_relid: Oid, _lockmode: c_int) -> Relation {
    unimplemented!("TODO(pg-port): table_open")
}
/// TODO(pg-port): access/table/table.c not ported.
unsafe fn table_close(_rel: Relation, _lockmode: c_int) {
    // TODO(pg-port): access/table/table.c not ported.
}

// -- heap tuples (access/heap/heapam.h) --

/// TODO(pg-port): access/heap/heapam.c not ported.
unsafe fn heap_copytuple(_tuple: HeapTuple) -> HeapTuple {
    unimplemented!("TODO(pg-port): heap_copytuple")
}
/// TODO(pg-port): access/heap/heapam.c not ported.
unsafe fn heap_freetuple(_tuple: HeapTuple) {
    // TODO(pg-port): access/heap/heapam.c not ported.
}
/// TODO(pg-port): access/heap/heapam.c heap_getattr.
unsafe fn heap_getattr(
    _tuple: HeapTuple,
    _attnum: c_int,
    _tupdesc: TupleDesc,
    _isnull: *mut bool,
) -> Datum {
    unimplemented!("TODO(pg-port): heap_getattr")
}
/// TODO(pg-port): access/heap/heapam.c heap_attisnull.
unsafe fn heap_attisnull(_tuple: HeapTuple, _attnum: c_int, _tupdesc: TupleDesc) -> bool {
    unimplemented!("TODO(pg-port): heap_attisnull")
}
/// TODO(pg-port): access/heap/heaptuple.c fastgetattr.
unsafe fn fastgetattr(
    _tup: HeapTuple,
    _attnum: c_int,
    _tupleDesc: TupleDesc,
    _isnull: *mut bool,
) -> Datum {
    unimplemented!("TODO(pg-port): fastgetattr")
}

// -- GETSTRUCT macro (access/htup_details.h) --

/// TODO(pg-port): GETSTRUCT(tup) = pointer to tuple data after header.
unsafe fn GETSTRUCT(tup: HeapTuple) -> *mut c_void {
    unimplemented!("TODO(pg-port): GETSTRUCT")
}
unsafe fn HeapTupleIsValid(tup: HeapTuple) -> bool {
    !tup.is_null()
}
unsafe fn HeapTupleHeaderSetXmin(_hdr: *mut c_void, _xid: TransactionId) {
    // TODO(pg-port): access/htup_details.c not ported.
}
unsafe fn HeapTupleHeaderGetXmin(_hdr: *mut c_void) -> TransactionId {
    0 // TODO(pg-port)
}
/// Size of HeapTupleData header (access/htup.h: HEAPTUPLESIZE)
pub const HEAPTUPLESIZE: usize = core::mem::size_of::<HeapTupleData>();

// -- tupdesc helpers (access/tupdesc.h) --

/// TODO(pg-port): access/common/tupdesc.c CreateTemplateTupleDesc.
unsafe fn CreateTemplateTupleDesc(_natts: c_int) -> TupleDesc {
    unimplemented!("TODO(pg-port): CreateTemplateTupleDesc")
}
/// TODO(pg-port): access/common/tupdesc.c CreateTupleDescCopy.
unsafe fn CreateTupleDescCopy(_tupdesc: TupleDesc) -> TupleDesc {
    unimplemented!("TODO(pg-port): CreateTupleDescCopy")
}
/// TODO(pg-port): access/common/tupdesc.c equalTupleDescs.
unsafe fn equalTupleDescs(_a: TupleDesc, _b: TupleDesc) -> bool {
    unimplemented!("TODO(pg-port): equalTupleDescs")
}
/// TODO(pg-port): access/common/tupdesc.c FreeTupleDesc.
unsafe fn FreeTupleDesc(_tupdesc: TupleDesc) {
    // TODO(pg-port): access/common/tupdesc.c not ported.
}
/// TODO(pg-port): access/common/tupdesc.c TupleDescAttr.
unsafe fn TupleDescAttr(_tupdesc: TupleDesc, _i: c_int) -> Form_pg_attribute {
    unimplemented!("TODO(pg-port): TupleDescAttr")
}
/// TODO(pg-port): access/tupdesc_details.h TupleDescCompactAttr.
unsafe fn TupleDescCompactAttr(_tupdesc: TupleDesc, _i: c_int) -> *mut CompactAttribute {
    unimplemented!("TODO(pg-port): TupleDescCompactAttr")
}
/// TODO(pg-port): access/common/tupdesc.c populate_compact_attribute.
unsafe fn populate_compact_attribute(_tupdesc: TupleDesc, _i: c_int) {
    // TODO(pg-port): access/common/tupdesc.c not ported.
}

// -- pg_class helpers --
/// CLASS_TUPLE_SIZE (catalog/pg_class.h)
pub const CLASS_TUPLE_SIZE: usize = 0; // TODO(pg-port): replace with sizeof(FormData_pg_class)

// -- lock manager (storage/lmgr.h) --

/// TODO(pg-port): storage/lmgr.c not ported.
unsafe fn LockRelationOid(_relid: Oid, _lockmode: c_int) {
    // TODO(pg-port): storage/lmgr.c not ported.
}
unsafe fn UnlockRelationOid(_relid: Oid, _lockmode: c_int) {
    // TODO(pg-port): storage/lmgr.c not ported.
}
unsafe fn RelationInitLockInfo(_relation: Relation) {
    // TODO(pg-port): storage/lmgr.c not ported.
}
unsafe fn UnlockTuple(_rel: Relation, _tid: *const ItemPointerData, _lockmode: c_int) {
    // TODO(pg-port): storage/lmgr.c not ported.
}
// InplaceUpdateTupleLock and RowExclusiveLock defined in lockdefs; use LOCKMODE type below.
// (Real defs added after LOCKMODE type is introduced further down.)

// -- smgr (storage/smgr.h) --

/// TODO(pg-port): storage/smgr/smgr.c not ported.
unsafe fn RelationCloseSmgr(_relation: Relation) {
    // TODO(pg-port): storage/smgr.c not ported.
}
unsafe fn smgropen(_rlocator: crate::common::blkreftable::RelFileLocator, _backend: c_int) -> SMgrRelation {
    unimplemented!("TODO(pg-port): smgropen")
}
unsafe fn smgrdounlinkall(_srels: *mut SMgrRelation, _nrels: c_int, _isRedo: bool) {
    // TODO(pg-port): storage/smgr.c not ported.
}
unsafe fn smgrclose(_srel: SMgrRelation) {
    // TODO(pg-port): storage/smgr.c not ported.
}
unsafe fn smgrreleaseall() {
    // TODO(pg-port): storage/smgr.c not ported.
}

// -- pgstat (pgstat.h) --

/// TODO(pg-port): pgstat_relation.c not ported.
unsafe fn pgstat_unlink_relation(_relation: Relation) {
    // TODO(pg-port): pgstat.c not ported.
}

// -- trigger (commands/trigger.h) --

/// TODO(pg-port): commands/trigger.c not ported.
unsafe fn RelationBuildTriggers(_relation: Relation) {
    unimplemented!("TODO(pg-port): RelationBuildTriggers (commands/trigger.c not ported)")
}
unsafe fn FreeTriggerDesc(_trigdesc: *mut c_void) {
    // TODO(pg-port): commands/trigger.c not ported.
}

// -- row security (rewrite/rowsecurity.h) --

/// TODO(pg-port): rewrite/rowsecurity.c not ported.
unsafe fn RelationBuildRowSecurity(_relation: Relation) {
    unimplemented!("TODO(pg-port): RelationBuildRowSecurity (rewrite/rowsecurity.c not ported)")
}

// -- rel options (access/reloptions.h) --

/// TODO(pg-port): access/common/reloptions.c not ported.
type amoptions_function = *mut c_void;
unsafe fn extractRelOptions(
    _tuple: HeapTuple,
    _tupdesc: TupleDesc,
    _amoptsfn: amoptions_function,
) -> *mut bytea {
    unimplemented!("TODO(pg-port): extractRelOptions")
}
pub type bytea = c_void;
unsafe fn VARSIZE(_val: *const bytea) -> usize {
    0 // TODO(pg-port)
}

// -- index AM (access/index/amapi.h) --

/// TODO(pg-port): access/index/amapi.c not ported.
unsafe fn GetIndexAmRoutine(
    _handler: RegProcedure,
) -> *mut crate::access::index::amapi::IndexAmRoutine {
    unimplemented!("TODO(pg-port): GetIndexAmRoutine")
}
unsafe fn GetTableAmRoutine(_handler: RegProcedure) -> TableAmRoutine {
    unimplemented!("TODO(pg-port): GetTableAmRoutine")
}
unsafe fn GetHeapamTableAmRoutine() -> TableAmRoutine {
    unimplemented!("TODO(pg-port): GetHeapamTableAmRoutine")
}
/*
 * Fill in the TableAmRoutine for a relation
 *
 * relation's rd_amhandler must be valid already.
 */
unsafe fn InitTableAmRoutine(relation: Relation) {
    (*relation).rd_tableam =
        crate::access::table::tableamapi::GetTableAmRoutine((*relation).rd_amhandler)
            as *const c_void;
}
// RelationGetIndexAttOptions stub removed -- real impl in Part 8 below.
unsafe fn index_opclass_options(
    _rel: Relation,
    _attnum: c_int,
    _attoptions: Datum,
    _validate: bool,
) -> *mut bytea {
    unimplemented!("TODO(pg-port): index_opclass_options")
}
unsafe fn get_attoptions(_relid: Oid, _attnum: c_int) -> Datum {
    0 // TODO(pg-port): utils/lsyscache.c not ported.
}

// -- table_relation_set_new_filelocator --

/// TODO(pg-port): access/table/tableam.c not ported.
unsafe fn table_relation_set_new_filelocator(
    _rel: Relation,
    _newrlocator: *const crate::common::blkreftable::RelFileLocator,
    _persistence: c_char,
    _freezeXid: *mut TransactionId,
    _minmulti: *mut c_uint,
) {
    unimplemented!("TODO(pg-port): table_relation_set_new_filelocator")
}
unsafe fn RelationCreateStorage(
    _rlocator: crate::common::blkreftable::RelFileLocator,
    _persistence: c_char,
    _doFsync: bool,
) -> SMgrRelation {
    unimplemented!("TODO(pg-port): RelationCreateStorage")
}
unsafe fn RelationDropStorage(_relation: Relation) {
    // TODO(pg-port): catalog/storage.c not ported.
}

// -- syscache (utils/syscache.h) --

/// TODO(pg-port): utils/cache/syscache.c not ported.
unsafe fn SearchSysCache1(_cacheval: c_int, _arg1: Datum) -> HeapTuple {
    unimplemented!("TODO(pg-port): SearchSysCache1")
}
unsafe fn SearchSysCacheLockedCopy1(_cacheval: c_int, _arg1: Datum) -> HeapTuple {
    unimplemented!("TODO(pg-port): SearchSysCacheLockedCopy1")
}
unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    // TODO(pg-port): utils/cache/syscache.c not ported.
}
unsafe fn InitCatalogCachePhase2() {
    // TODO(pg-port): utils/cache/syscache.c not ported.
}
unsafe fn RelationSupportsSysCache(_relid: Oid) -> bool {
    false // TODO(pg-port): utils/cache/syscache.c not ported.
}

// -- relation mapper (utils/relmapper.h) --

/// TODO(pg-port): utils/cache/relmapper.c not fully ported.
unsafe fn RelationMapOidToFilenumber(_relid: Oid, _shared: bool) -> RelFileNumber {
    unimplemented!("TODO(pg-port): RelationMapOidToFilenumber")
}
pub type RelFileNumber = Oid;
unsafe fn RelFileNumberIsValid(_num: RelFileNumber) -> bool {
    _num != 0
}
pub const InvalidRelFileNumber: RelFileNumber = 0;
unsafe fn RelationMapUpdateMap(_relid: Oid, _filenum: RelFileNumber, _shared: bool, _immediate: bool) {
    // TODO(pg-port): utils/cache/relmapper.c not ported.
}
unsafe fn RelationMapInitialize() {
    // TODO(pg-port): utils/cache/relmapper.c not ported.
}
unsafe fn RelationMapInitializePhase2() {
    // TODO(pg-port): utils/cache/relmapper.c not ported.
}
unsafe fn RelationMapInitializePhase3() {
    // TODO(pg-port): utils/cache/relmapper.c not ported.
}
unsafe fn RelationMapInvalidateAll() {
    // TODO(pg-port): utils/cache/relmapper.c not ported.
}

// -- dynahash (utils/hash/dynahash.h) --

/// TODO(pg-port): HASH_* action flags from utils/hash/dynahash.rs.
pub const HASH_ENTER: c_int = 1;
pub const HASH_FIND: c_int = 2;
pub const HASH_REMOVE: c_int = 3;
pub const HASH_ELEM: c_uint = 0x010;
pub const HASH_BLOBS: c_uint = 0x100;

/// TODO(pg-port): utils/hash/dynahash.c hash_create.
unsafe fn hash_create(
    _tabname: *const c_char,
    _nelem: c_long,
    _info: *const HASHCTL,
    _flags: c_uint,
) -> *mut HTAB {
    unimplemented!("TODO(pg-port): hash_create")
}
/// TODO(pg-port): utils/hash/dynahash.c hash_search.
unsafe fn hash_search(
    _hashp: *mut HTAB,
    _keyPtr: *const c_void,
    _action: c_int,
    _foundPtr: *mut bool,
) -> *mut c_void {
    unimplemented!("TODO(pg-port): hash_search")
}
/// TODO(pg-port): utils/hash/dynahash.c hash_seq_init.
unsafe fn hash_seq_init(_status: *mut HASH_SEQ_STATUS, _hashp: *mut HTAB) {
    unimplemented!("TODO(pg-port): hash_seq_init")
}
/// TODO(pg-port): utils/hash/dynahash.c hash_seq_search.
unsafe fn hash_seq_search(_status: *mut HASH_SEQ_STATUS) -> *mut c_void {
    unimplemented!("TODO(pg-port): hash_seq_search")
}
/// TODO(pg-port): utils/hash/dynahash.c hash_seq_term.
unsafe fn hash_seq_term(_status: *mut HASH_SEQ_STATUS) {
    unimplemented!("TODO(pg-port): hash_seq_term")
}
unsafe fn GetLockMethodLocalHash() -> *mut HTAB {
    unimplemented!("TODO(pg-port): GetLockMethodLocalHash")
}

// -- list helpers (nodes/pg_list.h) --

pub const NIL: *mut List = core::ptr::null_mut();
unsafe fn list_copy(_list: *mut List) -> *mut List {
    unimplemented!("TODO(pg-port): list_copy")
}
unsafe fn list_free(_list: *mut List) {
    // TODO(pg-port): nodes/list.c not ported.
}
unsafe fn list_free_deep(_list: *mut List) {
    // TODO(pg-port): nodes/list.c not ported.
}
unsafe fn list_length(_list: *const List) -> c_int {
    0 // TODO(pg-port)
}
unsafe fn lcons(_datum: *mut c_void, _list: *mut List) -> *mut List {
    unimplemented!("TODO(pg-port): lcons")
}
unsafe fn lappend(_list: *mut List, _datum: *mut c_void) -> *mut List {
    unimplemented!("TODO(pg-port): lappend")
}
unsafe fn lappend_oid(_list: *mut List, _datum: Oid) -> *mut List {
    unimplemented!("TODO(pg-port): lappend_oid")
}
unsafe fn lfirst(_lc: *mut c_void) -> *mut c_void {
    unimplemented!("TODO(pg-port): lfirst")
}
unsafe fn lfirst_oid(_lc: *mut c_void) -> Oid {
    unimplemented!("TODO(pg-port): lfirst_oid")
}
unsafe fn list_sort(_list: *mut List, _cmpfn: unsafe fn(*const c_void, *const c_void) -> c_int) {
    // TODO(pg-port): nodes/list.c not ported.
}
unsafe fn list_oid_cmp(_a: *const c_void, _b: *const c_void) -> c_int {
    0 // TODO(pg-port)
}
unsafe fn list_concat_unique_oid(_a: *mut List, _b: *mut List) -> *mut List {
    unimplemented!("TODO(pg-port): list_concat_unique_oid")
}
unsafe fn equal(_a: *const c_void, _b: *const c_void) -> bool {
    unimplemented!("TODO(pg-port): equal (nodes/equalfuncs.c not ported)")
}
unsafe fn copyObject(_obj: *mut c_void) -> *mut c_void {
    unimplemented!("TODO(pg-port): copyObject (nodes/copyfuncs.c not ported)")
}

// -- bitmapset (nodes/bitmapset.h) --

unsafe fn bms_copy(_a: *mut Bitmapset) -> *mut Bitmapset {
    unimplemented!("TODO(pg-port): bms_copy")
}
unsafe fn bms_free(_a: *mut Bitmapset) {
    // TODO(pg-port): nodes/bitmapset.c not ported.
}
unsafe fn bms_add_member(_a: *mut Bitmapset, _x: c_int) -> *mut Bitmapset {
    unimplemented!("TODO(pg-port): bms_add_member")
}

// -- Relation predicate macros (utils/rel.h) --

#[inline]
pub unsafe fn RelationIsValid(r: Relation) -> bool {
    !r.is_null()
}
#[inline]
pub unsafe fn RelationHasReferenceCountZero(r: Relation) -> bool {
    (*r).rd_refcnt == 0
}
#[inline]
pub unsafe fn RelationGetRelid(r: Relation) -> Oid {
    (*r).rd_id
}
#[inline]
pub unsafe fn RelationGetRelid_mut(r: Relation) -> &'static mut Oid {
    &mut (*r).rd_id
}
#[inline]
pub unsafe fn RelationGetForm(r: Relation) -> Form_pg_class {
    (*r).rd_rel
}
#[inline]
pub unsafe fn RelationGetNumberOfAttributes(r: Relation) -> c_int {
    if (*r).rd_rel.is_null() {
        return 0;
    }
    (*(*r).rd_rel).relnatts as c_int
}
#[inline]
pub unsafe fn RelationGetDescr(r: Relation) -> TupleDesc {
    (*r).rd_att
}
#[inline]
pub unsafe fn RelationGetNamespace(r: Relation) -> Oid {
    if (*r).rd_rel.is_null() {
        return 0;
    }
    (*(*r).rd_rel).relnamespace
}
#[inline]
pub unsafe fn RelationIsMapped(r: Relation) -> bool {
    !(*r).rd_rel.is_null() && (*(*r).rd_rel).relfilenode == InvalidRelFileNumber
}
#[inline]
pub unsafe fn RelationIsPermanent(r: Relation) -> bool {
    !(*r).rd_rel.is_null() && (*(*r).rd_rel).relpersistence == RELPERSISTENCE_PERMANENT
}
#[inline]
pub unsafe fn RelationIsAccessibleInLogicalDecoding(_r: Relation) -> bool {
    false // TODO(pg-port)
}
#[inline]
pub unsafe fn RelationHasSecurityInvoker(_r: Relation) -> bool {
    false // TODO(pg-port)
}

// -- persistence / relkind constants (catalog/pg_class.h) --

pub const RELPERSISTENCE_PERMANENT: c_char = b'p' as c_char;
pub const RELPERSISTENCE_UNLOGGED: c_char = b'u' as c_char;
pub const RELPERSISTENCE_TEMP: c_char = b't' as c_char;

pub const RELKIND_RELATION: c_char = b'r' as c_char;
pub const RELKIND_INDEX: c_char = b'i' as c_char;
pub const RELKIND_SEQUENCE: c_char = b'S' as c_char;
pub const RELKIND_TOASTVALUE: c_char = b't' as c_char;
pub const RELKIND_VIEW: c_char = b'v' as c_char;
pub const RELKIND_MATVIEW: c_char = b'm' as c_char;
pub const RELKIND_PARTITIONED_TABLE: c_char = b'p' as c_char;
pub const RELKIND_PARTITIONED_INDEX: c_char = b'I' as c_char;

#[inline]
pub fn RELKIND_HAS_TABLE_AM(relkind: c_char) -> bool {
    relkind == RELKIND_RELATION
        || relkind == RELKIND_TOASTVALUE
        || relkind == RELKIND_MATVIEW
        || relkind == RELKIND_PARTITIONED_TABLE
}
#[inline]
pub fn RELKIND_HAS_STORAGE(relkind: c_char) -> bool {
    relkind == RELKIND_RELATION
        || relkind == RELKIND_INDEX
        || relkind == RELKIND_SEQUENCE
        || relkind == RELKIND_TOASTVALUE
        || relkind == RELKIND_MATVIEW
}

pub const REPLICA_IDENTITY_DEFAULT: c_char = b'd' as c_char;
pub const REPLICA_IDENTITY_NOTHING: c_char = b'n' as c_char;
pub const REPLICA_IDENTITY_INDEX: c_char = b'i' as c_char;
pub const REPLICA_IDENTITY_FULL: c_char = b'f' as c_char;

// -- attnullability (access/tupdesc_details.h) --
pub const ATTNULLABLE_UNKNOWN: c_char = b'u' as c_char;
pub const ATTNULLABLE_VALID: c_char = b'v' as c_char;
pub const ATTNULLABLE_INVALID: c_char = b'i' as c_char;
pub const ATTNULLABLE_UNRESTRICTED: c_char = b'r' as c_char;
pub const ATTRIBUTE_GENERATED_STORED: c_char = b's' as c_char;
pub const ATTRIBUTE_GENERATED_VIRTUAL: c_char = b'v' as c_char;
pub const ATTRIBUTE_FIXED_PART_SIZE: usize = 0; // TODO(pg-port): sizeof(FormData_pg_attribute)

// -- catalog helpers --

unsafe fn IsCatalogRelation(_r: Relation) -> bool {
    false // TODO(pg-port): catalog/catalog.c not ported.
}
unsafe fn IsCatalogNamespace(_ns: Oid) -> bool {
    false // TODO(pg-port)
}
unsafe fn IsSystemRelation(_r: Relation) -> bool {
    false // TODO(pg-port)
}
unsafe fn IsSharedRelation(_relid: Oid) -> bool {
    false // TODO(pg-port): catalog/catalog.c not ported.
}
unsafe fn isTempOrTempToastNamespace(_ns: Oid) -> bool {
    false // TODO(pg-port): catalog/namespace.c not ported.
}
unsafe fn ProcNumberForTempRelations() -> c_int {
    0 // TODO(pg-port)
}
unsafe fn GetTempNamespaceProcNumber(_ns: Oid) -> c_int {
    0 // TODO(pg-port)
}
pub const INVALID_PROC_NUMBER: c_int = -1;
pub const BOOTSTRAP_SUPERUSERID: Oid = 10;
pub const ENOENT: c_int = 2;
pub const NUM_CRITICAL_SHARED_RELS: c_int = 5;
pub const NUM_CRITICAL_SHARED_INDEXES: c_int = 6;
pub const NUM_CRITICAL_LOCAL_RELS: c_int = 4;
pub const NUM_CRITICAL_LOCAL_INDEXES: c_int = 7;

/// LOCKMODE -- lock manager mode (storage/lockdefs.h).
pub type LOCKMODE = crate::storage::lockdefs::LOCKMODE;

/// RelFileLocator from storage/relfilelocator.h.
pub use crate::common::blkreftable::RelFileLocator;
unsafe fn get_namespace_name(_ns: Oid) -> *mut c_char {
    core::ptr::null_mut() // TODO(pg-port): catalog/namespace.c not ported.
}
unsafe fn get_attname(_relid: Oid, _attnum: int16, _missing_ok: bool) -> *const c_char {
    core::ptr::null() // TODO(pg-port): utils/lsyscache.c not ported.
}
unsafe fn get_rel_namespace(_relid: Oid) -> Oid {
    0 // TODO(pg-port): utils/lsyscache.c not ported.
}
unsafe fn get_opcode(_opid: Oid) -> RegProcedure {
    0 // TODO(pg-port): utils/lsyscache.c not ported.
}
unsafe fn get_op_opfamily_strategy(_opid: Oid, _opfamily: Oid) -> StrategyNumber {
    0 // TODO(pg-port): utils/lsyscache.c not ported.
}
unsafe fn GetNewRelFileNumber(_tablespace: Oid, _pg_class: Relation, _persistence: c_char) -> RelFileNumber {
    unimplemented!("TODO(pg-port): GetNewRelFileNumber")
}
unsafe fn extractNotNullColumn(_htup: HeapTuple) -> AttrNumber {
    0 // TODO(pg-port)
}
unsafe fn DeconstructFkConstraintRow(
    _htup: HeapTuple,
    _nkeys: *mut c_int,
    _conkey: *mut c_void,
    _confkey: *mut c_void,
    _conpfeqop: *mut c_void,
    _a: *mut c_void,
    _b: *mut c_void,
    _c: *mut c_void,
    _d: *mut c_void,
) {
    // TODO(pg-port): catalog/pg_constraint.c not ported.
}

// -- OID-related helpers --

#[inline]
pub fn OidIsValid(oid: Oid) -> bool {
    oid != 0
}

unsafe fn ObjectIdGetDatum(oid: Oid) -> Datum {
    oid as Datum
}
unsafe fn Int16GetDatum(val: int16) -> Datum {
    val as Datum
}
unsafe fn DatumGetPointer(datum: Datum) -> *mut c_void {
    datum as *mut c_void
}
unsafe fn DatumGetArrayTypeP(datum: Datum) -> *mut ArrayType {
    datum as *mut ArrayType
}
unsafe fn PointerGetDatum(ptr: *mut c_void) -> Datum {
    ptr as Datum
}
unsafe fn ARR_DIMS(_arr: *mut ArrayType) -> *mut c_int {
    unimplemented!("TODO(pg-port): ARR_DIMS")
}
unsafe fn ARR_DATA_PTR(_arr: *mut ArrayType) -> *mut c_void {
    unimplemented!("TODO(pg-port): ARR_DATA_PTR")
}
unsafe fn ARR_NDIM(_arr: *mut ArrayType) -> c_int { 0 }
unsafe fn ARR_HASNULL(_arr: *mut ArrayType) -> bool { false }
unsafe fn ARR_ELEMTYPE(_arr: *mut ArrayType) -> Oid { 0 }
unsafe fn PointerIsValid(ptr: *const c_void) -> bool { !ptr.is_null() }
/// NameData -- use canonical type from crate::c.
pub use crate::c::NameData;
unsafe fn namestrcpy(_dst: *mut NameData, _src: *const c_char) {
    // TODO(pg-port): utils/strutils.c not ported.
}
unsafe fn NameStr(_name: *mut c_void) -> *const c_char {
    core::ptr::null() // TODO(pg-port)
}
unsafe fn TextDatumGetCString(_datum: Datum) -> *mut c_char {
    unimplemented!("TODO(pg-port): TextDatumGetCString")
}
unsafe fn datumCopy(_datum: Datum, _byval: bool, _len: c_int) -> Datum {
    unimplemented!("TODO(pg-port): datumCopy")
}
unsafe fn MemoryContextAlloc_bytes(cxt: *mut c_void, size: Size) -> *mut c_void {
    MemoryContextAlloc(cxt, size)
}

// -- query/parse helpers --

/// TODO(pg-port): nodes/makefuncs.c not ported.
unsafe fn makeNode(_tag: *mut c_void) -> *mut c_void {
    unimplemented!("TODO(pg-port): makeNode")
}
unsafe fn stringToNode(_str: *const c_char) -> *mut c_void {
    unimplemented!("TODO(pg-port): stringToNode (nodes/readfuncs.c not ported)")
}
unsafe fn setRuleCheckAsUser(_node: *mut c_void, _checkAsUser: Oid) {
    // TODO(pg-port): rewrite/rewriteDefine.c not ported.
}
unsafe fn eval_const_expressions(_pstate: *mut c_void, _node: *mut c_void) -> *mut c_void {
    unimplemented!("TODO(pg-port): eval_const_expressions (optimizer not ported)")
}
unsafe fn fix_opfuncids(_node: *mut c_void) {
    // TODO(pg-port): optimizer/prep/prepqual.c not ported.
}
unsafe fn canonicalize_qual(_qual: *mut c_void, _is_check: bool) -> *mut c_void {
    unimplemented!("TODO(pg-port): canonicalize_qual")
}
unsafe fn make_ands_implicit(_qual: *mut c_void) -> *mut c_void {
    unimplemented!("TODO(pg-port): make_ands_implicit")
}
unsafe fn pull_varattnos(_node: *mut c_void, _varno: c_int, _varattnos: *mut *mut Bitmapset) {
    // TODO(pg-port): optimizer not ported.
}
unsafe fn exprType(_node: *const c_void) -> Oid { 0 }
unsafe fn exprTypmod(_node: *const c_void) -> c_int { 0 }
unsafe fn exprCollation(_node: *const c_void) -> Oid { 0 }
unsafe fn makeConst(_typeid: Oid, _typmod: c_int, _collation: Oid, _typlen: c_int, _val: Datum, _isnull: bool, _byval: bool) -> *mut c_void {
    unimplemented!("TODO(pg-port): makeConst")
}

// -- transaction helpers --

unsafe fn IsTransactionState() -> bool {
    false // TODO(pg-port): access/transam/xact.c not ported.
}
unsafe fn GetCurrentSubTransactionId() -> SubTransactionId {
    0 // TODO(pg-port)
}
unsafe fn GetCurrentTransactionId() -> TransactionId {
    0 // TODO(pg-port)
}
unsafe fn IsParallelWorker() -> bool {
    false // TODO(pg-port)
}

// -- snapshot (utils/snapmgr.h) --

unsafe fn HistoricSnapshotActive() -> bool {
    false // TODO(pg-port)
}
unsafe fn RegisterSnapshot(_snap: Snapshot) -> Snapshot {
    unimplemented!("TODO(pg-port): RegisterSnapshot")
}
unsafe fn UnregisterSnapshot(_snap: Snapshot) {
    // TODO(pg-port)
}
unsafe fn GetNonHistoricCatalogSnapshot(_relid: Oid) -> Snapshot {
    unimplemented!("TODO(pg-port): GetNonHistoricCatalogSnapshot")
}
unsafe fn GetTransactionSnapshot() -> Snapshot {
    unimplemented!("TODO(pg-port): GetTransactionSnapshot")
}
unsafe fn PushActiveSnapshot(_snap: Snapshot) {
    // TODO(pg-port)
}
unsafe fn PopActiveSnapshot() {
    // TODO(pg-port)
}

// -- cache invalidation --

unsafe fn CacheInvalidateRelcache(_relation: Relation) {
    // TODO(pg-port): utils/cache/inval.c not ported.
}
unsafe fn AcceptInvalidationMessages() {
    // TODO(pg-port): utils/cache/inval.c not ported.
}

// -- cmd helpers --

unsafe fn CommandCounterIncrement() {
    // TODO(pg-port): access/transam/xact.c not ported.
}
unsafe fn CatalogTupleUpdate(_rel: Relation, _otid: *const ItemPointerData, _tup: HeapTuple) {
    // TODO(pg-port): catalog/indexing.c not ported.
}

// -- publication helpers (catalog/publication.h) --

unsafe fn is_publishable_relation(_relation: Relation) -> bool {
    false // TODO(pg-port)
}
unsafe fn GetRelationPublications(_relid: Oid) -> *mut List {
    core::ptr::null_mut() // TODO(pg-port)
}
unsafe fn GetSchemaPublications(_schemaid: Oid) -> *mut List {
    core::ptr::null_mut() // TODO(pg-port)
}
unsafe fn GetAllTablesPublications() -> *mut List {
    core::ptr::null_mut() // TODO(pg-port)
}
unsafe fn get_partition_ancestors(_relid: Oid) -> *mut List {
    core::ptr::null_mut() // TODO(pg-port)
}
unsafe fn pub_rf_contains_invalid_column(
    _pubid: Oid,
    _relation: Relation,
    _ancestors: *mut List,
    _pubviaroot: bool,
) -> bool {
    false // TODO(pg-port)
}
unsafe fn pub_contains_invalid_column(
    _pubid: Oid,
    _relation: Relation,
    _ancestors: *mut List,
    _pubviaroot: bool,
    _pubgencols: *mut c_void,
    _invalid_column_list: *mut bool,
    _invalid_gen_col: *mut bool,
) -> bool {
    false // TODO(pg-port)
}

// -- lock helpers --

unsafe fn LWLockAcquire(_lock: *mut c_void, _mode: LWLockMode) {
    // TODO(pg-port): storage/lwlock.c not ported.
}
unsafe fn LWLockRelease(_lock: *mut c_void) {
    // TODO(pg-port): storage/lwlock.c not ported.
}
static mut RelCacheInitLock: *mut c_void = core::ptr::null_mut();

// -- file I/O (storage/fd.h) --

/// TODO(pg-port): storage/file/fd.c not ported.
unsafe fn AllocateFile(_name: *const c_char, _mode: *const c_char) -> *mut c_void {
    unimplemented!("TODO(pg-port): AllocateFile")
}
unsafe fn FreeFile(_fp: *mut c_void) -> c_int {
    0 // TODO(pg-port)
}
unsafe fn AllocateDir(_path: *const c_char) -> *mut c_void {
    unimplemented!("TODO(pg-port): AllocateDir")
}
unsafe fn FreeDir(_dir: *mut c_void) {
    // TODO(pg-port)
}
pub type DIR = c_void;
/// dirent stub (access to d_name field required).
#[repr(C)]
pub struct dirent {
    pub d_name: [c_char; 256],
}
unsafe fn ReadDirExtended(_dir: *mut DIR, _path: *const c_char, _elevel: c_int) -> *mut dirent {
    unimplemented!("TODO(pg-port): ReadDirExtended")
}
unsafe fn fread(_buf: *mut c_void, _size: usize, _nmemb: usize, _fp: *mut c_void) -> usize {
    0 // TODO(pg-port)
}
unsafe fn fwrite(_buf: *const c_void, _size: usize, _nmemb: usize, _fp: *mut c_void) -> usize {
    0 // TODO(pg-port)
}

// database path
static mut DatabasePath: *const c_char = core::ptr::null();
static mut MyDatabaseId: Oid = 0;
static mut MyDatabaseTableSpace: Oid = 0;
static mut MyProcPid: c_int = 0;
pub const PG_BINARY_R: *const c_char = b"rb\0".as_ptr() as *const c_char;
pub const PG_BINARY_W: *const c_char = b"wb\0".as_ptr() as *const c_char;
pub const PG_TBLSPC_DIR: *const c_char = b"pg_tblspc\0".as_ptr() as *const c_char;
pub const TABLESPACE_VERSION_DIRECTORY: *const c_char = b"PG_16_202307071\0".as_ptr() as *const c_char;
pub const RELCACHE_INIT_FILENAME: *const c_char = b"pg_internal.init\0".as_ptr() as *const c_char;

// error codes
pub const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 0;
pub const ERRCODE_DATA_CORRUPTED: c_int = 0;
pub const ERRCODE_UNDEFINED_OBJECT: c_int = 0;
pub const LOG: c_int = 15;
pub const WARNING: c_int = 19;
pub const ERROR: c_int = 20;
pub const FATAL: c_int = 21;
pub const PANIC: c_int = 22;

// errcode_for_file_access placeholder
unsafe fn errcode_for_file_access() -> c_int { 0 }
unsafe fn errcode(_code: c_int) -> c_int { 0 }
unsafe fn errmsg_internal(_fmt: &str) -> c_int { 0 }
unsafe fn errmsg(_fmt: &str) -> c_int { 0 }
unsafe fn errdetail(_fmt: &str) -> c_int { 0 }
unsafe fn err_generic_string(_field: c_int, _val: *const c_char) {}
pub const PG_DIAG_SCHEMA_NAME: c_int = b's' as c_int;
pub const PG_DIAG_TABLE_NAME: c_int = b't' as c_int;
pub const PG_DIAG_COLUMN_NAME: c_int = b'c' as c_int;
pub const PG_DIAG_CONSTRAINT_NAME: c_int = b'n' as c_int;

/// RelationGetRelationName -- get relation name as *const c_char (utils/rel.h macro).
unsafe fn RelationGetRelationName(rel: Relation) -> *const c_char {
    if rel.is_null() || (*rel).rd_rel.is_null() {
        return b"<unknown>\0".as_ptr() as *const c_char;
    }
    (*(*rel).rd_rel).relname.data.as_ptr() as *const c_char
}

unsafe fn elog(_level: c_int, _fmt: &str) {
    // TODO(pg-port): utils/elog.c not ported (real home).
}
unsafe fn ereport(_level: c_int, _code: c_int, _msg: c_int) {
    // TODO(pg-port): utils/elog.c not ported.
}
unsafe fn ereport2(_level: c_int, _code: c_int, _msg: c_int, _detail: c_int) {
    // TODO(pg-port): utils/elog.c not ported.
}

// psprintf / snprintf stubs (non-variadic; macros swallow extra format args).
// TODO(pg-port): Replace with real libc wrappers when ported.
macro_rules! psprintf {
    ($fmt:expr $(, $arg:expr)*) => {{
        let _ = ($($arg,)*);
        core::ptr::null_mut::<c_char>()
    }};
}
macro_rules! snprintf {
    ($buf:expr, $size:expr, $fmt:expr $(, $arg:expr)* $(,)?) => {{
        let _ = ($buf, $size, $fmt, $($arg,)*);
        0 as c_int
    }};
}
unsafe fn rename(_old: *const c_char, _new: *const c_char) -> c_int {
    unimplemented!("TODO(pg-port): rename")
}
unsafe fn unlink(_path: *const c_char) -> c_int {
    unimplemented!("TODO(pg-port): unlink")
}
unsafe fn errno_location() -> *mut c_int {
    unimplemented!("TODO(pg-port): errno_location")
}

// index_open / index_close (access/index/indexam.h)
unsafe fn index_open(_indexoid: Oid, _lockmode: c_int) -> Relation {
    unimplemented!("TODO(pg-port): index_open")
}
unsafe fn index_close(_indexrel: Relation, _lockmode: c_int) {
    // TODO(pg-port): access/index/indexam.c not ported.
}
unsafe fn IndexRelationGetNumberOfAttributes(_r: Relation) -> c_int {
    0 // TODO(pg-port)
}
unsafe fn IndexRelationGetNumberOfKeyAttributes(_r: Relation) -> c_int {
    0 // TODO(pg-port)
}

// qsort helper
unsafe fn qsort(_base: *mut c_void, _nmemb: usize, _size: usize, _cmp: unsafe extern "C" fn(*const c_void, *const c_void) -> c_int) {
    // TODO(pg-port): use std::slice::sort in real implementation.
}

// ItemPointerData (storage/itemptr.h) - use canonical type
pub use crate::storage::itemptr::ItemPointerData;

// MultiXactId (access/multixact.h)
pub type MultiXactId = uint32;
pub const InvalidMultiXactId: MultiXactId = 0;

// F_OIDEQ / BTEqualStrategyNumber / BTGreaterStrategyNumber / F_INT2GT
pub const F_OIDEQ: RegProcedure = 0; // TODO(pg-port)
pub const F_INT2GT: RegProcedure = 0; // TODO(pg-port)
pub const BTEqualStrategyNumber: StrategyNumber = 3;
pub const BTGreaterStrategyNumber: StrategyNumber = 5;

// Anum_* constants (catalog/pg_class.h etc.)
// TODO(pg-port): these will come from generated catalog headers.
pub const Anum_pg_class_oid: c_int = 1;
pub const Anum_pg_attribute_attrelid: c_int = 1;
pub const Anum_pg_attribute_attnum: c_int = 6;
pub const Anum_pg_attribute_attmissingval: c_int = 43;
pub const Anum_pg_rewrite_ev_class: c_int = 2;
pub const Anum_pg_rewrite_ev_action: c_int = 7;
pub const Anum_pg_rewrite_ev_qual: c_int = 8;
pub const Anum_pg_opclass_oid: c_int = 1;
pub const Anum_pg_amproc_amprocfamily: c_int = 1;
pub const Anum_pg_amproc_amproclefttype: c_int = 2;
pub const Anum_pg_amproc_amprocrighttype: c_int = 3;
pub const Anum_pg_index_indcollation: c_int = 20;
pub const Anum_pg_index_indclass: c_int = 21;
pub const Anum_pg_index_indoption: c_int = 22;
pub const Anum_pg_index_indexprs: c_int = 19;
pub const Anum_pg_index_indpred: c_int = 23;
pub const Anum_pg_index_indrelid: c_int = 1;
pub const Anum_pg_attrdef_adrelid: c_int = 1;
pub const Anum_pg_attrdef_adbin: c_int = 3;
pub const Anum_pg_constraint_conrelid: c_int = 2;
pub const Anum_pg_constraint_conbin: c_int = 14;
pub const Anum_pg_constraint_conexclop: c_int = 22;
pub const Anum_pg_statistic_ext_stxrelid: c_int = 1;

// CONSTRAINT_* (catalog/pg_constraint.h)
pub const CONSTRAINT_CHECK: c_char = b'c' as c_char;
pub const CONSTRAINT_FOREIGN: c_char = b'f' as c_char;
pub const CONSTRAINT_EXCLUSION: c_char = b'x' as c_char;
pub const CONSTRAINT_NOTNULL: c_char = b'n' as c_char;
pub const CONSTRAINT_PRIMARY: c_char = b'p' as c_char;
pub const CONSTRAINT_UNIQUE: c_char = b'u' as c_char;

// CMD_SELECT (nodes/parsenodes.h)
pub const CMD_SELECT: c_int = 1;

// IndexAttrBitmapKind (utils/relcache.h)
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum IndexAttrBitmapKind {
    INDEX_ATTR_BITMAP_KEY = 0,
    INDEX_ATTR_BITMAP_PRIMARY_KEY = 1,
    INDEX_ATTR_BITMAP_IDENTITY_KEY = 2,
    INDEX_ATTR_BITMAP_HOT_BLOCKING = 3,
    INDEX_ATTR_BITMAP_SUMMARIZED = 4,
}

// ForeignKeyCacheInfo (utils/relcache.h)
// TODO(pg-port): placeholder struct.
pub type ForeignKeyCacheInfo = c_void;

// OID_BTREE_OPS_OID / INT2_BTREE_OPS_OID (utils/fmgroids.h)
pub const OID_BTREE_OPS_OID: Oid = 402;
pub const INT2_BTREE_OPS_OID: Oid = 423;

// debug_discard_caches (miscadmin.h)
static mut debug_discard_caches: c_int = 0;

// LOCALLOCK (storage/proc.h)
pub type LOCALLOCK = c_void;
pub type LockTagType = c_int;
pub const LOCKTAG_RELATION: LockTagType = 0;

// snprintf helpers
unsafe fn snprintf_path(buf: &mut [u8], fmt: &str, args: &str) {
    // TODO(pg-port): real snprintf via libc.
}

// Bootstrap modes
unsafe fn AssertCouldGetRelation() {
    // In C: Assert(IsTransactionState()); AssertBufferLocksPermitCatalogRead();
    // TODO(pg-port): assertions.
}
unsafe fn AssertBufferLocksPermitCatalogRead() {
    // TODO(pg-port): storage/bufmgr.c not ported.
}

// PartitionKey / PartitionDesc
pub type PartitionKey = *mut c_void;
pub type PartitionDesc = *mut c_void;

/// LOCKMODE constants (storage/lockdefs.h) -- repeated here for match-arm convenience.
pub const InplaceUpdateTupleLock: LOCKMODE = 6;
pub const RowExclusiveLock: LOCKMODE = crate::storage::lockdefs::RowExclusiveLock;

/// PublicationActions (catalog/publication.h).
#[repr(C)]
pub struct PublicationActions {
    pub pubinsert: bool,
    pub pubupdate: bool,
    pub pubdelete: bool,
    pub pubtruncate: bool,
}

/// PublicationDesc (catalog/publication.h).
#[repr(C)]
pub struct PublicationDesc {
    pub pubactions: PublicationActions,
    pub rf_valid_for_update: bool,
    pub rf_valid_for_delete: bool,
    pub cols_valid_for_update: bool,
    pub cols_valid_for_delete: bool,
    pub gencols_valid_for_update: bool,
    pub gencols_valid_for_delete: bool,
}

/// RowSecurityDesc (commands/policy.h).
#[repr(C)]
pub struct RowSecurityDesc {
    // TODO(pg-port): real fields not ported.
}

// c_uint alias
pub type c_uint = u32;
// c_long alias (already in prelude typically)
pub type c_long = i64;
// c_char
pub type c_char = i8;

// ===========================================================================
// Part 2: ScanPgRelation, AllocateRelationDesc, RelationParseRelOptions
// ===========================================================================

/*
 *		ScanPgRelation
 *
 *		This is used by RelationBuildDesc to find a pg_class
 *		tuple matching targetRelId.  The caller must hold at least
 *		AccessShareLock on the target relid to prevent concurrent-update
 *		scenarios; it isn't guaranteed that all scans used to build the
 *		relcache entry will use the same snapshot.
 *
 *		NB: the returned tuple has been copied into palloc'd storage
 *		and must eventually be freed with heap_freetuple.
 */
unsafe fn ScanPgRelation(
    targetRelId: Oid,
    indexOK: bool,
    force_non_historic: bool,
) -> HeapTuple {
    let mut pg_class_tuple: HeapTuple;
    let pg_class_desc: Relation;
    let pg_class_scan: SysScanDesc;
    let mut key: [ScanKeyData; 1] = [core::mem::zeroed()];
    let mut snapshot: Snapshot = core::ptr::null_mut();

    /*
     * If something goes wrong during backend startup, we might find ourselves
     * trying to read pg_class before we've selected a database.  That ain't
     * gonna work, so bail out with a useful error message.  If this happens,
     * it probably means a relcache entry that needs to be nailed isn't.
     */
    if !OidIsValid(MyDatabaseId) {
        elog(FATAL, "cannot read pg_class without having selected a database");
    }

    /*
     * form a scan key
     */
    ScanKeyInit(
        key.as_mut_ptr(),
        Anum_pg_class_oid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(targetRelId),
    );

    /*
     * Open pg_class and fetch a tuple.  Force heap scan if we haven't yet
     * built the critical relcache entries (this includes initdb and startup
     * without a pg_internal.init file).  The caller can also force a heap
     * scan by setting indexOK == false.
     */
    pg_class_desc = table_open(RelationRelationId, AccessShareLock);

    /*
     * The caller might need a tuple that's newer than what's visible to the
     * historic snapshot; currently the only case requiring to do so is
     * looking up the relfilenumber of non mapped system relations during
     * decoding.
     */
    if force_non_historic {
        snapshot = RegisterSnapshot(GetNonHistoricCatalogSnapshot(RelationRelationId));
    }

    pg_class_scan = systable_beginscan(
        pg_class_desc,
        ClassOidIndexId,
        indexOK && criticalRelcachesBuilt,
        snapshot,
        1,
        key.as_mut_ptr(),
    );

    pg_class_tuple = systable_getnext(pg_class_scan);

    /*
     * Must copy tuple before releasing buffer.
     */
    if HeapTupleIsValid(pg_class_tuple) {
        pg_class_tuple = heap_copytuple(pg_class_tuple);
    }

    /* all done */
    systable_endscan(pg_class_scan);

    if !snapshot.is_null() {
        UnregisterSnapshot(snapshot);
    }

    table_close(pg_class_desc, AccessShareLock);

    pg_class_tuple
}

/*
 *		AllocateRelationDesc
 *
 *		This is used to allocate memory for a new relation descriptor
 *		and initialize the rd_rel field from the given pg_class tuple.
 */
unsafe fn AllocateRelationDesc(relp: Form_pg_class) -> Relation {
    let relation: Relation;
    let oldcxt: *mut c_void;
    let relationForm: Form_pg_class;

    /* Relcache entries must live in CacheMemoryContext */
    oldcxt = MemoryContextSwitchTo(CacheMemoryContext as *mut c_void);

    /*
     * allocate and zero space for new relation descriptor
     */
    relation = palloc0(core::mem::size_of::<RelationData>()) as Relation;

    /* make sure relation is marked as having no open file yet */
    (*relation).rd_smgr = core::ptr::null_mut();

    /*
     * Copy the relation tuple form
     *
     * We only allocate space for the fixed fields, ie, CLASS_TUPLE_SIZE. The
     * variable-length fields (relacl, reloptions) are NOT stored in the
     * relcache --- there'd be little point in it, since we don't copy the
     * tuple's nulls bitmap and hence wouldn't know if the values are valid.
     * Bottom line is that relacl *cannot* be retrieved from the relcache. Get
     * it from the syscache if you need it.  The same goes for the original
     * form of reloptions (however, we do store the parsed form of reloptions
     * in rd_options).
     */
    relationForm = palloc(CLASS_TUPLE_SIZE) as Form_pg_class;

    core::ptr::copy_nonoverlapping(relp as *const u8, relationForm as *mut u8, CLASS_TUPLE_SIZE);

    /* initialize relation tuple form */
    (*relation).rd_rel = relationForm;

    /* and allocate attribute tuple form storage */
    (*relation).rd_att = CreateTemplateTupleDesc((*relationForm).relnatts as c_int);
    /* which we mark as a reference-counted tupdesc */
    (*(*relation).rd_att).tdrefcount = 1;

    MemoryContextSwitchTo(oldcxt);

    relation
}

/*
 * RelationParseRelOptions
 *		Convert pg_class.reloptions into pre-parsed rd_options
 *
 * tuple is the real pg_class tuple (not rd_rel!) for relation
 *
 * Note: rd_rel and (if an index) rd_indam must be valid already
 */
unsafe fn RelationParseRelOptions(relation: Relation, tuple: HeapTuple) {
    let mut options: *mut bytea;
    let amoptsfn: amoptions_function;

    (*relation).rd_options = core::ptr::null_mut();

    /*
     * Look up any AM-specific parse function; fall out if relkind should not
     * have options.
     */
    match (*(*relation).rd_rel).relkind {
        k if k == RELKIND_RELATION
            || k == RELKIND_TOASTVALUE
            || k == RELKIND_VIEW
            || k == RELKIND_MATVIEW
            || k == RELKIND_PARTITIONED_TABLE =>
        {
            amoptsfn = core::ptr::null_mut();
        }
        k if k == RELKIND_INDEX || k == RELKIND_PARTITIONED_INDEX => {
            // amoptsfn = relation->rd_indam->amoptions;
            // TODO(pg-port): rd_indam not accessible without IndexAmRoutine field.
            amoptsfn = core::ptr::null_mut();
        }
        _ => {
            return;
        }
    }

    /*
     * Fetch reloptions from tuple; have to use a hardwired descriptor because
     * we might not have any other for pg_class yet (consider executing this
     * code for pg_class itself)
     */
    options = extractRelOptions(tuple, GetPgClassDescriptor(), amoptsfn);

    /*
     * Copy parsed data into CacheMemoryContext.  To guard against the
     * possibility of leaks in the reloptions code, we want to do the actual
     * parsing in the caller's memory context and copy the results into
     * CacheMemoryContext after the fact.
     */
    if !options.is_null() {
        (*relation).rd_options =
            MemoryContextAlloc(CacheMemoryContext as *mut c_void, VARSIZE(options)) as *mut bytea;
        core::ptr::copy_nonoverlapping(
            options as *const u8,
            (*relation).rd_options as *mut u8,
            VARSIZE(options),
        );
        pfree(options as *mut c_void);
    }
}

/*
 *		RelationBuildTupleDesc
 *
 *		Form the relation's tuple descriptor from information in
 *		the pg_attribute, pg_attrdef & pg_constraint system catalogs.
 */
unsafe fn RelationBuildTupleDesc(relation: Relation) {
    let mut pg_attribute_tuple: HeapTuple;
    let pg_attribute_desc: Relation;
    let pg_attribute_scan: SysScanDesc;
    let mut skey: [ScanKeyData; 2] = [const { core::mem::zeroed() }; 2];
    let mut need: c_int;
    let constr: *mut TupleConstr;
    let mut attrmiss: *mut AttrMissing = core::ptr::null_mut();
    let mut ndef: c_int = 0;

    /* fill rd_att's type ID fields (compare heap.c's AddNewRelationTuple) */
    (*(*relation).rd_att).tdtypeid = if (*(*relation).rd_rel).reltype != 0 {
        (*(*relation).rd_rel).reltype
    } else {
        RECORDOID
    };
    (*(*relation).rd_att).tdtypmod = -1; /* just to be sure */

    constr = MemoryContextAllocZero(
        CacheMemoryContext as *mut c_void,
        core::mem::size_of::<TupleConstr>(),
    ) as *mut TupleConstr;

    /*
     * Form a scan key that selects only user attributes (attnum > 0).
     * (Eliminating system attribute rows at the index level is lots faster
     * than fetching them.)
     */
    ScanKeyInit(
        skey.as_mut_ptr(),
        Anum_pg_attribute_attrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(relation)),
    );
    ScanKeyInit(
        skey.as_mut_ptr().add(1),
        Anum_pg_attribute_attnum,
        BTGreaterStrategyNumber,
        F_INT2GT,
        Int16GetDatum(0),
    );

    /*
     * Open pg_attribute and begin a scan.  Force heap scan if we haven't yet
     * built the critical relcache entries (this includes initdb and startup
     * without a pg_internal.init file).
     */
    pg_attribute_desc = table_open(AttributeRelationId, AccessShareLock);
    pg_attribute_scan = systable_beginscan(
        pg_attribute_desc,
        AttributeRelidNumIndexId,
        criticalRelcachesBuilt,
        core::ptr::null_mut(),
        2,
        skey.as_mut_ptr(),
    );

    /*
     * add attribute data to relation->rd_att
     */
    need = RelationGetNumberOfAttributes(relation);

    loop {
        pg_attribute_tuple = systable_getnext(pg_attribute_scan);
        if !HeapTupleIsValid(pg_attribute_tuple) {
            break;
        }
        let attp = GETSTRUCT(pg_attribute_tuple) as Form_pg_attribute;

        let attnum = (*(attp as *mut FormData_pg_attribute_stub)).attnum;
        if attnum <= 0 || attnum > RelationGetNumberOfAttributes(relation) as i16 {
            elog(
                ERROR,
                "invalid attribute number for relcache entry (see relcache.c)",
            );
        }

        core::ptr::copy_nonoverlapping(
            attp as *const u8,
            TupleDescAttr((*relation).rd_att, (attnum - 1) as c_int) as *mut u8,
            ATTRIBUTE_FIXED_PART_SIZE,
        );

        populate_compact_attribute((*relation).rd_att, (attnum - 1) as c_int);

        /* Update constraint/default info */
        if (*(attp as *mut FormData_pg_attribute_stub)).attnotnull {
            (*(constr as *mut TupleConstrStub)).has_not_null = true;
        }
        if (*(attp as *mut FormData_pg_attribute_stub)).attgenerated == ATTRIBUTE_GENERATED_STORED {
            (*(constr as *mut TupleConstrStub)).has_generated_stored = true;
        }
        if (*(attp as *mut FormData_pg_attribute_stub)).attgenerated == ATTRIBUTE_GENERATED_VIRTUAL {
            (*(constr as *mut TupleConstrStub)).has_generated_virtual = true;
        }
        if (*(attp as *mut FormData_pg_attribute_stub)).atthasdef {
            ndef += 1;
        }

        /* If the column has a "missing" value, put it in the attrmiss array */
        if (*(attp as *mut FormData_pg_attribute_stub)).atthasmissing {
            let mut missingNull: bool = false;
            let missingval = heap_getattr(
                pg_attribute_tuple,
                Anum_pg_attribute_attmissingval,
                (*pg_attribute_desc).rd_att,
                &mut missingNull,
            );
            if !missingNull {
                /* Yes, fetch from the array */
                let oldcxt: *mut c_void;
                let mut is_null: bool = false;
                let mut one: c_int = 1;
                // TODO(pg-port): array_get_element and datumCopy not fully ported.
                // let missval = array_get_element(missingval, 1, &mut one, -1,
                //     attp->attlen, attp->attbyval, attp->attalign, &mut is_null);
                // For now skip actual extraction (TODO(pg-port)).
                if attrmiss.is_null() {
                    attrmiss = MemoryContextAllocZero(
                        CacheMemoryContext as *mut c_void,
                        (*(*relation).rd_rel).relnatts as usize
                            * core::mem::size_of::<AttrMissing>(),
                    ) as *mut AttrMissing;
                }
                // attrmiss[(attnum-1)].am_present = true; -- TODO(pg-port)
            }
        }

        need -= 1;
        if need == 0 {
            break;
        }
    }

    /*
     * end the scan and close the attribute relation
     */
    systable_endscan(pg_attribute_scan);
    table_close(pg_attribute_desc, AccessShareLock);

    if need != 0 {
        elog(
            ERROR,
            "pg_attribute catalog is missing attribute(s) for relation (see relcache.c)",
        );
    }

    /*
     * We can easily set the attcacheoff value for the first attribute: it
     * must be zero.  This eliminates the need for special cases for attnum=1
     * that used to exist in fastgetattr() and index_getattr().
     */
    if RelationGetNumberOfAttributes(relation) > 0 {
        // TupleDescCompactAttr(relation->rd_att, 0)->attcacheoff = 0;
        // TODO(pg-port): CompactAttribute layout not ported.
    }

    /*
     * Set up constraint/default info
     */
    let has_not_null = (*(constr as *mut TupleConstrStub)).has_not_null;
    let has_gen_stored = (*(constr as *mut TupleConstrStub)).has_generated_stored;
    let has_gen_virt = (*(constr as *mut TupleConstrStub)).has_generated_virtual;
    if has_not_null
        || has_gen_stored
        || has_gen_virt
        || ndef > 0
        || !attrmiss.is_null()
        || (*(*relation).rd_rel).relchecks > 0
    {
        let is_catalog = IsCatalogRelation(relation);

        (*(*relation).rd_att).constr = constr as *mut crate::access::common::tupdesc::TupleConstr;

        if ndef > 0 {
            /* DEFAULTs */
            AttrDefaultFetch(relation, ndef);
        } else {
            (*(constr as *mut TupleConstrStub)).num_defval = 0;
        }

        // constr->missing = attrmiss; -- TODO(pg-port)

        /* CHECK and NOT NULLs */
        if (*(*relation).rd_rel).relchecks > 0 || (!is_catalog && has_not_null) {
            CheckNNConstraintFetch(relation);
        }

        /*
         * Any not-null constraint that wasn't marked invalid by
         * CheckNNConstraintFetch must necessarily be valid; make it so in the
         * CompactAttribute array.
         */
        if !is_catalog {
            for i in 0..(*(*relation).rd_rel).relnatts as c_int {
                // attr = TupleDescCompactAttr(relation->rd_att, i);
                // if (attr->attnullability == ATTNULLABLE_UNKNOWN)
                //     attr->attnullability = ATTNULLABLE_VALID;
                // TODO(pg-port): CompactAttribute layout not ported.
            }
        }

        if (*(*relation).rd_rel).relchecks == 0 {
            (*(constr as *mut TupleConstrStub)).num_check = 0;
        }
    } else {
        pfree(constr as *mut c_void);
        (*(*relation).rd_att).constr = core::ptr::null_mut();
    }
}

/// Stub TupleConstr field layout for direct field access.
/// TODO(pg-port): replace with real TupleConstr once access/common/tupdesc.rs is complete.
#[repr(C)]
struct TupleConstrStub {
    defval: *mut c_void,
    missing: *mut c_void,
    check: *mut c_void,
    num_defval: uint16,
    num_check: uint16,
    has_not_null: bool,
    has_generated_stored: bool,
    has_generated_virtual: bool,
}

/// Stub FormData_pg_attribute for field access.
/// TODO(pg-port): replace with real FormData_pg_attribute once catalog headers are ported.
#[repr(C)]
struct FormData_pg_attribute_stub {
    attrelid: Oid,
    attname: [c_char; 64],
    atttypid: Oid,
    attlen: int16,
    attnum: int16,
    attndims: c_int,
    attcacheoff: c_int,
    atttypmod: c_int,
    attbyval: bool,
    attstorage: c_char,
    attalign: c_char,
    attnotnull: bool,
    atthasdef: bool,
    atthasmissing: bool,
    attidentity: c_char,
    attgenerated: c_char,
    attisdropped: bool,
    attislocal: bool,
    attinhcount: int16,
    attcollation: Oid,
}

// ===========================================================================
// Part 3: RelationBuildRuleLock, equalRuleLocks, equalPolicy, equalRSDesc,
//         RelationBuildDesc, RelationInitPhysicalAddr
// ===========================================================================

/*
 *		RelationBuildRuleLock
 *
 *		Form the relation's rewrite rules from information in
 *		the pg_rewrite system catalog.
 *
 * Note: The rule parsetrees are potentially very complex node structures.
 * To allow these trees to be freed when the relcache entry is flushed,
 * we make a private memory context to hold the RuleLock information for
 * each relcache entry that has associated rules.
 *
 * Note: The relation's reloptions must have been extracted first.
 */
unsafe fn RelationBuildRuleLock(relation: Relation) {
    let rulescxt: *mut c_void;
    let mut oldcxt: *mut c_void;
    let mut rewrite_tuple: HeapTuple;
    let rewrite_desc: Relation;
    let rewrite_tupdesc: TupleDesc;
    let rewrite_scan: SysScanDesc;
    let mut key: [ScanKeyData; 1] = [core::mem::zeroed()];
    let rulelock: *mut RuleLock;
    let mut numlocks: c_int;
    let mut rules: *mut *mut RewriteRule;
    let mut maxlocks: c_int;

    /*
     * Make the private context.  Assume it'll not contain much data.
     */
    rulescxt = AllocSetContextCreate(
        CacheMemoryContext as *mut c_void,
        b"relation rules ".as_ptr() as *const c_char,
        ALLOCSET_SMALL_SIZES,
    );
    (*relation).rd_rulescxt = rulescxt;
    MemoryContextCopyAndSetIdentifier(rulescxt, b"relation rules ".as_ptr() as *const c_char); // TODO: use RelationGetRelationName

    /*
     * allocate an array to hold the rewrite rules (the array is extended if
     * necessary)
     */
    maxlocks = 4;
    rules = MemoryContextAlloc(
        rulescxt,
        core::mem::size_of::<*mut RewriteRule>() * maxlocks as usize,
    ) as *mut *mut RewriteRule;
    numlocks = 0;

    /*
     * form a scan key
     */
    ScanKeyInit(
        key.as_mut_ptr(),
        Anum_pg_rewrite_ev_class,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(relation)),
    );

    /*
     * open pg_rewrite and begin a scan
     *
     * Note: since we scan the rules using RewriteRelRulenameIndexId, we will
     * be reading the rules in name order, except possibly during
     * emergency-recovery operations (ie, IgnoreSystemIndexes). This in turn
     * ensures that rules will be fired in name order.
     */
    rewrite_desc = table_open(RewriteRelationId, AccessShareLock);
    rewrite_tupdesc = RelationGetDescr(rewrite_desc);
    rewrite_scan = systable_beginscan(
        rewrite_desc,
        RewriteRelRulenameIndexId,
        true,
        core::ptr::null_mut(),
        1,
        key.as_mut_ptr(),
    );

    loop {
        rewrite_tuple = systable_getnext(rewrite_scan);
        if !HeapTupleIsValid(rewrite_tuple) {
            break;
        }
        let rewrite_form = GETSTRUCT(rewrite_tuple) as Form_pg_rewrite;
        let mut isnull: bool = false;
        let mut rule_datum: Datum;
        let mut rule_str: *mut c_char;
        let rule: *mut RewriteRule;
        let check_as_user: Oid;

        rule = MemoryContextAlloc(rulescxt, core::mem::size_of::<RewriteRule>()) as *mut RewriteRule;

        // rule->ruleId = rewrite_form->oid;
        // rule->event = rewrite_form->ev_type - '0';
        // rule->enabled = rewrite_form->ev_enabled;
        // rule->isInstead = rewrite_form->is_instead;
        // TODO(pg-port): Form_pg_rewrite field access not ported.

        /*
         * Must use heap_getattr to fetch ev_action and ev_qual.  Also, the
         * rule strings are often large enough to be toasted.  To avoid
         * leaking memory in the caller's context, do the detoasting here so
         * we can free the detoasted version.
         */
        rule_datum = heap_getattr(
            rewrite_tuple,
            Anum_pg_rewrite_ev_action,
            rewrite_tupdesc,
            &mut isnull,
        );
        // Assert(!isnull);
        rule_str = TextDatumGetCString(rule_datum);
        oldcxt = MemoryContextSwitchTo(rulescxt);
        // rule->actions = (List *) stringToNode(rule_str);
        // TODO(pg-port): stringToNode and rewrite rule struct not ported.
        MemoryContextSwitchTo(oldcxt);
        pfree(rule_str as *mut c_void);

        rule_datum = heap_getattr(
            rewrite_tuple,
            Anum_pg_rewrite_ev_qual,
            rewrite_tupdesc,
            &mut isnull,
        );
        // Assert(!isnull);
        rule_str = TextDatumGetCString(rule_datum);
        oldcxt = MemoryContextSwitchTo(rulescxt);
        // rule->qual = (Node *) stringToNode(rule_str);
        // TODO(pg-port): stringToNode not ported.
        MemoryContextSwitchTo(oldcxt);
        pfree(rule_str as *mut c_void);

        /*
         * If this is a SELECT rule defining a view, and the view has
         * "security_invoker" set, we must perform all permissions checks on
         * relations referred to by the rule as the invoking user.
         *
         * In all other cases (including non-SELECT rules on security invoker
         * views), perform the permissions checks as the relation owner.
         */
        // if (rule->event == CMD_SELECT && ...)
        //     check_as_user = InvalidOid;
        // else
        //     check_as_user = relation->rd_rel->relowner;
        // TODO(pg-port): rule->event not accessible without porting RewriteRule struct.
        check_as_user = 0; // placeholder

        // setRuleCheckAsUser((Node *) rule->actions, check_as_user);
        // setRuleCheckAsUser(rule->qual, check_as_user);
        // TODO(pg-port): RewriteRule struct not ported.

        if numlocks >= maxlocks {
            maxlocks *= 2;
            rules = repalloc(
                rules as *mut c_void,
                core::mem::size_of::<*mut RewriteRule>() * maxlocks as usize,
            ) as *mut *mut RewriteRule;
        }
        *rules.add(numlocks as usize) = rule;
        numlocks += 1;
    }

    /*
     * end the scan and close the attribute relation
     */
    systable_endscan(rewrite_scan);
    table_close(rewrite_desc, AccessShareLock);

    /*
     * there might not be any rules (if relhasrules is out-of-date)
     */
    if numlocks == 0 {
        (*relation).rd_rules = core::ptr::null_mut();
        (*relation).rd_rulescxt = core::ptr::null_mut();
        MemoryContextDelete(rulescxt);
        return;
    }

    /*
     * form a RuleLock and insert into relation
     */
    // rulelock = (RuleLock *) MemoryContextAlloc(rulescxt, sizeof(RuleLock));
    // rulelock->numLocks = numlocks;
    // rulelock->rules = rules;
    // relation->rd_rules = rulelock;
    // TODO(pg-port): RuleLock struct not ported.
}

/*
 *		equalRuleLocks
 *
 *		Determine whether two RuleLocks are equivalent
 *
 *		Probably this should be in the rules code someplace...
 */
unsafe fn equalRuleLocks(rlock1: *const RuleLock, rlock2: *const RuleLock) -> bool {
    /*
     * As of 7.3 we assume the rule ordering is repeatable, because
     * RelationBuildRuleLock should read 'em in a consistent order.  So just
     * compare corresponding slots.
     */
    // TODO(pg-port): RuleLock struct not ported.
    if !rlock1.is_null() {
        if rlock2.is_null() {
            return false;
        }
        // compare numLocks, then each rule's ruleId/event/enabled/isInstead/qual/actions
        // TODO(pg-port)
    } else if !rlock2.is_null() {
        return false;
    }
    true
}

/*
 *		equalPolicy
 *
 *		Determine whether two policies are equivalent
 */
unsafe fn equalPolicy(policy1: *const RowSecurityPolicy, policy2: *const RowSecurityPolicy) -> bool {
    if !policy1.is_null() {
        if policy2.is_null() {
            return false;
        }
        // TODO(pg-port): RowSecurityPolicy struct not ported.
    } else if !policy2.is_null() {
        return false;
    }
    true
}

/*
 *		equalRSDesc
 *
 *		Determine whether two RowSecurityDesc's are equivalent
 */
unsafe fn equalRSDesc(rsdesc1: *const RowSecurityDesc, rsdesc2: *const RowSecurityDesc) -> bool {
    if rsdesc1.is_null() && rsdesc2.is_null() {
        return true;
    }
    if (!rsdesc1.is_null() && rsdesc2.is_null()) || (rsdesc1.is_null() && !rsdesc2.is_null()) {
        return false;
    }
    // TODO(pg-port): list_length(rsdesc1->policies) etc. not ported.
    true
}

/*
 *		RelationBuildDesc
 *
 *		Build a relation descriptor.  The caller must hold at least
 *		AccessShareLock on the target relid.
 *
 *		The new descriptor is inserted into the hash table if insertIt is true.
 *
 *		Returns NULL if no pg_class row could be found for the given relid
 *		(suggesting we are trying to access a just-deleted relation).
 *		Any other error is reported via elog.
 */
unsafe fn RelationBuildDesc(targetRelId: Oid, insertIt: bool) -> Relation {
    let mut in_progress_offset: c_int;
    let mut relation: Relation;
    let mut relid: Oid;
    let mut pg_class_tuple: HeapTuple;
    let mut relp: Form_pg_class;

    /* Register to catch invalidation messages */
    if in_progress_list_len >= in_progress_list_maxlen {
        let allocsize = in_progress_list_maxlen * 2;
        in_progress_list = repalloc(
            in_progress_list as *mut c_void,
            allocsize as usize * core::mem::size_of::<InProgressEnt>(),
        ) as *mut InProgressEnt;
        in_progress_list_maxlen = allocsize;
    }
    in_progress_offset = in_progress_list_len;
    in_progress_list_len += 1;
    (*in_progress_list.add(in_progress_offset as usize)).reloid = targetRelId;

    // 'retry: label -- use a loop with boolean flag
    loop {
        (*in_progress_list.add(in_progress_offset as usize)).invalidated = false;

        /*
         * find the tuple in pg_class corresponding to the given relation id
         */
        pg_class_tuple = ScanPgRelation(targetRelId, true, false);

        /*
         * if no such tuple exists, return NULL
         */
        if !HeapTupleIsValid(pg_class_tuple) {
            debug_assert_eq!(in_progress_offset + 1, in_progress_list_len);
            in_progress_list_len -= 1;
            return core::ptr::null_mut();
        }

        /*
         * get information from the pg_class_tuple
         */
        relp = GETSTRUCT(pg_class_tuple) as Form_pg_class;
        relid = (*relp).oid;
        debug_assert_eq!(relid, targetRelId);

        /*
         * allocate storage for the relation descriptor, and copy pg_class_tuple
         * to relation->rd_rel.
         */
        relation = AllocateRelationDesc(relp);

        /*
         * initialize the relation's relation id (relation->rd_id)
         */
        (*relation).rd_id = relid;

        /*
         * Normal relations are not nailed into the cache.  Since we don't flush
         * new relations, it won't be new.  It could be temp though.
         */
        (*relation).rd_refcnt = 0;
        (*relation).rd_isnailed = false;
        (*relation).rd_createSubid = InvalidSubTransactionId;
        (*relation).rd_newRelfilelocatorSubid = InvalidSubTransactionId;
        (*relation).rd_firstRelfilelocatorSubid = InvalidSubTransactionId;
        (*relation).rd_droppedSubid = InvalidSubTransactionId;

        match (*(*relation).rd_rel).relpersistence {
            k if k == RELPERSISTENCE_UNLOGGED || k == RELPERSISTENCE_PERMANENT => {
                (*relation).rd_backend = INVALID_PROC_NUMBER;
                (*relation).rd_islocaltemp = false;
            }
            k if k == RELPERSISTENCE_TEMP => {
                if isTempOrTempToastNamespace((*(*relation).rd_rel).relnamespace) {
                    (*relation).rd_backend = ProcNumberForTempRelations();
                    (*relation).rd_islocaltemp = true;
                } else {
                    /*
                     * If it's a temp table, but not one of ours, we have to use
                     * the slow, grotty method to figure out the owning backend.
                     */
                    (*relation).rd_backend =
                        GetTempNamespaceProcNumber((*(*relation).rd_rel).relnamespace);
                    debug_assert_ne!((*relation).rd_backend, INVALID_PROC_NUMBER);
                    (*relation).rd_islocaltemp = false;
                }
            }
            _ => {
                elog(ERROR, "invalid relpersistence in RelationBuildDesc");
            }
        }

        /*
         * initialize the tuple descriptor (relation->rd_att).
         */
        RelationBuildTupleDesc(relation);

        /* foreign key data is not loaded till asked for */
        (*relation).rd_fkeylist = NIL;
        (*relation).rd_fkeyvalid = false;

        /* partitioning data is not loaded till asked for */
        (*relation).rd_partkey = core::ptr::null_mut();
        (*relation).rd_partkeycxt = core::ptr::null_mut();
        (*relation).rd_partdesc = core::ptr::null_mut();
        (*relation).rd_partdesc_nodetached = core::ptr::null_mut();
        (*relation).rd_partdesc_nodetached_xmin = InvalidTransactionId;
        (*relation).rd_pdcxt = core::ptr::null_mut();
        (*relation).rd_pddcxt = core::ptr::null_mut();
        (*relation).rd_partcheck = NIL;
        (*relation).rd_partcheckvalid = false;
        (*relation).rd_partcheckcxt = core::ptr::null_mut();

        /*
         * initialize access method information
         */
        let relkind = (*(*relation).rd_rel).relkind;
        if relkind == RELKIND_INDEX || relkind == RELKIND_PARTITIONED_INDEX {
            RelationInitIndexAccessInfo(relation);
        } else if RELKIND_HAS_TABLE_AM(relkind) || relkind == RELKIND_SEQUENCE {
            RelationInitTableAccessMethod(relation);
        } else if relkind == RELKIND_PARTITIONED_TABLE {
            /*
             * Do nothing: access methods are a setting that partitions can
             * inherit.
             */
        } else {
            // Assert(relation->rd_rel->relam == InvalidOid);
        }

        /* extract reloptions if any */
        RelationParseRelOptions(relation, pg_class_tuple);

        /*
         * Fetch rules and triggers that affect this relation.
         *
         * Note that RelationBuildRuleLock() relies on this being done after
         * extracting the relation's reloptions.
         */
        if (*(*relation).rd_rel).relhasrules {
            RelationBuildRuleLock(relation);
        } else {
            (*relation).rd_rules = core::ptr::null_mut();
            (*relation).rd_rulescxt = core::ptr::null_mut();
        }

        if (*(*relation).rd_rel).relhastriggers {
            RelationBuildTriggers(relation);
        } else {
            (*relation).trigdesc = core::ptr::null_mut();
        }

        if (*(*relation).rd_rel).relrowsecurity {
            RelationBuildRowSecurity(relation);
        } else {
            (*relation).rd_rsdesc = core::ptr::null_mut();
        }

        /*
         * initialize the relation lock manager information
         */
        RelationInitLockInfo(relation); /* see lmgr.c */

        /*
         * initialize physical addressing information for the relation
         */
        RelationInitPhysicalAddr(relation);

        /* make sure relation is marked as having no open file yet */
        (*relation).rd_smgr = core::ptr::null_mut();

        /*
         * now we can free the memory allocated for pg_class_tuple
         */
        heap_freetuple(pg_class_tuple);

        /*
         * If an invalidation arrived mid-build, start over.
         */
        if (*in_progress_list.add(in_progress_offset as usize)).invalidated {
            RelationDestroyRelation(relation, false);
            // goto retry -- continue the loop
            continue;
        }

        debug_assert_eq!(in_progress_offset + 1, in_progress_list_len);
        in_progress_list_len -= 1;

        /*
         * Insert newly created relation into relcache hash table, if requested.
         */
        if insertIt {
            RelationCacheInsert(relation, true);
        }

        /* It's fully valid */
        (*relation).rd_isvalid = true;

        return relation;
    }
}

/*
 * Initialize the physical addressing info (RelFileLocator) for a relcache entry
 */
unsafe fn RelationInitPhysicalAddr(relation: Relation) {
    use crate::common::blkreftable::RelFileLocator;

    let oldnumber = (*relation).rd_locator.relNumber;

    /* these relations kinds never have storage */
    if !RELKIND_HAS_STORAGE((*(*relation).rd_rel).relkind) {
        return;
    }

    if (*(*relation).rd_rel).reltablespace != 0 {
        (*relation).rd_locator.spcOid = (*(*relation).rd_rel).reltablespace;
    } else {
        (*relation).rd_locator.spcOid = MyDatabaseTableSpace;
    }
    if (*relation).rd_locator.spcOid == GLOBALTABLESPACE_OID {
        (*relation).rd_locator.dbOid = 0; // InvalidOid
    } else {
        (*relation).rd_locator.dbOid = MyDatabaseId;
    }

    if (*(*relation).rd_rel).relfilenode != 0 {
        /*
         * Even if we are using a decoding snapshot that doesn't represent the
         * current state of the catalog we need to make sure the filenode
         * points to the current file since the older file will be gone (or
         * truncated).
         */
        if HistoricSnapshotActive()
            && RelationIsAccessibleInLogicalDecoding(relation)
            && IsTransactionState()
        {
            let phys_tuple: HeapTuple;
            let physrel: Form_pg_class;

            phys_tuple = ScanPgRelation(
                RelationGetRelid(relation),
                RelationGetRelid(relation) != ClassOidIndexId,
                true,
            );
            if !HeapTupleIsValid(phys_tuple) {
                elog(ERROR, "could not find pg_class entry for relation in RelationInitPhysicalAddr");
            }
            physrel = GETSTRUCT(phys_tuple) as Form_pg_class;

            (*(*relation).rd_rel).reltablespace = (*physrel).reltablespace;
            (*(*relation).rd_rel).relfilenode = (*physrel).relfilenode;
            heap_freetuple(phys_tuple);
        }

        (*relation).rd_locator.relNumber = (*(*relation).rd_rel).relfilenode;
    } else {
        /* Consult the relation mapper */
        (*relation).rd_locator.relNumber = RelationMapOidToFilenumber(
            (*relation).rd_id,
            (*(*relation).rd_rel).relisshared,
        );
        if !RelFileNumberIsValid((*relation).rd_locator.relNumber) {
            elog(
                ERROR,
                "could not find relation mapping in RelationInitPhysicalAddr",
            );
        }
    }

    /*
     * For RelationNeedsWAL() to answer correctly on parallel workers, restore
     * rd_firstRelfilelocatorSubid.
     */
    if IsParallelWorker() && oldnumber != (*relation).rd_locator.relNumber {
        // if RelFileLocatorSkippingWAL(relation->rd_locator):
        //     relation->rd_firstRelfilelocatorSubid = TopSubTransactionId;
        // else:
        //     relation->rd_firstRelfilelocatorSubid = InvalidSubTransactionId;
        // TODO(pg-port): RelFileLocatorSkippingWAL not ported.
    }
}

// ===========================================================================
// Part 4: InitIndexAmRoutine, RelationInitIndexAccessInfo,
//         IndexSupportInitialize, LookupOpclassInfo,
//         InitTableAmRoutine, RelationInitTableAccessMethod,
//         formrdesc, AssertCouldGetRelation, RelationIdGetRelation
// ===========================================================================

/*
 * Fill in the IndexAmRoutine for an index relation.
 *
 * relation's rd_amhandler and rd_indexcxt must be valid already.
 */
unsafe fn InitIndexAmRoutine(relation: Relation) {
    use crate::access::index::amapi::IndexAmRoutine;

    /*
     * Call the amhandler in current, short-lived memory context, just in case
     * it leaks anything (it probably won't, but let's be paranoid).
     */
    let tmp = GetIndexAmRoutine((*relation).rd_amhandler);

    /* OK, now transfer the data into relation's rd_indexcxt. */
    let cached = MemoryContextAlloc(
        (*relation).rd_indexcxt,
        core::mem::size_of::<IndexAmRoutine>(),
    ) as *mut IndexAmRoutine;
    core::ptr::copy_nonoverlapping(tmp, cached, 1);
    (*relation).rd_indam = cached as *mut crate::access::index::amapi::IndexAmRoutine;

    pfree(tmp as *mut c_void);
}

/*
 * Initialize index-access-method support data for an index relation
 */
pub unsafe fn RelationInitIndexAccessInfo(relation: Relation) {
    let mut tuple: HeapTuple;
    let aform: Form_pg_am;
    let mut indcollDatum: Datum;
    let mut indclassDatum: Datum;
    let mut indoptionDatum: Datum;
    let mut isnull: bool;
    let indcoll: *mut oidvector;
    let indclass: *mut oidvector;
    let indoption: *mut int2vector;
    let indexcxt: *mut c_void;
    let mut oldcontext: *mut c_void;
    let indnatts: c_int;
    let indnkeyatts: c_int;
    let amsupport: uint16;

    /*
     * Make a copy of the pg_index entry for the index.
     */
    tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(RelationGetRelid(relation)));
    if !HeapTupleIsValid(tuple) {
        elog(ERROR, "cache lookup failed for index in RelationInitIndexAccessInfo");
    }
    oldcontext = MemoryContextSwitchTo(CacheMemoryContext as *mut c_void);
    (*relation).rd_indextuple = heap_copytuple(tuple) as *mut c_void;
    (*relation).rd_index = GETSTRUCT((*relation).rd_indextuple as HeapTuple) as Form_pg_index;
    MemoryContextSwitchTo(oldcontext);
    ReleaseSysCache(tuple);

    /*
     * Look up the index's access method, save the OID of its handler function
     */
    // Assert(relation->rd_rel->relam != InvalidOid);
    tuple = SearchSysCache1(AMOID, ObjectIdGetDatum((*(*relation).rd_rel).relam));
    if !HeapTupleIsValid(tuple) {
        elog(ERROR, "cache lookup failed for access method in RelationInitIndexAccessInfo");
    }
    aform = GETSTRUCT(tuple) as Form_pg_am;
    // relation->rd_amhandler = aform->amhandler;
    // TODO(pg-port): Form_pg_am field access not ported.
    ReleaseSysCache(tuple);

    indnatts = RelationGetNumberOfAttributes(relation);
    if indnatts != IndexRelationGetNumberOfAttributes(relation) {
        elog(ERROR, "relnatts disagrees with indnatts for index in RelationInitIndexAccessInfo");
    }
    indnkeyatts = IndexRelationGetNumberOfKeyAttributes(relation);

    /*
     * Make the private context to hold index access info.
     */
    indexcxt = AllocSetContextCreate(
        CacheMemoryContext as *mut c_void,
        b"index info ".as_ptr() as *const c_char,
        ALLOCSET_SMALL_SIZES,
    );
    (*relation).rd_indexcxt = indexcxt;
    MemoryContextCopyAndSetIdentifier(indexcxt, b"index info ".as_ptr() as *const c_char);

    /*
     * Now we can fetch the index AM's API struct
     */
    InitIndexAmRoutine(relation);

    /*
     * Allocate arrays to hold data. Opclasses are not used for included
     * columns, so allocate them for indnkeyatts only.
     */
    (*relation).rd_opfamily = MemoryContextAllocZero(
        indexcxt,
        indnkeyatts as usize * core::mem::size_of::<Oid>(),
    ) as *mut Oid;
    (*relation).rd_opcintype = MemoryContextAllocZero(
        indexcxt,
        indnkeyatts as usize * core::mem::size_of::<Oid>(),
    ) as *mut Oid;

    // amsupport = relation->rd_indam->amsupport;
    amsupport = 0; // TODO(pg-port): rd_indam field access
    if amsupport > 0 {
        let nsupport = indnatts as usize * amsupport as usize;
        (*relation).rd_support = MemoryContextAllocZero(
            indexcxt,
            nsupport * core::mem::size_of::<RegProcedure>(),
        ) as *mut RegProcedure;
        (*relation).rd_supportinfo = MemoryContextAllocZero(
            indexcxt,
            nsupport * core::mem::size_of::<crate::utils::fmgr::FmgrInfo>(),
        ) as *mut crate::utils::fmgr::FmgrInfo;
    } else {
        (*relation).rd_support = core::ptr::null_mut();
        (*relation).rd_supportinfo = core::ptr::null_mut();
    }

    (*relation).rd_indcollation = MemoryContextAllocZero(
        indexcxt,
        indnkeyatts as usize * core::mem::size_of::<Oid>(),
    ) as *mut Oid;
    (*relation).rd_indoption = MemoryContextAllocZero(
        indexcxt,
        indnkeyatts as usize * core::mem::size_of::<int16>(),
    ) as *mut int16;

    /*
     * indcollation cannot be referenced directly through the C struct,
     * because it comes after the variable-width indkey field.
     */
    isnull = false;
    indcollDatum = fastgetattr(
        (*relation).rd_indextuple as HeapTuple,
        Anum_pg_index_indcollation,
        GetPgIndexDescriptor(),
        &mut isnull,
    );
    // Assert(!isnull);
    indcoll = DatumGetPointer(indcollDatum) as *mut oidvector;
    // memcpy(relation->rd_indcollation, indcoll->values, indnkeyatts * sizeof(Oid));
    // TODO(pg-port): oidvector->values field not ported.

    /*
     * indclass cannot be referenced directly through the C struct, because it
     * comes after the variable-width indkey field.
     */
    isnull = false;
    indclassDatum = fastgetattr(
        (*relation).rd_indextuple as HeapTuple,
        Anum_pg_index_indclass,
        GetPgIndexDescriptor(),
        &mut isnull,
    );
    // Assert(!isnull);
    indclass = DatumGetPointer(indclassDatum) as *mut oidvector;

    /*
     * Fill the support procedure OID array, as well as the info about
     * opfamilies and opclass input types.
     */
    IndexSupportInitialize(
        indclass,
        (*relation).rd_support,
        (*relation).rd_opfamily,
        (*relation).rd_opcintype,
        amsupport,
        indnkeyatts,
    );

    /*
     * Similarly extract indoption and copy it to the cache entry
     */
    isnull = false;
    indoptionDatum = fastgetattr(
        (*relation).rd_indextuple as HeapTuple,
        Anum_pg_index_indoption,
        GetPgIndexDescriptor(),
        &mut isnull,
    );
    // Assert(!isnull);
    indoption = DatumGetPointer(indoptionDatum) as *mut int2vector;
    // memcpy(relation->rd_indoption, indoption->values, indnkeyatts * sizeof(int16));
    // TODO(pg-port): int2vector->values not ported.

    RelationGetIndexAttOptions(relation, false);

    /*
     * expressions, predicate, exclusion caches will be filled later
     */
    (*relation).rd_indexprs = NIL;
    (*relation).rd_indpred = NIL;
    (*relation).rd_exclops = core::ptr::null_mut();
    (*relation).rd_exclprocs = core::ptr::null_mut();
    (*relation).rd_exclstrats = core::ptr::null_mut();
    (*relation).rd_amcache = core::ptr::null_mut();
}

/*
 * IndexSupportInitialize
 *		Initializes an index's cached opclass information,
 *		given the index's pg_index.indclass entry.
 */
unsafe fn IndexSupportInitialize(
    indclass: *mut oidvector,
    indexSupport: *mut RegProcedure,
    opFamily: *mut Oid,
    opcInType: *mut Oid,
    maxSupportNumber: StrategyNumber,
    maxAttributeNumber: c_int,
) {
    for attIndex in 0..maxAttributeNumber {
        let opcentry: *mut OpClassCacheEnt;

        // if (!OidIsValid(indclass->values[attIndex]))
        //     elog(ERROR, "bogus pg_index tuple");
        // TODO(pg-port): oidvector field access not ported.

        /* look up the info for this opclass, using a cache */
        // opcentry = LookupOpclassInfo(indclass->values[attIndex], maxSupportNumber);
        // TODO(pg-port): oidvector field access not ported.
        // opcentry = LookupOpclassInfo(0, maxSupportNumber); // placeholder

        /* copy cached data into relcache entry */
        // *opFamily.add(attIndex as usize) = opcentry->opcfamily;
        // *opcInType.add(attIndex as usize) = opcentry->opcintype;
        // if maxSupportNumber > 0 { ... }
        // TODO(pg-port)
    }
}

/*
 * LookupOpclassInfo
 *
 * This routine maintains a per-opclass cache of the information needed
 * by IndexSupportInitialize().
 */
unsafe fn LookupOpclassInfo(
    operatorClassOid: Oid,
    numSupport: StrategyNumber,
) -> *mut OpClassCacheEnt {
    let mut opcentry: *mut OpClassCacheEnt;
    let mut found: bool = false;
    let rel: Relation;
    let scan: SysScanDesc;
    let mut skey: [ScanKeyData; 3] = [const { core::mem::zeroed() }; 3];
    let mut htup: HeapTuple;
    let mut indexOK: bool;

    if OpClassCache.is_null() {
        /* First time through: initialize the opclass cache */
        let mut ctl: HASHCTL = core::mem::zeroed();

        /* Also make sure CacheMemoryContext exists */
        if CacheMemoryContext.is_null() {
            CreateCacheMemoryContext();
        }

        ctl.keysize = core::mem::size_of::<Oid>();
        ctl.entrysize = core::mem::size_of::<OpClassCacheEnt>();
        OpClassCache = hash_create(
            b"Operator class cache\0".as_ptr() as *const c_char,
            64,
            &ctl,
            HASH_ELEM | HASH_BLOBS,
        );
    }

    opcentry = hash_search(
        OpClassCache,
        &operatorClassOid as *const Oid as *const c_void,
        HASH_ENTER,
        &mut found,
    ) as *mut OpClassCacheEnt;

    if !found {
        /* Initialize new entry */
        (*opcentry).valid = false; /* until known OK */
        (*opcentry).numSupport = numSupport;
        (*opcentry).supportProcs = core::ptr::null_mut(); /* filled below */
    } else {
        // Assert(numSupport == opcentry->numSupport);
    }

    /*
     * When aggressively testing cache-flush hazards, disable the operator
     * class cache.  (DISCARD_CACHES_ENABLED path omitted -- not needed.)
     */

    if (*opcentry).valid {
        return opcentry;
    }

    /*
     * Need to fill in new entry.
     */
    if (*opcentry).supportProcs.is_null() && numSupport > 0 {
        (*opcentry).supportProcs = MemoryContextAllocZero(
            CacheMemoryContext as *mut c_void,
            numSupport as usize * core::mem::size_of::<RegProcedure>(),
        ) as *mut RegProcedure;
    }

    /*
     * To avoid infinite recursion during startup, force heap scans if we're
     * looking up info for the opclasses used by the indexes we would like to
     * reference here.
     */
    indexOK = criticalRelcachesBuilt
        || (operatorClassOid != OID_BTREE_OPS_OID && operatorClassOid != INT2_BTREE_OPS_OID);

    /*
     * We have to fetch the pg_opclass row to determine its opfamily and
     * opcintype.
     */
    ScanKeyInit(
        skey.as_mut_ptr(),
        Anum_pg_opclass_oid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(operatorClassOid),
    );
    let rel = table_open(OperatorClassRelationId, AccessShareLock);
    let scan = systable_beginscan(
        rel,
        OpclassOidIndexId,
        indexOK,
        core::ptr::null_mut(),
        1,
        skey.as_mut_ptr(),
    );

    htup = systable_getnext(scan);
    if HeapTupleIsValid(htup) {
        let opclassform = GETSTRUCT(htup) as Form_pg_opclass;
        // opcentry->opcfamily = opclassform->opcfamily;
        // opcentry->opcintype = opclassform->opcintype;
        // TODO(pg-port): Form_pg_opclass field access not ported.
    } else {
        elog(ERROR, "could not find tuple for opclass in LookupOpclassInfo");
    }

    systable_endscan(scan);
    table_close(rel, AccessShareLock);

    /*
     * Scan pg_amproc to obtain support procs for the opclass.  We only fetch
     * the default ones (those with lefttype = righttype = opcintype).
     */
    if numSupport > 0 {
        ScanKeyInit(
            skey.as_mut_ptr(),
            Anum_pg_amproc_amprocfamily,
            BTEqualStrategyNumber,
            F_OIDEQ,
            ObjectIdGetDatum((*opcentry).opcfamily),
        );
        ScanKeyInit(
            skey.as_mut_ptr().add(1),
            Anum_pg_amproc_amproclefttype,
            BTEqualStrategyNumber,
            F_OIDEQ,
            ObjectIdGetDatum((*opcentry).opcintype),
        );
        ScanKeyInit(
            skey.as_mut_ptr().add(2),
            Anum_pg_amproc_amprocrighttype,
            BTEqualStrategyNumber,
            F_OIDEQ,
            ObjectIdGetDatum((*opcentry).opcintype),
        );
        let rel2 = table_open(AccessMethodProcedureRelationId, AccessShareLock);
        let scan2 = systable_beginscan(
            rel2,
            AccessMethodProcedureIndexId2,
            indexOK,
            core::ptr::null_mut(),
            3,
            skey.as_mut_ptr(),
        );

        loop {
            htup = systable_getnext(scan2);
            if !HeapTupleIsValid(htup) {
                break;
            }
            let amprocform = GETSTRUCT(htup) as Form_pg_amproc;
            // if (amprocform->amprocnum <= 0 ||
            //     (StrategyNumber) amprocform->amprocnum > numSupport)
            //     elog(ERROR, "invalid amproc number %d for opclass %u",
            //          amprocform->amprocnum, operatorClassOid);
            // opcentry->supportProcs[amprocform->amprocnum - 1] = amprocform->amproc;
            // TODO(pg-port): Form_pg_amproc field access not ported.
        }

        systable_endscan(scan2);
        table_close(rel2, AccessShareLock);
    }

    (*opcentry).valid = true;
    opcentry
}

/*
 * Fill in the TableAmRoutine for a relation
 */
unsafe fn InitTableAmRoutine_inner(relation: Relation) {
    (*relation).rd_tableam = GetTableAmRoutine((*relation).rd_amhandler);
}

/*
 * Initialize table access method support for a table like relation
 */
pub unsafe fn RelationInitTableAccessMethod(relation: Relation) {
    let mut tuple: HeapTuple;
    let aform: Form_pg_am;

    if (*(*relation).rd_rel).relkind == RELKIND_SEQUENCE {
        /*
         * Sequences are currently accessed like heap tables, but it doesn't
         * seem prudent to show that in the catalog. So just overwrite it here.
         */
        // Assert(relation->rd_rel->relam == InvalidOid);
        (*relation).rd_amhandler = F_HEAP_TABLEAM_HANDLER;
    } else if IsCatalogRelation(relation) {
        /*
         * Avoid doing a syscache lookup for catalog tables.
         */
        // Assert(relation->rd_rel->relam == HEAP_TABLE_AM_OID);
        (*relation).rd_amhandler = F_HEAP_TABLEAM_HANDLER;
    } else {
        /*
         * Look up the table access method, save the OID of its handler
         * function.
         */
        // Assert(relation->rd_rel->relam != InvalidOid);
        tuple = SearchSysCache1(AMOID, ObjectIdGetDatum((*(*relation).rd_rel).relam));
        if !HeapTupleIsValid(tuple) {
            elog(
                ERROR,
                "cache lookup failed for access method in RelationInitTableAccessMethod",
            );
        }
        aform = GETSTRUCT(tuple) as Form_pg_am;
        // relation->rd_amhandler = aform->amhandler;
        // TODO(pg-port): Form_pg_am field access not ported.
        ReleaseSysCache(tuple);
    }

    /*
     * Now we can fetch the table AM's API struct
     */
    InitTableAmRoutine_inner(relation);
}

/*
 *		formrdesc
 *
 *		This is a special cut-down version of RelationBuildDesc(),
 *		used while initializing the relcache.
 *		The relation descriptor is built just from the supplied parameters,
 *		without actually looking at any system table entries.  We cheat
 *		quite a lot since we only need to work for a few basic system
 *		catalogs.
 *
 * NOTE: we assume we are already switched into CacheMemoryContext.
 */
unsafe fn formrdesc(
    relationName: *const c_char,
    relationReltype: Oid,
    isshared: bool,
    natts: c_int,
    attrs: *const FormData_pg_attribute,
) {
    let relation: Relation;
    let mut has_not_null: bool;

    /*
     * allocate new relation desc, clear all fields of reldesc
     */
    relation = palloc0(core::mem::size_of::<RelationData>()) as Relation;

    /* make sure relation is marked as having no open file yet */
    (*relation).rd_smgr = core::ptr::null_mut();

    /*
     * initialize reference count: 1 because it is nailed in cache
     */
    (*relation).rd_refcnt = 1;

    /*
     * all entries built with this routine are nailed-in-cache; none are for
     * new or temp relations.
     */
    (*relation).rd_isnailed = true;
    (*relation).rd_createSubid = InvalidSubTransactionId;
    (*relation).rd_newRelfilelocatorSubid = InvalidSubTransactionId;
    (*relation).rd_firstRelfilelocatorSubid = InvalidSubTransactionId;
    (*relation).rd_droppedSubid = InvalidSubTransactionId;
    (*relation).rd_backend = INVALID_PROC_NUMBER;
    (*relation).rd_islocaltemp = false;

    /*
     * initialize relation tuple form
     *
     * The data we insert here is pretty incomplete/bogus, but it'll serve to
     * get us launched.  RelationCacheInitializePhase3() will read the real
     * data from pg_class and replace what we've done here.
     */
    (*relation).rd_rel = palloc0(CLASS_TUPLE_SIZE) as Form_pg_class;

    namestrcpy(core::ptr::addr_of_mut!((*(*relation).rd_rel).relname), relationName);
    (*(*relation).rd_rel).relnamespace = PG_CATALOG_NAMESPACE;
    (*(*relation).rd_rel).reltype = relationReltype;

    /*
     * It's important to distinguish between shared and non-shared relations,
     * even at bootstrap time.
     */
    (*(*relation).rd_rel).relisshared = isshared;
    if isshared {
        (*(*relation).rd_rel).reltablespace = GLOBALTABLESPACE_OID;
    }

    /* formrdesc is used only for permanent relations */
    (*(*relation).rd_rel).relpersistence = RELPERSISTENCE_PERMANENT;

    /* ... and they're always populated, too */
    (*(*relation).rd_rel).relispopulated = true;

    (*(*relation).rd_rel).relreplident = REPLICA_IDENTITY_NOTHING;
    (*(*relation).rd_rel).relpages = 0;
    (*(*relation).rd_rel).reltuples = -1.0; // float4 in C
    (*(*relation).rd_rel).relallvisible = 0;
    (*(*relation).rd_rel).relallfrozen = 0;
    (*(*relation).rd_rel).relkind = RELKIND_RELATION;
    (*(*relation).rd_rel).relnatts = natts as int16;

    /*
     * initialize attribute tuple form
     */
    (*relation).rd_att = CreateTemplateTupleDesc(natts);
    (*(*relation).rd_att).tdrefcount = 1; /* mark as refcounted */

    (*(*relation).rd_att).tdtypeid = relationReltype;
    (*(*relation).rd_att).tdtypmod = -1; /* just to be sure */

    /*
     * initialize tuple desc info
     */
    has_not_null = false;
    for i in 0..natts {
        core::ptr::copy_nonoverlapping(
            attrs.add(i as usize) as *const u8,
            TupleDescAttr((*relation).rd_att, i) as *mut u8,
            ATTRIBUTE_FIXED_PART_SIZE,
        );
        // has_not_null |= attrs[i].attnotnull;
        // TODO(pg-port): FormData_pg_attribute field access not ported.
        populate_compact_attribute((*relation).rd_att, i);
    }

    /* initialize first attribute's attcacheoff */
    // TupleDescCompactAttr(relation->rd_att, 0)->attcacheoff = 0;
    // TODO(pg-port)

    /* mark not-null status */
    if has_not_null {
        let constr = palloc0(core::mem::size_of::<TupleConstr>()) as *mut TupleConstr;
        (*(constr as *mut TupleConstrStub)).has_not_null = true;
        (*(*relation).rd_att).constr = constr as *mut crate::access::common::tupdesc::TupleConstr;
    }

    /*
     * initialize relation id from info in att array (my, this is ugly)
     */
    (*relation).rd_id = (*(TupleDescAttr((*relation).rd_att, 0) as *mut FormData_pg_attribute_stub)).attrelid;

    /*
     * All relations made with formrdesc are mapped.
     */
    (*(*relation).rd_rel).relfilenode = InvalidRelFileNumber;
    if IsBootstrapProcessingMode() {
        RelationMapUpdateMap((*relation).rd_id, (*relation).rd_id, isshared, true);
    }

    /*
     * initialize the relation lock manager information
     */
    RelationInitLockInfo(relation); /* see lmgr.c */

    /*
     * initialize physical addressing information for the relation
     */
    RelationInitPhysicalAddr(relation);

    /*
     * initialize the table am handler
     */
    (*(*relation).rd_rel).relam = HEAP_TABLE_AM_OID;
    (*relation).rd_tableam = GetHeapamTableAmRoutine();

    /*
     * initialize the rel-has-index flag, using hardwired knowledge
     */
    if IsBootstrapProcessingMode() {
        /* In bootstrap mode, we have no indexes */
        (*(*relation).rd_rel).relhasindex = false;
    } else {
        /* Otherwise, all the rels formrdesc is used for have indexes */
        (*(*relation).rd_rel).relhasindex = true;
    }

    /*
     * add new reldesc to relcache
     */
    RelationCacheInsert(relation, false);

    /* It's fully valid */
    (*relation).rd_isvalid = true;
}

/*
 *		AssertCouldGetRelation
 *
 *		Check safety of calling RelationIdGetRelation().
 */
pub unsafe fn AssertCouldGetRelation_pub() {
    // Assert(IsTransactionState());
    // AssertBufferLocksPermitCatalogRead();
    // TODO(pg-port): debug assertions only.
}

/*
 *		RelationIdGetRelation
 *
 *		Lookup a reldesc by OID; make one if not already in cache.
 *
 *		Returns NULL if no pg_class row could be found for the given relid
 *		(suggesting we are trying to access a just-deleted relation).
 *		Any other error is reported via elog.
 *
 *		NB: caller should already have at least AccessShareLock on the
 *		relation ID, else there are nasty race conditions.
 *
 *		NB: relation ref count is incremented, or set to 1 if new entry.
 *		Caller should eventually decrement count.  (Usually,
 *		that happens by calling RelationClose().)
 */
pub unsafe fn RelationIdGetRelation(relationId: Oid) -> Relation {
    let mut rd: Relation = core::ptr::null_mut();

    AssertCouldGetRelation();

    /*
     * first try to find reldesc in the cache
     */
    RelationIdCacheLookup(relationId, &mut rd);

    if RelationIsValid(rd) {
        /* return NULL for dropped relations */
        if (*rd).rd_droppedSubid != InvalidSubTransactionId {
            // Assert(!rd->rd_isvalid);
            return core::ptr::null_mut();
        }

        RelationIncrementReferenceCount(rd);
        /* revalidate cache entry if necessary */
        if !(*rd).rd_isvalid {
            RelationRebuildRelation(rd);

            /*
             * Normally entries need to be valid here, but before the relcache
             * has been initialized, not enough infrastructure exists to
             * perform pg_class lookups. The structure of such entries doesn't
             * change, but we still want to update the rd_rel entry.
             */
            // Assert(rd->rd_isvalid || (rd->rd_isnailed && !criticalRelcachesBuilt));
        }
        return rd;
    }

    /*
     * no reldesc in the cache, so have RelationBuildDesc() build one and add it.
     */
    rd = RelationBuildDesc(relationId, true);
    if RelationIsValid(rd) {
        RelationIncrementReferenceCount(rd);
    }
    rd
}

// ===========================================================================
// Part 5: Cache invalidation support, RelationIncrementReferenceCount,
//         RelationDecrementReferenceCount, RelationClose,
//         RelationReloadIndexInfo, RelationReloadNailed,
//         RelationDestroyRelation, RelationInvalidateRelation,
//         RelationClearRelation, RelationRebuildRelation,
//         RelationFlushRelation, RelationForgetRelation,
//         RelationCacheInvalidateEntry, RelationCacheInvalidate,
//         RememberToFreeTupleDescAtEOX, AssertPendingSyncConsistency
// ===========================================================================

/*
 * RelationIncrementReferenceCount
 *		Increments relation reference count.
 */
pub unsafe fn RelationIncrementReferenceCount(rel: Relation) {
    ResourceOwnerEnlarge(CurrentResourceOwner);
    (*rel).rd_refcnt += 1;
    if !IsBootstrapProcessingMode() {
        ResourceOwnerRememberRelationRef(CurrentResourceOwner, rel);
    }
}

/*
 * RelationDecrementReferenceCount
 *		Decrements relation reference count.
 */
pub unsafe fn RelationDecrementReferenceCount(rel: Relation) {
    debug_assert!((*rel).rd_refcnt > 0);
    (*rel).rd_refcnt -= 1;
    if !IsBootstrapProcessingMode() {
        ResourceOwnerForgetRelationRef(CurrentResourceOwner, rel);
    }
}

/*
 * RelationClose - close an open relation
 *
 *	Actually, we just decrement the refcount.
 */
pub unsafe fn RelationClose(relation: Relation) {
    /* Note: no locking manipulations needed */
    RelationDecrementReferenceCount(relation);

    RelationCloseCleanup(relation);
}

unsafe fn RelationCloseCleanup(relation: Relation) {
    /*
     * If the relation is no longer open in this session, we can clean up any
     * stale partition descriptors it has.
     */
    if RelationHasReferenceCountZero(relation) {
        if !(*relation).rd_pdcxt.is_null() {
            // if (relation->rd_pdcxt->firstchild != NULL)
            //     MemoryContextDeleteChildren(relation->rd_pdcxt);
            // TODO(pg-port): MemoryContextData->firstchild field not ported.
            MemoryContextDeleteChildren((*relation).rd_pdcxt);
        }
        if !(*relation).rd_pddcxt.is_null() {
            MemoryContextDeleteChildren((*relation).rd_pddcxt);
        }
    }

    // #ifdef RELCACHE_FORCE_RELEASE -- omitted
}

/*
 * RelationReloadIndexInfo - reload minimal information for an open index
 */
unsafe fn RelationReloadIndexInfo(relation: Relation) {
    let mut indexOK: bool;
    let mut pg_class_tuple: HeapTuple;
    let relp: Form_pg_class;

    /* Should be called only for invalidated, live indexes */
    // Assert((relkind == RELKIND_INDEX || ...) && !relation->rd_isvalid && ...);

    /*
     * If it's a shared index, we might be called before backend startup has
     * finished selecting a database.
     */
    if (*(*relation).rd_rel).relisshared && !criticalRelcachesBuilt {
        RelationInitPhysicalAddr(relation);
        (*relation).rd_isvalid = true;
        return;
    }

    /*
     * Read the pg_class row
     *
     * Don't try to use an indexscan of pg_class_oid_index to reload the info
     * for pg_class_oid_index ...
     */
    indexOK = RelationGetRelid(relation) != ClassOidIndexId;
    pg_class_tuple = ScanPgRelation(RelationGetRelid(relation), indexOK, false);
    if !HeapTupleIsValid(pg_class_tuple) {
        elog(
            ERROR,
            "could not find pg_class tuple for index in RelationReloadIndexInfo",
        );
    }
    relp = GETSTRUCT(pg_class_tuple) as Form_pg_class;
    core::ptr::copy_nonoverlapping(
        relp as *const u8,
        (*relation).rd_rel as *mut u8,
        CLASS_TUPLE_SIZE,
    );
    /* Reload reloptions in case they changed */
    if !(*relation).rd_options.is_null() {
        pfree((*relation).rd_options as *mut c_void);
    }
    RelationParseRelOptions(relation, pg_class_tuple);
    /* done with pg_class tuple */
    heap_freetuple(pg_class_tuple);
    /* We must recalculate physical address in case it changed */
    RelationInitPhysicalAddr(relation);

    /*
     * For a non-system index, there are fields of the pg_index row that are
     * allowed to change.
     */
    if !IsSystemRelation(relation) {
        let mut tuple: HeapTuple;
        let index: Form_pg_index;

        tuple = SearchSysCache1(
            INDEXRELID,
            ObjectIdGetDatum(RelationGetRelid(relation)),
        );
        if !HeapTupleIsValid(tuple) {
            elog(
                ERROR,
                "cache lookup failed for index in RelationReloadIndexInfo",
            );
        }
        index = GETSTRUCT(tuple) as Form_pg_index;

        /*
         * Basically, let's just copy all the bool fields.
         */
        // relation->rd_index->indisunique = index->indisunique;
        // etc. -- TODO(pg-port): Form_pg_index field access not ported.

        /* Copy xmin too */
        // HeapTupleHeaderSetXmin(relation->rd_indextuple->t_data,
        //                        HeapTupleHeaderGetXmin(tuple->t_data));
        // TODO(pg-port): HeapTupleHeader field access not ported.

        ReleaseSysCache(tuple);
    }

    /* Okay, now it's valid again */
    (*relation).rd_isvalid = true;
}

/*
 * RelationReloadNailed - reload minimal information for nailed relations.
 */
unsafe fn RelationReloadNailed(relation: Relation) {
    /* Should be called only for invalidated, nailed relations */
    // Assert(!relation->rd_isvalid);
    // Assert(relation->rd_isnailed);
    // Assert(relation->rd_rel->relkind == RELKIND_RELATION);
    AssertCouldGetRelation();

    /*
     * Redo RelationInitPhysicalAddr in case it is a mapped relation whose
     * mapping changed.
     */
    RelationInitPhysicalAddr(relation);

    /*
     * Reload a non-index entry.
     */
    if criticalRelcachesBuilt {
        let mut pg_class_tuple: HeapTuple;
        let relp: Form_pg_class;

        /* NB: Mark the entry as valid before starting to scan */
        (*relation).rd_isvalid = true;

        pg_class_tuple = ScanPgRelation(RelationGetRelid(relation), true, false);
        relp = GETSTRUCT(pg_class_tuple) as Form_pg_class;
        core::ptr::copy_nonoverlapping(
            relp as *const u8,
            (*relation).rd_rel as *mut u8,
            CLASS_TUPLE_SIZE,
        );
        heap_freetuple(pg_class_tuple);

        /* Again mark as valid */
        (*relation).rd_isvalid = true;
    }
}

/*
 * RelationDestroyRelation
 *
 *	Physically delete a relation cache entry and all subsidiary data.
 */
unsafe fn RelationDestroyRelation(relation: Relation, remember_tupdesc: bool) {
    debug_assert!(RelationHasReferenceCountZero(relation));

    /*
     * Make sure smgr and lower levels close the relation's files, if they
     * weren't closed already.
     */
    RelationCloseSmgr(relation);

    /* break mutual link with stats entry */
    pgstat_unlink_relation(relation);

    /*
     * Free all the subsidiary data structures of the relcache entry, then
     * the entry itself.
     */
    if !(*relation).rd_rel.is_null() {
        pfree((*relation).rd_rel as *mut c_void);
    }
    /* can't use DecrTupleDescRefCount here */
    debug_assert!((*(*relation).rd_att).tdrefcount > 0);
    (*(*relation).rd_att).tdrefcount -= 1;
    if (*(*relation).rd_att).tdrefcount == 0 {
        /*
         * If we Rebuilt a relcache entry during a transaction then its
         * possible we did that because the TupDesc changed as the result of
         * an ALTER TABLE.
         */
        if remember_tupdesc {
            RememberToFreeTupleDescAtEOX((*relation).rd_att);
        } else {
            FreeTupleDesc((*relation).rd_att);
        }
    }
    FreeTriggerDesc((*relation).trigdesc);
    list_free_deep((*relation).rd_fkeylist);
    list_free((*relation).rd_indexlist);
    list_free((*relation).rd_statlist);
    bms_free((*relation).rd_keyattr);
    bms_free((*relation).rd_pkattr);
    bms_free((*relation).rd_idattr);
    bms_free((*relation).rd_hotblockingattr);
    bms_free((*relation).rd_summarizedattr);
    if !(*relation).rd_pubdesc.is_null() {
        pfree((*relation).rd_pubdesc as *mut c_void);
    }
    if !(*relation).rd_options.is_null() {
        pfree((*relation).rd_options as *mut c_void);
    }
    if !(*relation).rd_indextuple.is_null() {
        pfree((*relation).rd_indextuple as *mut c_void);
    }
    if !(*relation).rd_amcache.is_null() {
        pfree((*relation).rd_amcache);
    }
    if !(*relation).rd_fdwroutine.is_null() {
        pfree((*relation).rd_fdwroutine as *mut c_void);
    }
    if !(*relation).rd_indexcxt.is_null() {
        MemoryContextDelete((*relation).rd_indexcxt);
    }
    if !(*relation).rd_rulescxt.is_null() {
        MemoryContextDelete((*relation).rd_rulescxt);
    }
    if !(*relation).rd_rsdesc.is_null() {
        // MemoryContextDelete(relation->rd_rsdesc->rscxt);
        // TODO(pg-port): RowSecurityDesc field access not ported.
    }
    if !(*relation).rd_partkeycxt.is_null() {
        MemoryContextDelete((*relation).rd_partkeycxt);
    }
    if !(*relation).rd_pdcxt.is_null() {
        MemoryContextDelete((*relation).rd_pdcxt);
    }
    if !(*relation).rd_pddcxt.is_null() {
        MemoryContextDelete((*relation).rd_pddcxt);
    }
    if !(*relation).rd_partcheckcxt.is_null() {
        MemoryContextDelete((*relation).rd_partcheckcxt);
    }
    pfree(relation as *mut c_void);
}

/*
 * RelationInvalidateRelation - mark a relation cache entry as invalid
 */
unsafe fn RelationInvalidateRelation(relation: Relation) {
    RelationCloseSmgr(relation);

    /* Free AM cached data, if any */
    if !(*relation).rd_amcache.is_null() {
        pfree((*relation).rd_amcache);
    }
    (*relation).rd_amcache = core::ptr::null_mut();

    (*relation).rd_isvalid = false;
}

/*
 * RelationClearRelation - physically blow away a relation cache entry
 */
unsafe fn RelationClearRelation(relation: Relation) {
    debug_assert!(RelationHasReferenceCountZero(relation));
    debug_assert!(!(*relation).rd_isnailed);

    /* Relations created in the same transaction must never be removed */
    debug_assert_eq!((*relation).rd_createSubid, InvalidSubTransactionId);
    debug_assert_eq!((*relation).rd_firstRelfilelocatorSubid, InvalidSubTransactionId);
    debug_assert_eq!((*relation).rd_droppedSubid, InvalidSubTransactionId);

    /* first mark it as invalid */
    RelationInvalidateRelation(relation);

    /* Remove it from the hash table */
    RelationCacheDelete(relation);

    /* And release storage */
    RelationDestroyRelation(relation, false);
}

/*
 * RelationRebuildRelation - rebuild a relation cache entry in place
 *
 * Reset and rebuild a relation cache entry from scratch.
 */
unsafe fn RelationRebuildRelation(relation: Relation) {
    debug_assert!(!RelationHasReferenceCountZero(relation));
    AssertCouldGetRelation();
    /* there is no reason to ever rebuild a dropped relation */
    debug_assert_eq!((*relation).rd_droppedSubid, InvalidSubTransactionId);

    /* Close and mark it as invalid until we've finished the rebuild */
    RelationInvalidateRelation(relation);

    let relkind = (*(*relation).rd_rel).relkind;

    /*
     * Indexes only have a limited number of possible schema changes.
     */
    if (relkind == RELKIND_INDEX || relkind == RELKIND_PARTITIONED_INDEX)
        && !(*relation).rd_indexcxt.is_null()
    {
        RelationReloadIndexInfo(relation);
        return;
    }
    /* Nailed relations are handled separately. */
    else if (*relation).rd_isnailed {
        RelationReloadNailed(relation);
        return;
    } else {
        /*
         * Our strategy for rebuilding an open relcache entry is to build a
         * new entry from scratch, swap its contents with the old entry, and
         * finally delete the new entry.
         */
        let save_relid: Oid = RelationGetRelid(relation);
        let mut keep_tupdesc: bool;
        let mut keep_rules: bool;
        let mut keep_policies: bool;
        let mut keep_partkey: bool;

        /* Build temporary entry, but don't link it into hashtable */
        let newrel = RelationBuildDesc(save_relid, false);

        if newrel.is_null() {
            /*
             * We can validly get here, if we're using a historic snapshot in
             * which a relation, accessed from outside logical decoding, is
             * still invisible.
             */
            if HistoricSnapshotActive() {
                return;
            }

            /*
             * This shouldn't happen as dropping a relation is intended to be
             * impossible if still referenced.
             */
            elog(
                ERROR,
                "relation deleted while still in use in RelationRebuildRelation",
            );
        }

        debug_assert_eq!((*(*relation).rd_rel).relkind, (*(*newrel).rd_rel).relkind);

        keep_tupdesc = equalTupleDescs((*relation).rd_att, (*newrel).rd_att);
        keep_rules = equalRuleLocks((*relation).rd_rules, (*newrel).rd_rules);
        keep_policies = equalRSDesc((*relation).rd_rsdesc as *const RowSecurityDesc, (*newrel).rd_rsdesc as *const RowSecurityDesc);
        /* partkey is immutable once set up, so we can always keep it */
        keep_partkey = !(*relation).rd_partkey.is_null();

        /*
         * Perform swapping of the relcache entry contents.  Within this
         * process the old entry is momentarily invalid, so there *must* be no
         * possibility of CHECK_FOR_INTERRUPTS within this sequence.
         */

        /* swap all Relation struct fields */
        {
            let mut tmpstruct: RelationData = core::mem::zeroed();
            core::ptr::copy_nonoverlapping(newrel as *const RelationData, &mut tmpstruct, 1);
            core::ptr::copy_nonoverlapping(relation as *const RelationData, newrel as *mut RelationData, 1);
            core::ptr::copy_nonoverlapping(&tmpstruct as *const RelationData, relation, 1);
        }

        macro_rules! SWAPFIELD {
            ($fldtype:ty, $fld:ident) => {{
                let _tmp = (*newrel).$fld;
                (*newrel).$fld = (*relation).$fld;
                (*relation).$fld = _tmp;
            }};
        }

        /* rd_smgr must not be swapped, due to back-links from smgr level */
        SWAPFIELD!(OpaquePtr, rd_smgr);
        /* rd_refcnt must be preserved */
        SWAPFIELD!(c_int, rd_refcnt);
        /* isnailed shouldn't change */
        debug_assert_eq!((*newrel).rd_isnailed, (*relation).rd_isnailed);
        /* creation sub-XIDs must be preserved */
        SWAPFIELD!(SubTransactionId, rd_createSubid);
        SWAPFIELD!(SubTransactionId, rd_newRelfilelocatorSubid);
        SWAPFIELD!(SubTransactionId, rd_firstRelfilelocatorSubid);
        SWAPFIELD!(SubTransactionId, rd_droppedSubid);
        /* un-swap rd_rel pointers, swap contents instead */
        SWAPFIELD!(Form_pg_class, rd_rel);
        /* ... but actually, we don't have to update newrel->rd_rel */
        core::ptr::copy_nonoverlapping(
            (*newrel).rd_rel as *const u8,
            (*relation).rd_rel as *mut u8,
            CLASS_TUPLE_SIZE,
        );
        /* preserve old tupledesc, rules, policies if no logical change */
        if keep_tupdesc {
            SWAPFIELD!(TupleDesc, rd_att);
        }
        if keep_rules {
            SWAPFIELD!(*mut RuleLock, rd_rules);
            SWAPFIELD!(*mut c_void, rd_rulescxt);
        }
        if keep_policies {
            SWAPFIELD!(*mut RowSecurityDesc, rd_rsdesc);
        }
        /* toast OID override must be preserved */
        SWAPFIELD!(Oid, rd_toastoid);
        /* pgstat_info / enabled must be preserved */
        SWAPFIELD!(*mut c_void, pgstat_info);
        SWAPFIELD!(bool, pgstat_enabled);
        /* preserve old partition key if we have one */
        if keep_partkey {
            SWAPFIELD!(PartitionKey, rd_partkey);
            SWAPFIELD!(*mut c_void, rd_partkeycxt);
        }
        if !(*newrel).rd_pdcxt.is_null() || !(*newrel).rd_pddcxt.is_null() {
            /*
             * We are rebuilding a partitioned relation with a non-zero
             * reference count, so we must keep the old partition descriptor
             * around, in case there's a PartitionDirectory with a pointer to
             * it.
             */
            (*relation).rd_partdesc = core::ptr::null_mut();
            (*relation).rd_partdesc_nodetached = core::ptr::null_mut();
            (*relation).rd_partdesc_nodetached_xmin = InvalidTransactionId;
            if !(*relation).rd_pdcxt.is_null() {
                MemoryContextSetParent((*newrel).rd_pdcxt, (*relation).rd_pdcxt);
            } else {
                (*relation).rd_pdcxt = (*newrel).rd_pdcxt;
            }
            if !(*relation).rd_pddcxt.is_null() {
                MemoryContextSetParent((*newrel).rd_pddcxt, (*relation).rd_pddcxt);
            } else {
                (*relation).rd_pddcxt = (*newrel).rd_pddcxt;
            }
            /* drop newrel's pointers so we don't destroy it below */
            (*newrel).rd_partdesc = core::ptr::null_mut();
            (*newrel).rd_partdesc_nodetached = core::ptr::null_mut();
            (*newrel).rd_partdesc_nodetached_xmin = InvalidTransactionId;
            (*newrel).rd_pdcxt = core::ptr::null_mut();
            (*newrel).rd_pddcxt = core::ptr::null_mut();
        }

        /* And now we can throw away the temporary entry */
        RelationDestroyRelation(newrel, !keep_tupdesc);
    }
}

/*
 * RelationFlushRelation
 *
 *	 Rebuild the relation if it is open (refcount > 0), else blow it away.
 */
unsafe fn RelationFlushRelation(relation: Relation) {
    if (*relation).rd_createSubid != InvalidSubTransactionId
        || (*relation).rd_firstRelfilelocatorSubid != InvalidSubTransactionId
    {
        /*
         * New relcache entries are always rebuilt, not flushed.
         */
        if IsTransactionState()
            && (*relation).rd_droppedSubid == InvalidSubTransactionId
        {
            /*
             * The rel could have zero refcnt here, so temporarily increment
             * the refcnt to ensure it's safe to rebuild it.
             */
            RelationIncrementReferenceCount(relation);
            RelationRebuildRelation(relation);
            RelationDecrementReferenceCount(relation);
        } else {
            RelationInvalidateRelation(relation);
        }
    } else {
        /*
         * Pre-existing rels can be dropped from the relcache if not open.
         */
        if RelationHasReferenceCountZero(relation) {
            RelationClearRelation(relation);
        } else if !IsTransactionState() {
            RelationInvalidateRelation(relation);
        } else if (*relation).rd_isnailed && (*relation).rd_refcnt == 1 {
            /*
             * A nailed relation with refcnt == 1 is unused.
             */
            RelationInvalidateRelation(relation);
        } else {
            RelationRebuildRelation(relation);
        }
    }
}

/*
 * RelationForgetRelation - caller reports that it dropped the relation
 */
pub unsafe fn RelationForgetRelation(rid: Oid) {
    let mut relation: Relation = core::ptr::null_mut();

    RelationIdCacheLookup(rid, &mut relation);

    if !PointerIsValid(relation as *const c_void) {
        return; /* not in cache, nothing to do */
    }

    if !RelationHasReferenceCountZero(relation) {
        elog(ERROR, "relation is still open in RelationForgetRelation");
    }

    debug_assert_eq!((*relation).rd_droppedSubid, InvalidSubTransactionId);
    if (*relation).rd_createSubid != InvalidSubTransactionId
        || (*relation).rd_firstRelfilelocatorSubid != InvalidSubTransactionId
    {
        /*
         * In the event of subtransaction rollback, we must not forget
         * rd_*Subid.  Mark the entry "dropped" and invalidate it.
         */
        (*relation).rd_droppedSubid = GetCurrentSubTransactionId();
        RelationInvalidateRelation(relation);
    } else {
        RelationClearRelation(relation);
    }
}

/*
 *		RelationCacheInvalidateEntry
 *
 *		This routine is invoked for SI cache flush messages.
 */
pub unsafe fn RelationCacheInvalidateEntry(relationId: Oid) {
    let mut relation: Relation = core::ptr::null_mut();

    RelationIdCacheLookup(relationId, &mut relation);

    if PointerIsValid(relation as *const c_void) {
        relcacheInvalsReceived += 1;
        RelationFlushRelation(relation);
    } else {
        for i in 0..in_progress_list_len {
            if (*in_progress_list.add(i as usize)).reloid == relationId {
                (*in_progress_list.add(i as usize)).invalidated = true;
            }
        }
    }
}

/*
 * RelationCacheInvalidate
 *	 Blow away cached relation descriptors that have zero reference counts,
 *	 and rebuild those with positive reference counts.
 */
pub unsafe fn RelationCacheInvalidate(debug_discard: bool) {
    let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut idhentry: *mut RelIdCacheEnt;
    let mut relation: Relation;
    let mut rebuildFirstList: *mut List = NIL;
    let mut rebuildList: *mut List = NIL;
    let mut l: *mut c_void;

    /*
     * Reload relation mapping data before starting to reconstruct cache.
     */
    RelationMapInvalidateAll();

    /* Phase 1 */
    hash_seq_init(&mut status, RelationIdCache);

    loop {
        idhentry = hash_seq_search(&mut status) as *mut RelIdCacheEnt;
        if idhentry.is_null() {
            break;
        }
        relation = (*idhentry).reldesc;

        /*
         * Ignore new relations.
         */
        if (*relation).rd_createSubid != InvalidSubTransactionId
            || (*relation).rd_firstRelfilelocatorSubid != InvalidSubTransactionId
        {
            continue;
        }

        relcacheInvalsReceived += 1;

        if RelationHasReferenceCountZero(relation) {
            /* Delete this entry immediately */
            RelationClearRelation(relation);
        } else {
            /*
             * If it's a mapped relation, immediately update its rd_locator.
             */
            if RelationIsMapped(relation) {
                RelationCloseSmgr(relation);
                RelationInitPhysicalAddr(relation);
            }

            /*
             * Add this entry to list of stuff to rebuild in second pass.
             */
            if RelationGetRelid(relation) == RelationRelationId {
                rebuildFirstList = lcons(relation as *mut c_void, rebuildFirstList);
            } else if RelationGetRelid(relation) == ClassOidIndexId {
                rebuildFirstList = lappend(rebuildFirstList, relation as *mut c_void);
            } else if (*relation).rd_isnailed {
                rebuildList = lcons(relation as *mut c_void, rebuildList);
            } else {
                rebuildList = lappend(rebuildList, relation as *mut c_void);
            }
        }
    }

    /*
     * We cannot destroy the SMgrRelations as there might still be references
     * to them, but close the underlying file descriptors.
     */
    smgrreleaseall();

    /*
     * Phase 2: rebuild (or invalidate) the items found to need rebuild in phase 1
     */
    // foreach(l, rebuildFirstList) { ... }
    // TODO(pg-port): foreach macro / list iteration not ported here; using placeholder.
    list_free(rebuildFirstList);
    // foreach(l, rebuildList) { ... }
    list_free(rebuildList);

    if !debug_discard {
        /* Any RelationBuildDesc() on the stack must start over. */
        for i in 0..in_progress_list_len {
            (*in_progress_list.add(i as usize)).invalidated = true;
        }
    }
}

unsafe fn RememberToFreeTupleDescAtEOX(td: TupleDesc) {
    if EOXactTupleDescArray.is_null() {
        let mut oldcxt: *mut c_void;

        oldcxt = MemoryContextSwitchTo(CacheMemoryContext as *mut c_void);

        EOXactTupleDescArray =
            palloc(16 * core::mem::size_of::<TupleDesc>()) as *mut TupleDesc;
        EOXactTupleDescArrayLen = 16;
        NextEOXactTupleDescNum = 0;
        MemoryContextSwitchTo(oldcxt);
    } else if NextEOXactTupleDescNum >= EOXactTupleDescArrayLen {
        let newlen = EOXactTupleDescArrayLen * 2;

        debug_assert!(EOXactTupleDescArrayLen > 0);

        EOXactTupleDescArray = repalloc(
            EOXactTupleDescArray as *mut c_void,
            newlen as usize * core::mem::size_of::<TupleDesc>(),
        ) as *mut TupleDesc;
        EOXactTupleDescArrayLen = newlen;
    }

    *EOXactTupleDescArray.add(NextEOXactTupleDescNum as usize) = td;
    NextEOXactTupleDescNum += 1;
}

/*
 * AssertPendingSyncConsistency -- only compiled with USE_ASSERT_CHECKING.
 * We include it unconditionally here (as a no-op release build).
 */
unsafe fn AssertPendingSyncConsistency(_relation: Relation) {
    // debug only -- TODO(pg-port): RelFileLocatorSkippingWAL not ported.
}

/*
 * AssertPendingSyncs_RelationCache
 *
 *	Assert that relcache.c and storage.c agree on whether to skip WAL.
 */
pub unsafe fn AssertPendingSyncs_RelationCache() {
    // TODO(pg-port): USE_ASSERT_CHECKING only; no-op in release.
}

// ===========================================================================
// Part 6: AtEOXact_RelationCache, AtEOSubXact_RelationCache,
//         RelationBuildLocalRelation, RelationSetNewRelfilenumber,
//         RelationAssumeNewRelfilelocator
// ===========================================================================

/*
 * AtEOXact_RelationCache
 *
 *	Clean up the relcache at main-transaction commit or abort.
 */
pub unsafe fn AtEOXact_RelationCache(isCommit: bool) {
    let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut idhentry: *mut RelIdCacheEnt;
    let mut i: c_int;

    /*
     * Forget in_progress_list.  This is relevant when we're aborting due to
     * an error during RelationBuildDesc().
     */
    debug_assert!(in_progress_list_len == 0 || !isCommit);
    in_progress_list_len = 0;

    /*
     * Unless the eoxact_list[] overflowed, we only need to examine the rels
     * listed in it.  Otherwise fall back on a hash_seq_search scan.
     */
    if eoxact_list_overflowed {
        hash_seq_init(&mut status, RelationIdCache);
        loop {
            idhentry = hash_seq_search(&mut status) as *mut RelIdCacheEnt;
            if idhentry.is_null() {
                break;
            }
            AtEOXact_cleanup((*idhentry).reldesc, isCommit);
        }
    } else {
        i = 0;
        while i < eoxact_list_len {
            let found: *mut bool = core::ptr::null_mut();
            idhentry = hash_search(
                RelationIdCache,
                &eoxact_list[i as usize] as *const Oid as *const c_void,
                HASH_FIND,
                core::ptr::null_mut(),
            ) as *mut RelIdCacheEnt;
            if !idhentry.is_null() {
                AtEOXact_cleanup((*idhentry).reldesc, isCommit);
            }
            i += 1;
        }
    }

    if EOXactTupleDescArrayLen > 0 {
        debug_assert!(!EOXactTupleDescArray.is_null());
        i = 0;
        while i < NextEOXactTupleDescNum {
            FreeTupleDesc(*EOXactTupleDescArray.add(i as usize));
            i += 1;
        }
        pfree(EOXactTupleDescArray as *mut c_void);
        EOXactTupleDescArray = core::ptr::null_mut();
    }

    /* Now we're out of the transaction and can clear the lists */
    eoxact_list_len = 0;
    eoxact_list_overflowed = false;
    NextEOXactTupleDescNum = 0;
    EOXactTupleDescArrayLen = 0;
}

/*
 * AtEOXact_cleanup
 *
 *	Clean up a single rel at main-transaction commit or abort
 */
unsafe fn AtEOXact_cleanup(relation: Relation, isCommit: bool) {
    let mut clear_relcache: bool = false;

    /*
     * The relcache entry's ref count should be back to its normal
     * not-in-a-transaction state: 0 unless it's nailed in cache.
     */
    // #ifdef USE_ASSERT_CHECKING -- omitted for release build

    /*
     * Is the relation live after this transaction ends?
     */
    clear_relcache = if isCommit {
        (*relation).rd_droppedSubid != InvalidSubTransactionId
    } else {
        (*relation).rd_createSubid != InvalidSubTransactionId
    };

    /*
     * Since we are now out of the transaction, reset the subids to zero.
     */
    (*relation).rd_createSubid = InvalidSubTransactionId;
    (*relation).rd_newRelfilelocatorSubid = InvalidSubTransactionId;
    (*relation).rd_firstRelfilelocatorSubid = InvalidSubTransactionId;
    (*relation).rd_droppedSubid = InvalidSubTransactionId;

    if clear_relcache {
        if RelationHasReferenceCountZero(relation) {
            RelationClearRelation(relation);
            return;
        } else {
            /*
             * Hmm, somewhere there's a (leaked?) reference to the relation.
             * We daren't remove the entry for fear of dereferencing a
             * dangling pointer later.  Bleat, and mark it as not belonging to
             * the current transaction.  Hopefully it'll get cleaned up
             * eventually.  This must be just a WARNING to avoid
             * error-during-error-recovery loops.
             */
            elog(
                WARNING,
                "cannot remove relcache entry because it has nonzero refcount in AtEOXact_cleanup",
            );
        }
    }
}

/*
 * AtEOSubXact_RelationCache
 *
 *	Clean up the relcache at sub-transaction commit or abort.
 *
 * Note: this must be called *before* processing invalidation messages.
 */
pub unsafe fn AtEOSubXact_RelationCache(
    isCommit: bool,
    mySubid: SubTransactionId,
    parentSubid: SubTransactionId,
) {
    let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut idhentry: *mut RelIdCacheEnt;
    let mut i: c_int;

    /*
     * Forget in_progress_list.
     */
    debug_assert!(in_progress_list_len == 0 || !isCommit);
    in_progress_list_len = 0;

    if eoxact_list_overflowed {
        hash_seq_init(&mut status, RelationIdCache);
        loop {
            idhentry = hash_seq_search(&mut status) as *mut RelIdCacheEnt;
            if idhentry.is_null() {
                break;
            }
            AtEOSubXact_cleanup((*idhentry).reldesc, isCommit, mySubid, parentSubid);
        }
    } else {
        i = 0;
        while i < eoxact_list_len {
            idhentry = hash_search(
                RelationIdCache,
                &eoxact_list[i as usize] as *const Oid as *const c_void,
                HASH_FIND,
                core::ptr::null_mut(),
            ) as *mut RelIdCacheEnt;
            if !idhentry.is_null() {
                AtEOSubXact_cleanup((*idhentry).reldesc, isCommit, mySubid, parentSubid);
            }
            i += 1;
        }
    }

    /* Don't reset the list; we still need more cleanup later */
}

/*
 * AtEOSubXact_cleanup
 *
 *	Clean up a single rel at subtransaction commit or abort
 */
unsafe fn AtEOSubXact_cleanup(
    relation: Relation,
    isCommit: bool,
    mySubid: SubTransactionId,
    parentSubid: SubTransactionId,
) {
    /*
     * Is it a relation created in the current subtransaction?
     *
     * During subcommit, mark it as belonging to the parent, instead, as long
     * as it has not been dropped.
     */
    if (*relation).rd_createSubid == mySubid {
        debug_assert!(
            (*relation).rd_droppedSubid == mySubid
                || (*relation).rd_droppedSubid == InvalidSubTransactionId
        );
        if isCommit && (*relation).rd_droppedSubid == InvalidSubTransactionId {
            (*relation).rd_createSubid = parentSubid;
        } else if RelationHasReferenceCountZero(relation) {
            /* allow the entry to be removed */
            (*relation).rd_createSubid = InvalidSubTransactionId;
            (*relation).rd_newRelfilelocatorSubid = InvalidSubTransactionId;
            (*relation).rd_firstRelfilelocatorSubid = InvalidSubTransactionId;
            (*relation).rd_droppedSubid = InvalidSubTransactionId;
            RelationClearRelation(relation);
            return;
        } else {
            /*
             * Hmm, somewhere there's a (leaked?) reference to the relation.
             * Transfer it to the parent subtransaction.
             */
            (*relation).rd_createSubid = parentSubid;
            elog(
                WARNING,
                "cannot remove relcache entry because it has nonzero refcount in AtEOSubXact_cleanup",
            );
        }
    }

    /*
     * Likewise, update or drop any new-relfilenumber-in-subtransaction record
     * or drop record.
     */
    if (*relation).rd_newRelfilelocatorSubid == mySubid {
        if isCommit {
            (*relation).rd_newRelfilelocatorSubid = parentSubid;
        } else {
            (*relation).rd_newRelfilelocatorSubid = InvalidSubTransactionId;
        }
    }

    if (*relation).rd_firstRelfilelocatorSubid == mySubid {
        if isCommit {
            (*relation).rd_firstRelfilelocatorSubid = parentSubid;
        } else {
            (*relation).rd_firstRelfilelocatorSubid = InvalidSubTransactionId;
        }
    }

    if (*relation).rd_droppedSubid == mySubid {
        if isCommit {
            (*relation).rd_droppedSubid = parentSubid;
        } else {
            (*relation).rd_droppedSubid = InvalidSubTransactionId;
        }
    }
}

/*
 *		RelationBuildLocalRelation
 *			Build a relcache entry for an about-to-be-created relation,
 *			and enter it into the relcache.
 */
pub unsafe fn RelationBuildLocalRelation(
    relname: *const c_char,
    relnamespace: Oid,
    tupDesc: TupleDesc,
    relid: Oid,
    accessmtd: Oid,
    relfilenumber: RelFileNumber,
    reltablespace: Oid,
    shared_relation: bool,
    mapped_relation: bool,
    relpersistence: c_char,
    relkind: c_char,
) -> Relation {
    let mut rel: Relation;
    let mut oldcxt: *mut c_void;
    let natts: c_int = (*tupDesc).natts;
    let mut i: c_int;
    let mut has_not_null: bool;
    let nailit: bool;

    debug_assert!(natts >= 0);

    /*
     * check for creation of a rel that must be nailed in cache.
     */
    nailit = matches!(
        relid,
        DatabaseRelationId
            | AuthIdRelationId
            | AuthMemRelationId
            | RelationRelationId
            | AttributeRelationId
            | ProcedureRelationId
            | TypeRelationId
    );

    /* check that hardwired list of shared rels matches */
    if shared_relation != IsSharedRelation(relid) {
        elog(
            ERROR,
            "shared_relation flag does not match IsSharedRelation in RelationBuildLocalRelation",
        );
    }

    /* Shared relations had better be mapped, too */
    debug_assert!(mapped_relation || !shared_relation);

    /*
     * switch to the cache context to create the relcache entry.
     */
    if CacheMemoryContext.is_null() {
        CreateCacheMemoryContext();
    }

    oldcxt = MemoryContextSwitchTo(CacheMemoryContext as *mut c_void);

    /*
     * allocate a new relation descriptor and fill in basic state fields.
     */
    rel = palloc0(core::mem::size_of::<RelationData>()) as Relation;

    /* make sure relation is marked as having no open file yet */
    (*rel).rd_smgr = core::ptr::null_mut();

    /* mark it nailed if appropriate */
    (*rel).rd_isnailed = nailit;

    (*rel).rd_refcnt = if nailit { 1 } else { 0 };

    /* it's being created in this transaction */
    (*rel).rd_createSubid = GetCurrentSubTransactionId();
    (*rel).rd_newRelfilelocatorSubid = InvalidSubTransactionId;
    (*rel).rd_firstRelfilelocatorSubid = InvalidSubTransactionId;
    (*rel).rd_droppedSubid = InvalidSubTransactionId;

    /*
     * create a new tuple descriptor from the one passed in.  We do this
     * partly to copy it into the cache context, and partly because the new
     * relation can't have any defaults or constraints yet.
     */
    (*rel).rd_att = CreateTupleDescCopy(tupDesc);
    (*(*rel).rd_att).tdrefcount = 1; /* mark as refcounted */
    has_not_null = false;
    i = 0;
    while i < natts {
        let satt = TupleDescAttr(tupDesc, i) as *mut FormData_pg_attribute_stub;
        let datt = TupleDescAttr((*rel).rd_att, i) as *mut FormData_pg_attribute_stub;
        (*datt).attidentity = (*satt).attidentity;
        (*datt).attgenerated = (*satt).attgenerated;
        (*datt).attnotnull = (*satt).attnotnull;
        has_not_null |= (*satt).attnotnull;
        populate_compact_attribute((*rel).rd_att, i);
        i += 1;
    }

    if has_not_null {
        let constr = palloc0(core::mem::size_of::<TupleConstrStub>()) as *mut TupleConstrStub;
        (*constr).has_not_null = true;
        let att = (*(*rel).rd_att).constr;
        // TODO(pg-port): TupleDesc.constr field access depends on TupleDescData layout
        // (*(*rel).rd_att).constr = constr as *mut TupleConstr;
        let _ = constr;
    }

    /*
     * initialize relation tuple form (caller may add/override data later)
     */
    (*rel).rd_rel = palloc0(CLASS_TUPLE_SIZE) as Form_pg_class;

    namestrcpy(
        core::ptr::addr_of_mut!((*(*rel).rd_rel).relname),
        relname,
    );
    (*(*rel).rd_rel).relnamespace = relnamespace;

    (*(*rel).rd_rel).relkind = relkind as i8;
    (*(*rel).rd_rel).relnatts = natts as i16;
    (*(*rel).rd_rel).reltype = InvalidOid;
    /* needed when bootstrapping: */
    (*(*rel).rd_rel).relowner = BOOTSTRAP_SUPERUSERID;

    /* set up persistence and relcache fields dependent on it */
    (*(*rel).rd_rel).relpersistence = relpersistence as i8;
    match relpersistence as i8 {
        x if x == RELPERSISTENCE_UNLOGGED || x == RELPERSISTENCE_PERMANENT => {
            (*rel).rd_backend = INVALID_PROC_NUMBER;
            (*rel).rd_islocaltemp = false;
        }
        x if x == RELPERSISTENCE_TEMP => {
            (*rel).rd_backend = ProcNumberForTempRelations();
            (*rel).rd_islocaltemp = true;
        }
        _ => {
            elog(ERROR, "invalid relpersistence in RelationBuildLocalRelation");
        }
    }

    /* if it's a materialized view, it's not populated initially */
    if relkind as i8 == RELKIND_MATVIEW {
        (*(*rel).rd_rel).relispopulated = false;
    } else {
        (*(*rel).rd_rel).relispopulated = true;
    }

    /* set replica identity */
    if !IsCatalogNamespace(relnamespace)
        && (relkind as i8 == RELKIND_RELATION
            || relkind as i8 == RELKIND_MATVIEW
            || relkind as i8 == RELKIND_PARTITIONED_TABLE)
    {
        (*(*rel).rd_rel).relreplident = REPLICA_IDENTITY_DEFAULT as i8;
    } else {
        (*(*rel).rd_rel).relreplident = REPLICA_IDENTITY_NOTHING as i8;
    }

    /*
     * Insert relation physical and logical identifiers (OIDs) into the right
     * places.
     */
    (*(*rel).rd_rel).relisshared = shared_relation;

    (*rel).rd_id = relid;

    i = 0;
    while i < natts {
        let attr = TupleDescAttr((*rel).rd_att, i) as *mut FormData_pg_attribute_stub;
        (*attr).attrelid = relid;
        i += 1;
    }

    (*(*rel).rd_rel).reltablespace = reltablespace;

    if mapped_relation {
        (*(*rel).rd_rel).relfilenode = InvalidRelFileNumber;
        /* Add it to the active mapping information */
        RelationMapUpdateMap(relid, relfilenumber, shared_relation, true);
    } else {
        (*(*rel).rd_rel).relfilenode = relfilenumber;
    }

    RelationInitLockInfo(rel);
    RelationInitPhysicalAddr(rel);

    (*(*rel).rd_rel).relam = accessmtd;

    /*
     * RelationInitTableAccessMethod will do syscache lookups, so we mustn't
     * run it in CacheMemoryContext.
     */
    MemoryContextSwitchTo(oldcxt);

    if RELKIND_HAS_TABLE_AM(relkind as i8) || relkind as i8 == RELKIND_SEQUENCE {
        RelationInitTableAccessMethod(rel);
    }

    /*
     * Okay to insert into the relcache hash table.
     */
    RelationCacheInsert(rel, nailit);

    /*
     * Flag relation as needing eoxact cleanup (to clear rd_createSubid).
     */
    EOXactListAdd(rel);

    /* It's fully valid */
    (*rel).rd_isvalid = true;

    /*
     * Caller expects us to pin the returned entry.
     */
    RelationIncrementReferenceCount(rel);

    rel
}

/*
 * RelationSetNewRelfilenumber
 *
 * Assign a new relfilenumber (physical file name), and possibly a new
 * persistence setting, to the relation.
 */
pub unsafe fn RelationSetNewRelfilenumber(relation: Relation, persistence: c_char) {
    let newrelfilenumber: RelFileNumber;
    let mut pg_class: Relation;
    let mut otid: ItemPointerData = core::mem::zeroed();
    let mut tuple: HeapTuple;
    let classform: Form_pg_class;
    let minmulti: MultiXactId = InvalidMultiXactId;
    let freezeXid: TransactionId = InvalidTransactionId;
    let mut newrlocator: RelFileLocator = core::mem::zeroed();

    if !IsBinaryUpgrade {
        /* Allocate a new relfilenumber */
        newrelfilenumber = GetNewRelFileNumber(
            (*(*relation).rd_rel).reltablespace,
            core::ptr::null_mut(),
            persistence as i8,
        );
    } else if (*(*relation).rd_rel).relkind == RELKIND_INDEX as i8 {
        if !OidIsValid(binary_upgrade_next_index_pg_class_relfilenumber) {
            ereport!(
                ERROR,
                errmsg!(
                    "index relfilenumber value not set when in binary upgrade mode"
                )
            );
        }
        newrelfilenumber = binary_upgrade_next_index_pg_class_relfilenumber;
        binary_upgrade_next_index_pg_class_relfilenumber = InvalidOid;
    } else if (*(*relation).rd_rel).relkind == RELKIND_RELATION as i8 {
        if !OidIsValid(binary_upgrade_next_heap_pg_class_relfilenumber) {
            ereport!(
                ERROR,
                errmsg!(
                    "heap relfilenumber value not set when in binary upgrade mode"
                )
            );
        }
        newrelfilenumber = binary_upgrade_next_heap_pg_class_relfilenumber;
        binary_upgrade_next_heap_pg_class_relfilenumber = InvalidOid;
    } else {
        ereport!(
            ERROR,
            errmsg!(
                "unexpected request for new relfilenumber in binary upgrade mode"
            )
        );
        return; /* unreachable */
    }

    /*
     * Get a writable copy of the pg_class tuple for the given relation.
     */
    pg_class = table_open(RelationRelationId, RowExclusiveLock);

    tuple = SearchSysCacheLockedCopy1(RELOID, ObjectIdGetDatum(RelationGetRelid(relation)));
    if !HeapTupleIsValid(tuple) {
        elog(
            ERROR,
            "could not find tuple for relation in RelationSetNewRelfilenumber",
        );
    }
    otid = (*tuple).t_self;
    classform = GETSTRUCT(tuple) as Form_pg_class;

    /*
     * Schedule unlinking of the old storage at transaction commit, except
     * when performing a binary upgrade.
     */
    if IsBinaryUpgrade {
        let srel = smgropen((*relation).rd_locator, (*relation).rd_backend);
        smgrdounlinkall(&srel as *const _ as *mut _, 1, false);
        smgrclose(srel);
    } else {
        /* Not a binary upgrade, so just schedule it to happen later. */
        RelationDropStorage(relation);
    }

    /*
     * Create storage for the main fork of the new relfilenumber.
     */
    newrlocator = (*relation).rd_locator;
    newrlocator.relNumber = newrelfilenumber;

    if RELKIND_HAS_TABLE_AM((*(*relation).rd_rel).relkind as i8) {
        table_relation_set_new_filelocator(
            relation,
            &newrlocator,
            persistence as i8,
            &freezeXid as *const _ as *mut _,
            &minmulti as *const _ as *mut _,
        );
    } else if RELKIND_HAS_STORAGE((*(*relation).rd_rel).relkind as i8) {
        /* handle these directly, at least for now */
        let srel = RelationCreateStorage(newrlocator, persistence as i8, true);
        smgrclose(srel);
    } else {
        /* we shouldn't be called for anything else */
        elog(
            ERROR,
            "relation does not have storage in RelationSetNewRelfilenumber",
        );
    }

    /*
     * If we're dealing with a mapped index, pg_class.relfilenode doesn't
     * change.
     */
    if RelationIsMapped(relation) {
        /* This case is only supported for indexes */
        debug_assert_eq!((*(*relation).rd_rel).relkind, RELKIND_INDEX as i8);

        /* Since we're not updating pg_class, these had better not change */
        debug_assert_eq!((*classform).relfrozenxid, freezeXid);
        debug_assert_eq!((*classform).relminmxid, minmulti);
        debug_assert_eq!((*classform).relpersistence, persistence as i8);

        GetCurrentTransactionId();

        /* Do the deed */
        RelationMapUpdateMap(
            RelationGetRelid(relation),
            newrelfilenumber,
            (*(*relation).rd_rel).relisshared,
            false,
        );

        /* Since we're not updating pg_class, must trigger inval manually */
        CacheInvalidateRelcache(relation);
    } else {
        /* Normal case, update the pg_class entry */
        (*classform).relfilenode = newrelfilenumber;

        /* relpages etc. never change for sequences */
        if (*(*relation).rd_rel).relkind != RELKIND_SEQUENCE as i8 {
            (*classform).relpages = 0; /* it's empty until further notice */
            (*classform).reltuples = -1.0;
            (*classform).relallvisible = 0;
            (*classform).relallfrozen = 0;
        }
        (*classform).relfrozenxid = freezeXid;
        (*classform).relminmxid = minmulti;
        (*classform).relpersistence = persistence as i8;

        CatalogTupleUpdate(pg_class, &otid, tuple);
    }

    UnlockTuple(pg_class, &otid, InplaceUpdateTupleLock);
    heap_freetuple(tuple);

    table_close(pg_class, RowExclusiveLock);

    /*
     * Make the pg_class row change or relation map change visible.
     */
    CommandCounterIncrement();

    RelationAssumeNewRelfilelocator(relation);
}

/*
 * RelationAssumeNewRelfilelocator
 *
 * Code that modifies pg_class.reltablespace or pg_class.relfilenode must call
 * this.
 */
pub unsafe fn RelationAssumeNewRelfilelocator(relation: Relation) {
    (*relation).rd_newRelfilelocatorSubid = GetCurrentSubTransactionId();
    if (*relation).rd_firstRelfilelocatorSubid == InvalidSubTransactionId {
        (*relation).rd_firstRelfilelocatorSubid = (*relation).rd_newRelfilelocatorSubid;
    }

    /* Flag relation as needing eoxact cleanup (to clear these fields) */
    EOXactListAdd(relation);
}

// ===========================================================================
// Part 7: RelationCacheInitialize, RelationCacheInitializePhase2/3,
//         load_critical_index, BuildHardcodedDescriptor,
//         GetPgClassDescriptor, GetPgIndexDescriptor,
//         AttrDefaultFetch, CheckNNConstraintFetch,
//         RelationGetFKeyList, RelationGetIndexList, RelationGetStatExtList,
//         RelationGetPrimaryKeyIndex, RelationGetReplicaIndex
// ===========================================================================

/* INITRELCACHESIZE defined at top of module. */

/*
 *		RelationCacheInitialize
 *
 *		This initializes the relation descriptor cache.
 */
pub unsafe fn RelationCacheInitialize() {
    let mut ctl: HASHCTL = core::mem::zeroed();
    let mut allocsize: c_int;

    /* make sure cache memory context exists */
    if CacheMemoryContext.is_null() {
        CreateCacheMemoryContext();
    }

    /* create hashtable that indexes the relcache */
    ctl.keysize = core::mem::size_of::<Oid>();
    ctl.entrysize = core::mem::size_of::<RelIdCacheEnt>();
    RelationIdCache = hash_create(
        b"Relcache by OID\0".as_ptr() as *const c_char,
        INITRELCACHESIZE as c_long,
        &ctl,
        HASH_ELEM | HASH_BLOBS,
    );

    /* reserve enough in_progress_list slots for many cases */
    allocsize = 4;
    in_progress_list = MemoryContextAlloc(
        CacheMemoryContext as *mut c_void,
        allocsize as usize * core::mem::size_of::<InProgressEnt>(),
    ) as *mut InProgressEnt;
    in_progress_list_maxlen = allocsize;

    /* relation mapper needs to be initialized too */
    RelationMapInitialize();
}

/*
 *		RelationCacheInitializePhase2
 *
 *		This is called to prepare for access to shared catalogs during startup.
 */
pub unsafe fn RelationCacheInitializePhase2() {
    let mut oldcxt: *mut c_void;

    /* relation mapper needs initialized too */
    RelationMapInitializePhase2();

    /* In bootstrap mode, the shared catalogs aren't there yet anyway */
    if IsBootstrapProcessingMode() {
        return;
    }

    /* switch to cache memory context */
    oldcxt = MemoryContextSwitchTo(CacheMemoryContext as *mut c_void);

    /*
     * Try to load the shared relcache cache file.  If unsuccessful, bootstrap
     * the cache with pre-made descriptors for the critical shared catalogs.
     */
    if !load_relcache_init_file(true) {
        formrdesc(
            b"pg_database\0".as_ptr() as *const c_char,
            DatabaseRelation_Rowtype_Id,
            true,
            Natts_pg_database,
            Desc_pg_database,
        );
        formrdesc(
            b"pg_authid\0".as_ptr() as *const c_char,
            AuthIdRelation_Rowtype_Id,
            true,
            Natts_pg_authid,
            Desc_pg_authid,
        );
        formrdesc(
            b"pg_auth_members\0".as_ptr() as *const c_char,
            AuthMemRelation_Rowtype_Id,
            true,
            Natts_pg_auth_members,
            Desc_pg_auth_members,
        );
        formrdesc(
            b"pg_shseclabel\0".as_ptr() as *const c_char,
            SharedSecLabelRelation_Rowtype_Id,
            true,
            Natts_pg_shseclabel,
            Desc_pg_shseclabel,
        );
        formrdesc(
            b"pg_subscription\0".as_ptr() as *const c_char,
            SubscriptionRelation_Rowtype_Id,
            true,
            Natts_pg_subscription,
            Desc_pg_subscription,
        );
        /* NUM_CRITICAL_SHARED_RELS = 5 */
    }

    MemoryContextSwitchTo(oldcxt);
}

/*
 *		RelationCacheInitializePhase3
 *
 *		This is called as soon as the catcache and transaction system
 *		are functional and we have determined MyDatabaseId.
 */
pub unsafe fn RelationCacheInitializePhase3() {
    let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut idhentry: *mut RelIdCacheEnt;
    let mut oldcxt: *mut c_void;
    let needNewCacheFile: bool = !criticalSharedRelcachesBuilt;

    /* relation mapper needs initialized too */
    RelationMapInitializePhase3();

    /* switch to cache memory context */
    oldcxt = MemoryContextSwitchTo(CacheMemoryContext as *mut c_void);

    /*
     * Try to load the local relcache cache file.
     */
    let mut needNewCacheFile = needNewCacheFile;
    if IsBootstrapProcessingMode() || !load_relcache_init_file(false) {
        needNewCacheFile = true;

        formrdesc(
            b"pg_class\0".as_ptr() as *const c_char,
            RelationRelation_Rowtype_Id,
            false,
            Natts_pg_class,
            Desc_pg_class,
        );
        formrdesc(
            b"pg_attribute\0".as_ptr() as *const c_char,
            AttributeRelation_Rowtype_Id,
            false,
            Natts_pg_attribute,
            Desc_pg_attribute,
        );
        formrdesc(
            b"pg_proc\0".as_ptr() as *const c_char,
            ProcedureRelation_Rowtype_Id,
            false,
            Natts_pg_proc,
            Desc_pg_proc,
        );
        formrdesc(
            b"pg_type\0".as_ptr() as *const c_char,
            TypeRelation_Rowtype_Id,
            false,
            Natts_pg_type,
            Desc_pg_type,
        );
        /* NUM_CRITICAL_LOCAL_RELS = 4 */
    }

    MemoryContextSwitchTo(oldcxt);

    /* In bootstrap mode, the faked-up formrdesc info is all we'll have */
    if IsBootstrapProcessingMode() {
        return;
    }

    /*
     * If we didn't get the critical system indexes loaded into relcache,
     * do so now.
     */
    if !criticalRelcachesBuilt {
        load_critical_index(ClassOidIndexId, RelationRelationId);
        load_critical_index(AttributeRelidNumIndexId, AttributeRelationId);
        load_critical_index(IndexRelidIndexId, IndexRelationId);
        load_critical_index(OpclassOidIndexId, OperatorClassRelationId);
        load_critical_index(AccessMethodProcedureIndexId, AccessMethodProcedureRelationId);
        load_critical_index(RewriteRelRulenameIndexId, RewriteRelationId);
        load_critical_index(TriggerRelidNameIndexId, TriggerRelationId);
        /* NUM_CRITICAL_LOCAL_INDEXES = 7 */
        criticalRelcachesBuilt = true;
    }

    /*
     * Process critical shared indexes too.
     */
    if !criticalSharedRelcachesBuilt {
        load_critical_index(DatabaseNameIndexId, DatabaseRelationId);
        load_critical_index(DatabaseOidIndexId, DatabaseRelationId);
        load_critical_index(AuthIdRolnameIndexId, AuthIdRelationId);
        load_critical_index(AuthIdOidIndexId, AuthIdRelationId);
        load_critical_index(AuthMemMemRoleIndexId, AuthMemRelationId);
        load_critical_index(SharedSecLabelObjectIndexId, SharedSecLabelRelationId);
        /* NUM_CRITICAL_SHARED_INDEXES = 6 */
        criticalSharedRelcachesBuilt = true;
    }

    /*
     * Now, scan all the relcache entries and update anything that might be
     * wrong in the results from formrdesc or the relcache cache file.
     */
    hash_seq_init(&mut status, RelationIdCache);

    loop {
        idhentry = hash_seq_search(&mut status) as *mut RelIdCacheEnt;
        if idhentry.is_null() {
            break;
        }
        let relation = (*idhentry).reldesc;
        let mut restart: bool = false;

        RelationIncrementReferenceCount(relation);

        /* If it's a faked-up entry, read the real pg_class tuple. */
        if (*(*relation).rd_rel).relowner == InvalidOid {
            let htup: HeapTuple;
            let relp: Form_pg_class;

            htup = SearchSysCache1(RELOID, ObjectIdGetDatum(RelationGetRelid(relation)));
            if !HeapTupleIsValid(htup) {
                ereport!(
                    FATAL,
                    errmsg!("cache lookup failed for relation in RelationCacheInitializePhase3")
                );
            }
            relp = GETSTRUCT(htup) as Form_pg_class;

            core::ptr::copy_nonoverlapping(
                relp as *const u8,
                (*relation).rd_rel as *mut u8,
                CLASS_TUPLE_SIZE,
            );

            /* Update rd_options while we have the tuple */
            if !(*relation).rd_options.is_null() {
                pfree((*relation).rd_options as *mut c_void);
            }
            RelationParseRelOptions(relation, htup);

            debug_assert_eq!((*(*relation).rd_att).tdtypeid, (*relp).reltype);
            debug_assert_eq!((*(*relation).rd_att).tdtypmod, -1);

            ReleaseSysCache(htup);

            /* relowner had better be OK now, else we'll loop forever */
            if (*(*relation).rd_rel).relowner == InvalidOid {
                elog(
                    ERROR,
                    "invalid relowner in pg_class entry in RelationCacheInitializePhase3",
                );
            }

            restart = true;
        }

        /* Fix data that isn't saved in relcache cache file. */
        if (*(*relation).rd_rel).relhasrules && (*relation).rd_rules.is_null() {
            RelationBuildRuleLock(relation);
            if (*relation).rd_rules.is_null() {
                (*(*relation).rd_rel).relhasrules = false;
            }
            restart = true;
        }
        if (*(*relation).rd_rel).relhastriggers && (*relation).trigdesc.is_null() {
            RelationBuildTriggers(relation);
            if (*relation).trigdesc.is_null() {
                (*(*relation).rd_rel).relhastriggers = false;
            }
            restart = true;
        }

        /*
         * Re-load the row security policies if the relation has them.
         */
        if (*(*relation).rd_rel).relrowsecurity && (*relation).rd_rsdesc.is_null() {
            RelationBuildRowSecurity(relation);
            debug_assert!(!(*relation).rd_rsdesc.is_null());
            restart = true;
        }

        /* Reload tableam data if needed */
        if (*relation).rd_tableam.is_null()
            && (RELKIND_HAS_TABLE_AM((*(*relation).rd_rel).relkind as i8)
                || (*(*relation).rd_rel).relkind == RELKIND_SEQUENCE as i8)
        {
            RelationInitTableAccessMethod(relation);
            debug_assert!(!(*relation).rd_tableam.is_null());
            restart = true;
        }

        /* Release hold on the relation */
        RelationDecrementReferenceCount(relation);

        /* Now, restart the hashtable scan if needed */
        if restart {
            hash_seq_term(&mut status);
            hash_seq_init(&mut status, RelationIdCache);
        }
    }

    /*
     * Lastly, write out new relcache cache files if needed.
     */
    if needNewCacheFile {
        InitCatalogCachePhase2();
        write_relcache_init_file(true);
        write_relcache_init_file(false);
    }
}

/*
 * Load one critical system index into the relcache
 */
unsafe fn load_critical_index(indexoid: Oid, heapoid: Oid) {
    let ird: Relation;

    /*
     * We must lock the underlying catalog before locking the index to avoid
     * deadlock.
     */
    LockRelationOid(heapoid, AccessShareLock);
    LockRelationOid(indexoid, AccessShareLock);
    ird = RelationBuildDesc(indexoid, true);
    if ird.is_null() {
        ereport!(
            PANIC,
            errmsg!("could not open critical system index in load_critical_index")
        );
    }
    (*ird).rd_isnailed = true;
    (*ird).rd_refcnt = 1;
    UnlockRelationOid(indexoid, AccessShareLock);
    UnlockRelationOid(heapoid, AccessShareLock);

    RelationGetIndexAttOptions(ird, false);
}

/*
 * BuildHardcodedDescriptor -- get a hardcoded tuple descriptor
 */
unsafe fn BuildHardcodedDescriptor(
    natts: c_int,
    attrs: *const c_void,
) -> TupleDesc {
    let result: TupleDesc;
    let mut oldcxt: *mut c_void;

    oldcxt = MemoryContextSwitchTo(CacheMemoryContext as *mut c_void);

    result = CreateTemplateTupleDesc(natts);
    (*result).tdtypeid = RECORDOID; /* not right, but we don't care */
    (*result).tdtypmod = -1;

    let mut i: c_int = 0;
    while i < natts {
        let dst = TupleDescAttr(result, i);
        let src = (attrs as *const u8).add(i as usize * ATTRIBUTE_FIXED_PART_SIZE);
        core::ptr::copy_nonoverlapping(src, dst as *mut u8, ATTRIBUTE_FIXED_PART_SIZE);
        populate_compact_attribute(result, i);
        i += 1;
    }

    /* initialize first attribute's attcacheoff, cf RelationBuildTupleDesc */
    // TupleDescCompactAttr(result, 0)->attcacheoff = 0;
    // TODO(pg-port): compact_attrs field access depends on TupleDescData layout

    MemoryContextSwitchTo(oldcxt);

    result
}

/*
 * GetPgClassDescriptor -- get a predefined tuple descriptor for pg_class
 */
unsafe fn GetPgClassDescriptor() -> TupleDesc {
    static mut pgclassdesc: TupleDesc = core::ptr::null_mut();

    if pgclassdesc.is_null() {
        pgclassdesc = BuildHardcodedDescriptor(Natts_pg_class, Desc_pg_class as *const c_void);
    }

    pgclassdesc
}

/*
 * GetPgIndexDescriptor -- get a predefined tuple descriptor for pg_index
 */
unsafe fn GetPgIndexDescriptor() -> TupleDesc {
    static mut pgindexdesc: TupleDesc = core::ptr::null_mut();

    if pgindexdesc.is_null() {
        pgindexdesc = BuildHardcodedDescriptor(Natts_pg_index, Desc_pg_index as *const c_void);
    }

    pgindexdesc
}

/*
 * AttrDefaultFetch -- load default attribute value definitions for the relation
 */
unsafe fn AttrDefaultFetch(relation: Relation, ndef: c_int) {
    let attrdef: *mut AttrDefault;
    let adrel: Relation;
    let mut adscan: SysScanDesc;
    let mut skey: ScanKeyData = core::mem::zeroed();
    let mut htup: HeapTuple;
    let mut found: c_int = 0;

    /* Allocate array with room for as many entries as expected */
    attrdef = MemoryContextAllocZero(
        CacheMemoryContext as *mut c_void,
        ndef as usize * core::mem::size_of::<AttrDefault>(),
    ) as *mut AttrDefault;

    /* Search pg_attrdef for relevant entries */
    ScanKeyInit(
        &mut skey,
        Anum_pg_attrdef_adrelid as c_int,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(relation)),
    );

    adrel = table_open(AttrDefaultRelationId, AccessShareLock);
    adscan = systable_beginscan(adrel, AttrDefaultIndexId, true, core::ptr::null_mut(), 1, &mut skey as *mut ScanKeyData);

    loop {
        htup = systable_getnext(adscan) as HeapTuple;
        if !HeapTupleIsValid(htup) {
            break;
        }
        let adform = GETSTRUCT(htup) as *mut FormData_pg_attrdef_stub;
        let mut val: Datum = 0;
        let mut isnull: bool = false;

        /* protect limited size of array */
        if found >= ndef {
            elog(
                WARNING,
                "unexpected pg_attrdef record found in AttrDefaultFetch",
            );
            break;
        }

        val = fastgetattr(
            htup,
            Anum_pg_attrdef_adbin as c_int,
            (*adrel).rd_att,
            &mut isnull,
        );
        if isnull {
            elog(
                WARNING,
                "null adbin for attribute in AttrDefaultFetch",
            );
        } else {
            /* detoast and convert to cstring in caller's context */
            let s = TextDatumGetCString(val);

            (*attrdef.add(found as usize)).adnum = (*adform).adnum;
            (*attrdef.add(found as usize)).adbin =
                MemoryContextStrdup(CacheMemoryContext as *mut c_void, s);
            pfree(s as *mut c_void);
            found += 1;
        }
    }

    systable_endscan(adscan);
    table_close(adrel, AccessShareLock);

    if found != ndef {
        elog(
            WARNING,
            "pg_attrdef record(s) missing for relation in AttrDefaultFetch",
        );
    }

    /* Sort the AttrDefault entries by adnum */
    if found > 1 {
        qsort(
            attrdef as *mut c_void,
            found as usize,
            core::mem::size_of::<AttrDefault>(),
            AttrDefaultCmp,
        );
    }

    /* Install array only after it's fully valid */
    // (*(*relation).rd_att).constr.defval = attrdef;
    // (*(*relation).rd_att).constr.num_defval = found;
    // TODO(pg-port): TupleConstr.defval field access not ported.
}

unsafe extern "C" fn AttrDefaultCmp(a: *const c_void, b: *const c_void) -> c_int {
    let ada = &*(a as *const AttrDefault);
    let adb = &*(b as *const AttrDefault);
    (ada.adnum as i32) - (adb.adnum as i32)
}

/*
 * CheckNNConstraintFetch -- load check constraints for the relation
 */
unsafe fn CheckNNConstraintFetch(relation: Relation) {
    let mut check: *mut ConstrCheck;
    let ncheck: c_int = (*(*relation).rd_rel).relchecks as c_int;
    let conrel: Relation;
    let mut conscan: SysScanDesc;
    let mut skey: [ScanKeyData; 1] = [core::mem::zeroed()];
    let mut htup: HeapTuple;
    let mut found: c_int = 0;

    /* Allocate array with room for as many entries as expected, if needed */
    if ncheck > 0 {
        check = MemoryContextAllocZero(
            CacheMemoryContext as *mut c_void,
            ncheck as usize * core::mem::size_of::<ConstrCheck>(),
        ) as *mut ConstrCheck;
    } else {
        check = core::ptr::null_mut();
    }

    /* Search pg_constraint for relevant entries */
    ScanKeyInit(
        &mut skey[0],
        Anum_pg_constraint_conrelid as c_int,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(relation)),
    );

    conrel = table_open(ConstraintRelationId, AccessShareLock);
    conscan = systable_beginscan(
        conrel,
        ConstraintRelidTypidNameIndexId,
        true,
        core::ptr::null_mut(),
        1,
        skey.as_mut_ptr(),
    );

    loop {
        htup = systable_getnext(conscan) as HeapTuple;
        if !HeapTupleIsValid(htup) {
            break;
        }
        let conform = GETSTRUCT(htup) as *mut FormData_pg_constraint_stub;
        let mut val: Datum = 0;
        let mut isnull: bool = false;

        /*
         * If this is a not-null constraint, then only look at it if it's
         * invalid.
         */
        if (*conform).contype == CONSTRAINT_NOTNULL as i8 {
            if !(*conform).convalidated {
                let attnum = extractNotNullColumn(htup);
                // relation->rd_att->compact_attrs[attnum - 1].attnullability = ATTNULLABLE_INVALID;
                // TODO(pg-port): compact_attrs field access not ported.
            }
            continue;
        }

        /* For what follows, consider check constraints only */
        if (*conform).contype != CONSTRAINT_CHECK as i8 {
            continue;
        }

        /* protect limited size of array */
        if found >= ncheck {
            elog(
                WARNING,
                "unexpected pg_constraint record found in CheckNNConstraintFetch",
            );
            break;
        }

        (*check.add(found as usize)).ccenforced = (*conform).conenforced;
        (*check.add(found as usize)).ccvalid = (*conform).convalidated;
        (*check.add(found as usize)).ccnoinherit = (*conform).connoinherit;
        (*check.add(found as usize)).ccname = MemoryContextStrdup(
            CacheMemoryContext as *mut c_void,
            (*conform).conname.as_ptr() as *const c_char,
        );

        /* Grab and test conbin is actually set */
        val = fastgetattr(
            htup,
            Anum_pg_constraint_conbin as c_int,
            (*conrel).rd_att,
            &mut isnull,
        );
        if isnull {
            elog(
                WARNING,
                "null conbin for relation in CheckNNConstraintFetch",
            );
        } else {
            let s = TextDatumGetCString(val);
            (*check.add(found as usize)).ccbin =
                MemoryContextStrdup(CacheMemoryContext as *mut c_void, s);
            pfree(s as *mut c_void);
            found += 1;
        }
    }

    systable_endscan(conscan);
    table_close(conrel, AccessShareLock);

    if found != ncheck {
        elog(
            WARNING,
            "pg_constraint record(s) missing for relation in CheckNNConstraintFetch",
        );
    }

    /* Sort the records by name */
    if found > 1 {
        qsort(
            check as *mut c_void,
            found as usize,
            core::mem::size_of::<ConstrCheck>(),
            CheckConstraintCmp,
        );
    }

    /* Install array only after it's fully valid */
    // (*(*relation).rd_att).constr.check = check;
    // (*(*relation).rd_att).constr.num_check = found;
    // TODO(pg-port): TupleConstr.check field access not ported.
}

unsafe extern "C" fn CheckConstraintCmp(a: *const c_void, b: *const c_void) -> c_int {
    let ca = &*(a as *const ConstrCheck);
    let cb = &*(b as *const ConstrCheck);
    libc_strcmp(ca.ccname, cb.ccname)
}

/*
 * RelationGetFKeyList -- get a list of foreign key info for the relation
 */
pub unsafe fn RelationGetFKeyList(relation: Relation) -> *mut List {
    let mut result: *mut List;
    let conrel: Relation;
    let mut conscan: SysScanDesc;
    let mut skey: ScanKeyData = core::mem::zeroed();
    let mut htup: HeapTuple;
    let oldlist: *mut List;
    let mut oldcxt: *mut c_void;

    /* Quick exit if we already computed the list. */
    if (*relation).rd_fkeyvalid {
        return (*relation).rd_fkeylist;
    }

    result = NIL;

    /* Prepare to scan pg_constraint for entries having conrelid = this rel. */
    ScanKeyInit(
        &mut skey,
        Anum_pg_constraint_conrelid as c_int,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(relation)),
    );

    conrel = table_open(ConstraintRelationId, AccessShareLock);
    conscan = systable_beginscan(
        conrel,
        ConstraintRelidTypidNameIndexId,
        true,
        core::ptr::null_mut(),
        1,
        &mut skey as *mut ScanKeyData,
    );

    loop {
        htup = systable_getnext(conscan) as HeapTuple;
        if !HeapTupleIsValid(htup) {
            break;
        }
        let constraint = GETSTRUCT(htup) as *mut FormData_pg_constraint_stub;

        /* consider only foreign keys */
        if (*constraint).contype != CONSTRAINT_FOREIGN as i8 {
            continue;
        }

        let info = makeNode_ForeignKeyCacheInfo();
        // Fill in info fields -- TODO(pg-port): ForeignKeyCacheInfo not ported.
        result = lappend(result, info as *mut c_void);
    }

    systable_endscan(conscan);
    table_close(conrel, AccessShareLock);

    /* Now save a copy of the completed list in the relcache entry. */
    oldcxt = MemoryContextSwitchTo(CacheMemoryContext as *mut c_void);
    oldlist = (*relation).rd_fkeylist;
    (*relation).rd_fkeylist = copyObject(result as *mut c_void) as *mut List;
    (*relation).rd_fkeyvalid = true;
    MemoryContextSwitchTo(oldcxt);

    /* Don't leak the old list, if there is one */
    list_free_deep(oldlist);

    result
}

/*
 * RelationGetIndexList -- get a list of OIDs of indexes on this relation
 */
pub unsafe fn RelationGetIndexList(relation: Relation) -> *mut List {
    let indrel: Relation;
    let mut indscan: SysScanDesc;
    let mut skey: ScanKeyData = core::mem::zeroed();
    let mut htup: HeapTuple;
    let mut result: *mut List;
    let oldlist: *mut List;
    let replident: i8 = (*(*relation).rd_rel).relreplident;
    let mut pkeyIndex: Oid = InvalidOid;
    let mut candidateIndex: Oid = InvalidOid;
    let mut pkdeferrable: bool = false;
    let mut oldcxt: *mut c_void;

    /* Quick exit if we already computed the list. */
    if (*relation).rd_indexvalid {
        return list_copy((*relation).rd_indexlist);
    }

    result = NIL;

    /* Prepare to scan pg_index for entries having indrelid = this rel. */
    ScanKeyInit(
        &mut skey,
        Anum_pg_index_indrelid as c_int,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(relation)),
    );

    indrel = table_open(IndexRelationId, AccessShareLock);
    indscan = systable_beginscan(
        indrel,
        IndexIndrelidIndexId,
        true,
        core::ptr::null_mut(),
        1,
        &mut skey as *mut ScanKeyData,
    );

    loop {
        htup = systable_getnext(indscan) as HeapTuple;
        if !HeapTupleIsValid(htup) {
            break;
        }
        let index = GETSTRUCT(htup) as *mut FormData_pg_index_stub;

        /* Ignore any indexes that are currently being dropped. */
        if !(*index).indislive {
            continue;
        }

        /* add index's OID to result list */
        result = lappend_oid(result, (*index).indexrelid);

        /*
         * Non-unique or predicate indexes aren't interesting for either oid
         * indexes or replication identity indexes.
         */
        if !(*index).indisunique
            || !heap_attisnull(htup, Anum_pg_index_indpred as c_int, core::ptr::null_mut())
        {
            continue;
        }

        /*
         * Remember primary key index, if any.
         */
        if (*index).indisprimary
            && ((*index).indisvalid
                || (*(*relation).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8)
        {
            pkeyIndex = (*index).indexrelid;
            pkdeferrable = !(*index).indimmediate;
        }

        if !(*index).indimmediate {
            continue;
        }
        if !(*index).indisvalid {
            continue;
        }

        /* remember explicitly chosen replica index */
        if (*index).indisreplident {
            candidateIndex = (*index).indexrelid;
        }
    }

    systable_endscan(indscan);
    table_close(indrel, AccessShareLock);

    /* Sort the result list into OID order, per API spec. */
    list_sort(result, list_oid_cmp);

    /* Now save a copy of the completed list in the relcache entry. */
    oldcxt = MemoryContextSwitchTo(CacheMemoryContext as *mut c_void);
    oldlist = (*relation).rd_indexlist;
    (*relation).rd_indexlist = list_copy(result);
    (*relation).rd_pkindex = pkeyIndex;
    (*relation).rd_ispkdeferrable = pkdeferrable;
    if replident == REPLICA_IDENTITY_DEFAULT as i8 && OidIsValid(pkeyIndex) && !pkdeferrable {
        (*relation).rd_replidindex = pkeyIndex;
    } else if replident == REPLICA_IDENTITY_INDEX as i8 && OidIsValid(candidateIndex) {
        (*relation).rd_replidindex = candidateIndex;
    } else {
        (*relation).rd_replidindex = InvalidOid;
    }
    (*relation).rd_indexvalid = true;
    MemoryContextSwitchTo(oldcxt);

    /* Don't leak the old list, if there is one */
    list_free(oldlist);

    result
}

/*
 * RelationGetStatExtList -- get a list of OIDs of statistics objects on this relation
 */
pub unsafe fn RelationGetStatExtList(relation: Relation) -> *mut List {
    let indrel: Relation;
    let mut indscan: SysScanDesc;
    let mut skey: ScanKeyData = core::mem::zeroed();
    let mut htup: HeapTuple;
    let mut result: *mut List;
    let oldlist: *mut List;
    let mut oldcxt: *mut c_void;

    /* Quick exit if we already computed the list. */
    if (*relation).rd_statvalid {
        return list_copy((*relation).rd_statlist);
    }

    result = NIL;

    /* Prepare to scan pg_statistic_ext for entries having stxrelid = this rel. */
    ScanKeyInit(
        &mut skey,
        Anum_pg_statistic_ext_stxrelid as c_int,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(relation)),
    );

    indrel = table_open(StatisticExtRelationId, AccessShareLock);
    indscan = systable_beginscan(
        indrel,
        StatisticExtRelidIndexId,
        true,
        core::ptr::null_mut(),
        1,
        &mut skey as *mut ScanKeyData,
    );

    loop {
        htup = systable_getnext(indscan) as HeapTuple;
        if !HeapTupleIsValid(htup) {
            break;
        }
        let oid = (*(GETSTRUCT(htup) as *mut FormData_pg_statistic_ext_stub)).oid;
        result = lappend_oid(result, oid);
    }

    systable_endscan(indscan);
    table_close(indrel, AccessShareLock);

    /* Sort the result list into OID order, per API spec. */
    list_sort(result, list_oid_cmp);

    /* Now save a copy of the completed list in the relcache entry. */
    oldcxt = MemoryContextSwitchTo(CacheMemoryContext as *mut c_void);
    oldlist = (*relation).rd_statlist;
    (*relation).rd_statlist = list_copy(result);
    (*relation).rd_statvalid = true;
    MemoryContextSwitchTo(oldcxt);

    /* Don't leak the old list, if there is one */
    list_free(oldlist);

    result
}

/*
 * RelationGetPrimaryKeyIndex -- get OID of the relation's primary key index
 */
pub unsafe fn RelationGetPrimaryKeyIndex(relation: Relation, deferrable_ok: bool) -> Oid {
    let ilist: *mut List;

    if !(*relation).rd_indexvalid {
        /* RelationGetIndexList does the heavy lifting. */
        ilist = RelationGetIndexList(relation);
        list_free(ilist);
        debug_assert!((*relation).rd_indexvalid);
    }

    if deferrable_ok {
        return (*relation).rd_pkindex;
    } else if (*relation).rd_ispkdeferrable {
        return InvalidOid;
    }
    (*relation).rd_pkindex
}

/*
 * RelationGetReplicaIndex -- get OID of the relation's replica identity index
 */
pub unsafe fn RelationGetReplicaIndex(relation: Relation) -> Oid {
    let ilist: *mut List;

    if !(*relation).rd_indexvalid {
        /* RelationGetIndexList does the heavy lifting. */
        ilist = RelationGetIndexList(relation);
        list_free(ilist);
        debug_assert!((*relation).rd_indexvalid);
    }

    (*relation).rd_replidindex
}

// ===========================================================================
// Part 8 (final): RelationGetIndexExpressions, RelationGetDummyIndexExpressions,
//                 RelationGetIndexPredicate, RelationGetIndexAttrBitmap,
//                 RelationGetIdentityKeyBitmap, RelationGetExclusionInfo,
//                 RelationBuildPublicationDesc, CopyIndexAttOptions,
//                 RelationGetIndexAttOptions, errtable, errtablecol,
//                 errtablecolname, errtableconstraint,
//                 load_relcache_init_file, write_relcache_init_file, write_item,
//                 RelationIdIsInInitFile, RelationCacheInitFilePreInvalidate,
//                 RelationCacheInitFilePostInvalidate, RelationCacheInitFileRemove,
//                 RelationCacheInitFileRemoveInDir, unlink_initfile,
//                 ResOwnerPrintRelCache, ResOwnerReleaseRelation,
//                 additional stubs for structs used in part 7
// ===========================================================================

/* ---- local stubs for structs accessed in parts 6/7 ----- */

/// Stub for FormData_pg_index field access (pg_index.h not fully ported).
#[repr(C)]
struct FormData_pg_index_stub {
    pub indexrelid: Oid,
    pub indrelid: Oid,
    pub indnatts: i16,
    pub indnkeyatts: i16,
    pub indisunique: bool,
    pub indnullsnotdistinct: bool,
    pub indisprimary: bool,
    pub indisexclusion: bool,
    pub indimmediate: bool,
    pub indisclustered: bool,
    pub indisvalid: bool,
    pub indcheckxmin: bool,
    pub indisready: bool,
    pub indislive: bool,
    pub indisreplident: bool,
    /* indkey and more follow, not needed for these stubs */
}

/// Stub for FormData_pg_attrdef field access.
#[repr(C)]
struct FormData_pg_attrdef_stub {
    pub oid: Oid,
    pub adrelid: Oid,
    pub adnum: i16,
}

/// Stub for FormData_pg_constraint field access.
#[repr(C)]
struct FormData_pg_constraint_stub {
    pub oid: Oid,
    pub conname: [c_char; 64],
    pub connamespace: Oid,
    pub contype: i8,
    pub condeferrable: bool,
    pub condeferred: bool,
    pub conenforced: bool,
    pub convalidated: bool,
    pub conrelid: Oid,
    pub contypid: Oid,
    pub conindid: Oid,
    pub conparentid: Oid,
    pub confrelid: Oid,
    pub confupdtype: i8,
    pub confdeltype: i8,
    pub confmatchtype: i8,
    pub conislocal: bool,
    pub coninhcount: i16,
    pub connoinherit: bool,
    pub conperiod: bool,
}

/// Stub for FormData_pg_statistic_ext field access.
#[repr(C)]
struct FormData_pg_statistic_ext_stub {
    pub oid: Oid,
    pub stxrelid: Oid,
    /* remaining fields omitted */
}

/* AttrDefault -- used by AttrDefaultFetch */
#[repr(C)]
pub struct AttrDefault {
    pub adnum: i16,
    pub adbin: *mut c_char,
}

/* ConstrCheck -- used by CheckNNConstraintFetch */
#[repr(C)]
pub struct ConstrCheck {
    pub ccname: *mut c_char,
    pub ccbin: *mut c_char,
    pub ccvalid: bool,
    pub ccnoinherit: bool,
    pub ccenforced: bool,
}

/* --- stub helpers for missing catalog accessors --- */

// TODO(pg-port): libc strcmp wrapper
unsafe fn libc_strcmp(a: *const c_char, b: *const c_char) -> c_int {
    // Simple byte-by-byte compare stub; real one would use libc.
    unimplemented!("TODO(pg-port): libc_strcmp not ported")
}

// TODO(pg-port): makeNode(ForeignKeyCacheInfo) stub
unsafe fn makeNode_ForeignKeyCacheInfo() -> *mut c_void {
    unimplemented!("TODO(pg-port): makeNode ForeignKeyCacheInfo not ported")
}

/*
 * RelationGetIndexExpressions -- get the index expressions for an index
 */
pub unsafe fn RelationGetIndexExpressions(relation: Relation) -> *mut List {
    let mut result: *mut List;
    let mut exprsDatum: Datum = 0;
    let mut isnull: bool = false;
    let exprsString: *mut c_char;
    let mut oldcxt: *mut c_void;

    /* Quick exit if we already computed the result. */
    if !(*relation).rd_indexprs.is_null() {
        return copyObject((*relation).rd_indexprs as *mut c_void) as *mut List;
    }

    /* Quick exit if there is nothing to do. */
    if (*relation).rd_indextuple.is_null()
        || heap_attisnull(
        (*relation).rd_indextuple as HeapTuple,
            Anum_pg_index_indexprs as c_int,
            core::ptr::null_mut(),
        )
    {
        return NIL;
    }

    exprsDatum = heap_getattr(
        (*relation).rd_indextuple as HeapTuple,
        Anum_pg_index_indexprs as c_int,
        GetPgIndexDescriptor(),
        &mut isnull,
    );
    debug_assert!(!isnull);
    exprsString = TextDatumGetCString(exprsDatum);
    result = stringToNode(exprsString) as *mut List;
    pfree(exprsString as *mut c_void);

    /*
     * Run the expressions through eval_const_expressions.
     */
    result = eval_const_expressions(core::ptr::null_mut(), result as *mut c_void) as *mut List;

    /* May as well fix opfuncids too */
    fix_opfuncids(result as *mut c_void);

    /* Now save a copy of the completed tree in the relcache entry. */
    oldcxt = MemoryContextSwitchTo((*relation).rd_indexcxt);
    (*relation).rd_indexprs = copyObject(result as *mut c_void) as *mut List;
    MemoryContextSwitchTo(oldcxt);

    result
}

/*
 * RelationGetDummyIndexExpressions -- get dummy expressions for an index
 */
pub unsafe fn RelationGetDummyIndexExpressions(relation: Relation) -> *mut List {
    let mut result: *mut List;
    let mut exprsDatum: Datum = 0;
    let mut isnull: bool = false;
    let exprsString: *mut c_char;
    let rawExprs: *mut List;

    /* Quick exit if there is nothing to do. */
    if (*relation).rd_indextuple.is_null()
        || heap_attisnull(
        (*relation).rd_indextuple as HeapTuple,
            Anum_pg_index_indexprs as c_int,
            core::ptr::null_mut(),
        )
    {
        return NIL;
    }

    /* Extract raw node tree(s) from index tuple. */
    exprsDatum = heap_getattr(
        (*relation).rd_indextuple as HeapTuple,
        Anum_pg_index_indexprs as c_int,
        GetPgIndexDescriptor(),
        &mut isnull,
    );
    debug_assert!(!isnull);
    exprsString = TextDatumGetCString(exprsDatum);
    rawExprs = stringToNode(exprsString) as *mut List;
    pfree(exprsString as *mut c_void);

    /* Construct null Consts */
    result = NIL;
    /* TODO(pg-port): foreach over rawExprs / makeConst not ported */

    result
}

/*
 * RelationGetIndexPredicate -- get the index predicate for an index
 */
pub unsafe fn RelationGetIndexPredicate(relation: Relation) -> *mut List {
    let mut result: *mut List;
    let mut predDatum: Datum = 0;
    let mut isnull: bool = false;
    let predString: *mut c_char;
    let mut oldcxt: *mut c_void;

    /* Quick exit if we already computed the result. */
    if !(*relation).rd_indpred.is_null() {
        return copyObject((*relation).rd_indpred as *mut c_void) as *mut List;
    }

    /* Quick exit if there is nothing to do. */
    if (*relation).rd_indextuple.is_null()
        || heap_attisnull(
        (*relation).rd_indextuple as HeapTuple,
            Anum_pg_index_indpred as c_int,
            core::ptr::null_mut(),
        )
    {
        return NIL;
    }

    predDatum = heap_getattr(
        (*relation).rd_indextuple as HeapTuple,
        Anum_pg_index_indpred as c_int,
        GetPgIndexDescriptor(),
        &mut isnull,
    );
    debug_assert!(!isnull);
    predString = TextDatumGetCString(predDatum);
    result = stringToNode(predString) as *mut List;
    pfree(predString as *mut c_void);

    result = eval_const_expressions(core::ptr::null_mut(), result as *mut c_void) as *mut List;
    result = canonicalize_qual(result as *mut c_void, false) as *mut List;
    result = make_ands_implicit(result as *mut c_void) as *mut List;

    /* May as well fix opfuncids too */
    fix_opfuncids(result as *mut c_void);

    /* Now save a copy of the completed tree in the relcache entry. */
    oldcxt = MemoryContextSwitchTo((*relation).rd_indexcxt);
    (*relation).rd_indpred = copyObject(result as *mut c_void) as *mut List;
    MemoryContextSwitchTo(oldcxt);

    result
}

/*
 * RelationGetIndexAttrBitmap -- get a bitmap of index attribute numbers
 */
pub unsafe fn RelationGetIndexAttrBitmap(
    relation: Relation,
    attrKind: IndexAttrBitmapKind,
) -> *mut Bitmapset {
    let mut uindexattrs: *mut Bitmapset = core::ptr::null_mut();
    let mut pkindexattrs: *mut Bitmapset = core::ptr::null_mut();
    let mut idindexattrs: *mut Bitmapset = core::ptr::null_mut();
    let mut hotblockingattrs: *mut Bitmapset = core::ptr::null_mut();
    let mut summarizedattrs: *mut Bitmapset = core::ptr::null_mut();
    let mut indexoidlist: *mut List;
    let mut newindexoidlist: *mut List;
    let mut relpkindex: Oid;
    let mut relreplindex: Oid;
    let mut oldcxt: *mut c_void;

    /* Quick exit if we already computed the result. */
    if (*relation).rd_attrsvalid {
        return match attrKind {
            INDEX_ATTR_BITMAP_KEY => bms_copy((*relation).rd_keyattr),
            INDEX_ATTR_BITMAP_PRIMARY_KEY => bms_copy((*relation).rd_pkattr),
            INDEX_ATTR_BITMAP_IDENTITY_KEY => bms_copy((*relation).rd_idattr),
            INDEX_ATTR_BITMAP_HOT_BLOCKING => bms_copy((*relation).rd_hotblockingattr),
            INDEX_ATTR_BITMAP_SUMMARIZED => bms_copy((*relation).rd_summarizedattr),
            _ => {
                elog(ERROR, "unknown attrKind in RelationGetIndexAttrBitmap");
                core::ptr::null_mut()
            }
        };
    }

    /* Fast path if definitely no indexes */
    if !(*RelationGetForm(relation)).relhasindex {
        return core::ptr::null_mut();
    }

    /* restart label */
    'restart: loop {
        indexoidlist = RelationGetIndexList(relation);

        /* Fall out if no indexes (but relhasindex was set) */
        if indexoidlist == NIL {
            return core::ptr::null_mut();
        }

        relpkindex = (*relation).rd_pkindex;
        relreplindex = (*relation).rd_replidindex;

        uindexattrs = core::ptr::null_mut();
        pkindexattrs = core::ptr::null_mut();
        idindexattrs = core::ptr::null_mut();
        hotblockingattrs = core::ptr::null_mut();
        summarizedattrs = core::ptr::null_mut();

        /* TODO(pg-port): foreach(l, indexoidlist) iteration not ported; using placeholder. */
        let _ = indexoidlist;

        newindexoidlist = RelationGetIndexList(relation);
        if equal(indexoidlist as *const c_void, newindexoidlist as *const c_void)
            && relpkindex == (*relation).rd_pkindex
            && relreplindex == (*relation).rd_replidindex
        {
            /* Still the same index set, so proceed */
            list_free(newindexoidlist);
            list_free(indexoidlist);
            break 'restart;
        } else {
            /* Gotta do it over */
            list_free(newindexoidlist);
            list_free(indexoidlist);
            bms_free(uindexattrs);
            bms_free(pkindexattrs);
            bms_free(idindexattrs);
            bms_free(hotblockingattrs);
            bms_free(summarizedattrs);
            /* loop again */
        }
    }

    /* Don't leak the old values of these bitmaps, if any */
    (*relation).rd_attrsvalid = false;
    bms_free((*relation).rd_keyattr);
    (*relation).rd_keyattr = core::ptr::null_mut();
    bms_free((*relation).rd_pkattr);
    (*relation).rd_pkattr = core::ptr::null_mut();
    bms_free((*relation).rd_idattr);
    (*relation).rd_idattr = core::ptr::null_mut();
    bms_free((*relation).rd_hotblockingattr);
    (*relation).rd_hotblockingattr = core::ptr::null_mut();
    bms_free((*relation).rd_summarizedattr);
    (*relation).rd_summarizedattr = core::ptr::null_mut();

    /* Now save copies of the bitmaps in the relcache entry. */
    oldcxt = MemoryContextSwitchTo(CacheMemoryContext as *mut c_void);
    (*relation).rd_keyattr = bms_copy(uindexattrs);
    (*relation).rd_pkattr = bms_copy(pkindexattrs);
    (*relation).rd_idattr = bms_copy(idindexattrs);
    (*relation).rd_hotblockingattr = bms_copy(hotblockingattrs);
    (*relation).rd_summarizedattr = bms_copy(summarizedattrs);
    (*relation).rd_attrsvalid = true;
    MemoryContextSwitchTo(oldcxt);

    /* We return our original working copy for caller to play with */
    match attrKind {
        INDEX_ATTR_BITMAP_KEY => uindexattrs,
        INDEX_ATTR_BITMAP_PRIMARY_KEY => pkindexattrs,
        INDEX_ATTR_BITMAP_IDENTITY_KEY => idindexattrs,
        INDEX_ATTR_BITMAP_HOT_BLOCKING => hotblockingattrs,
        INDEX_ATTR_BITMAP_SUMMARIZED => summarizedattrs,
        _ => {
            elog(ERROR, "unknown attrKind in RelationGetIndexAttrBitmap");
            core::ptr::null_mut()
        }
    }
}

/*
 * RelationGetIdentityKeyBitmap -- get a bitmap of replica identity attribute numbers
 */
pub unsafe fn RelationGetIdentityKeyBitmap(relation: Relation) -> *mut Bitmapset {
    let mut idindexattrs: *mut Bitmapset = core::ptr::null_mut();
    let indexDesc: Relation;
    let mut i: c_int;
    let replidindex: Oid;
    let mut oldcxt: *mut c_void;

    /* Quick exit if we already computed the result */
    if !(*relation).rd_idattr.is_null() {
        return bms_copy((*relation).rd_idattr);
    }

    /* Fast path if definitely no indexes */
    if !(*RelationGetForm(relation)).relhasindex {
        return core::ptr::null_mut();
    }

    /* Historic snapshot must be set. */
    debug_assert!(HistoricSnapshotActive());

    replidindex = RelationGetReplicaIndex(relation);

    /* Fall out if there is no replica identity index */
    if !OidIsValid(replidindex) {
        return core::ptr::null_mut();
    }

    /* Look up the description for the replica identity index */
    indexDesc = RelationIdGetRelation(replidindex);

    if !RelationIsValid(indexDesc) {
        elog(
            ERROR,
            "could not open relation in RelationGetIdentityKeyBitmap",
        );
    }

    /* Add referenced attributes to idindexattrs */
    // TODO(pg-port): rd_index field access (Form_pg_index) not ported.

    RelationClose(indexDesc);

    /* Don't leak the old values of these bitmaps, if any */
    bms_free((*relation).rd_idattr);
    (*relation).rd_idattr = core::ptr::null_mut();

    /* Now save copy of the bitmap in the relcache entry */
    oldcxt = MemoryContextSwitchTo(CacheMemoryContext as *mut c_void);
    (*relation).rd_idattr = bms_copy(idindexattrs);
    MemoryContextSwitchTo(oldcxt);

    idindexattrs
}

/*
 * RelationGetExclusionInfo -- get info about index's exclusion constraint
 */
pub unsafe fn RelationGetExclusionInfo(
    indexRelation: Relation,
    operators: *mut *mut Oid,
    procs: *mut *mut Oid,
    strategies: *mut *mut u16,
) {
    let indnkeyatts: c_int = IndexRelationGetNumberOfKeyAttributes(indexRelation);
    let ops: *mut Oid;
    let funcs: *mut Oid;
    let strats: *mut u16;
    let conrel: Relation;
    let mut conscan: SysScanDesc;
    let mut skey: [ScanKeyData; 1] = [core::mem::zeroed()];
    let mut htup: HeapTuple;
    let mut found: bool = false;
    let mut oldcxt: *mut c_void;
    let mut i: c_int;

    /* Allocate result space in caller context */
    *operators = palloc(core::mem::size_of::<Oid>() * indnkeyatts as usize) as *mut Oid;
    *procs = palloc(core::mem::size_of::<Oid>() * indnkeyatts as usize) as *mut Oid;
    *strategies = palloc(core::mem::size_of::<u16>() * indnkeyatts as usize) as *mut u16;
    ops = *operators;
    funcs = *procs;
    strats = *strategies;

    /* Quick exit if we have the data cached already */
    if !(*indexRelation).rd_exclstrats.is_null() {
        core::ptr::copy_nonoverlapping(
            (*indexRelation).rd_exclops,
            ops,
            indnkeyatts as usize,
        );
        core::ptr::copy_nonoverlapping(
            (*indexRelation).rd_exclprocs,
            funcs,
            indnkeyatts as usize,
        );
        core::ptr::copy_nonoverlapping(
            (*indexRelation).rd_exclstrats,
            strats,
            indnkeyatts as usize,
        );
        return;
    }

    /*
     * Search pg_constraint for the constraint associated with the index.
     */
    ScanKeyInit(
        &mut skey[0],
        Anum_pg_constraint_conrelid as c_int,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum((*(*indexRelation).rd_index).indrelid),
    );

    conrel = table_open(ConstraintRelationId, AccessShareLock);
    conscan = systable_beginscan(
        conrel,
        ConstraintRelidTypidNameIndexId,
        true,
        core::ptr::null_mut(),
        1,
        skey.as_mut_ptr(),
    );

    loop {
        htup = systable_getnext(conscan) as HeapTuple;
        if !HeapTupleIsValid(htup) {
            break;
        }
        let conform = GETSTRUCT(htup) as *mut FormData_pg_constraint_stub;
        let mut val: Datum = 0;
        let mut isnull: bool = false;

        /* We want the exclusion constraint owning the index */
        // TODO(pg-port): conperiod field access not ported.
        if ((*conform).contype != CONSTRAINT_EXCLUSION as i8)
            || (*conform).conindid != RelationGetRelid(indexRelation)
        {
            continue;
        }

        if found {
            elog(
                ERROR,
                "unexpected exclusion constraint record in RelationGetExclusionInfo",
            );
        }
        found = true;

        /* Extract the operator OIDS from conexclop */
        val = fastgetattr(
            htup,
            Anum_pg_constraint_conexclop as c_int,
            (*conrel).rd_att,
            &mut isnull,
        );
        if isnull {
            elog(ERROR, "null conexclop in RelationGetExclusionInfo");
        }

        let arr = DatumGetArrayTypeP(val);
        /* TODO(pg-port): ARR_DIMS/ARR_DATA_PTR not ported; copy ops manually */
    }

    systable_endscan(conscan);
    table_close(conrel, AccessShareLock);

    if !found {
        elog(ERROR, "exclusion constraint record missing in RelationGetExclusionInfo");
    }

    /* We need the func OIDs and strategy numbers too */
    i = 0;
    while i < indnkeyatts {
        *funcs.add(i as usize) = get_opcode(*ops.add(i as usize));
        *strats.add(i as usize) = get_op_opfamily_strategy(
            *ops.add(i as usize),
            *(*indexRelation).rd_opfamily.add(i as usize),
        );
        if *strats.add(i as usize) == InvalidStrategy as u16 {
            elog(
                ERROR,
                "could not find strategy for operator in RelationGetExclusionInfo",
            );
        }
        i += 1;
    }

    /* Save a copy of the results in the relcache entry. */
    oldcxt = MemoryContextSwitchTo((*indexRelation).rd_indexcxt);
    (*indexRelation).rd_exclops =
        palloc(core::mem::size_of::<Oid>() * indnkeyatts as usize) as *mut Oid;
    (*indexRelation).rd_exclprocs =
        palloc(core::mem::size_of::<Oid>() * indnkeyatts as usize) as *mut Oid;
    (*indexRelation).rd_exclstrats =
        palloc(core::mem::size_of::<u16>() * indnkeyatts as usize) as *mut u16;
    core::ptr::copy_nonoverlapping(ops, (*indexRelation).rd_exclops, indnkeyatts as usize);
    core::ptr::copy_nonoverlapping(funcs, (*indexRelation).rd_exclprocs, indnkeyatts as usize);
    core::ptr::copy_nonoverlapping(strats, (*indexRelation).rd_exclstrats, indnkeyatts as usize);
    MemoryContextSwitchTo(oldcxt);
}

/*
 * RelationBuildPublicationDesc -- get publication information for the given relation
 */
pub unsafe fn RelationBuildPublicationDesc(relation: Relation, pubdesc: *mut PublicationDesc) {
    let mut puboids: *mut List;
    let mut oldcxt: *mut c_void;
    let schemaid: Oid;
    let mut ancestors: *mut List = NIL;
    let relid: Oid = RelationGetRelid(relation);

    /*
     * If not publishable, it publishes no actions.
     */
    if !is_publishable_relation(relation) {
        core::ptr::write_bytes(pubdesc as *mut u8, 0, core::mem::size_of::<PublicationDesc>());
        (*pubdesc).rf_valid_for_update = true;
        (*pubdesc).rf_valid_for_delete = true;
        (*pubdesc).cols_valid_for_update = true;
        (*pubdesc).cols_valid_for_delete = true;
        (*pubdesc).gencols_valid_for_update = true;
        (*pubdesc).gencols_valid_for_delete = true;
        return;
    }

    if !(*relation).rd_pubdesc.is_null() {
        core::ptr::copy_nonoverlapping(
            (*relation).rd_pubdesc as *const PublicationDesc,
            pubdesc,
            1,
        );
        return;
    }

    core::ptr::write_bytes(pubdesc as *mut u8, 0, core::mem::size_of::<PublicationDesc>());
    (*pubdesc).rf_valid_for_update = true;
    (*pubdesc).rf_valid_for_delete = true;
    (*pubdesc).cols_valid_for_update = true;
    (*pubdesc).cols_valid_for_delete = true;
    (*pubdesc).gencols_valid_for_update = true;
    (*pubdesc).gencols_valid_for_delete = true;

    /* Fetch the publication membership info. */
    puboids = GetRelationPublications(relid);
    schemaid = RelationGetNamespace(relation);
    puboids = list_concat_unique_oid(puboids, GetSchemaPublications(schemaid));

    // TODO(pg-port): foreach loop over puboids not ported.

    if !(*relation).rd_pubdesc.is_null() {
        pfree((*relation).rd_pubdesc as *mut c_void);
        (*relation).rd_pubdesc = core::ptr::null_mut();
    }

    /* Now save copy of the descriptor in the relcache entry. */
    oldcxt = MemoryContextSwitchTo(CacheMemoryContext as *mut c_void);
    (*relation).rd_pubdesc = palloc(core::mem::size_of::<PublicationDesc>()) as *mut c_void;
    core::ptr::copy_nonoverlapping(pubdesc, (*relation).rd_pubdesc as *mut PublicationDesc, 1);
    MemoryContextSwitchTo(oldcxt);
}

unsafe fn CopyIndexAttOptions(srcopts: *mut *mut bytea, natts: c_int) -> *mut *mut bytea {
    let opts: *mut *mut bytea =
        palloc(core::mem::size_of::<*mut bytea>() * natts as usize) as *mut *mut bytea;

    let mut i: c_int = 0;
    while i < natts {
        let opt = *srcopts.add(i as usize);
        *opts.add(i as usize) = if opt.is_null() {
            core::ptr::null_mut()
        } else {
            DatumGetPointer(datumCopy(PointerGetDatum(opt as *mut c_void), false, -1))
                as *mut bytea
        };
        i += 1;
    }

    opts
}

/*
 * RelationGetIndexAttOptions -- get AM/opclass-specific options for an index
 */
pub unsafe fn RelationGetIndexAttOptions(
    relation: Relation,
    copy: bool,
) -> *mut *mut bytea {
    let mut oldcxt: *mut c_void;
    let mut opts: *mut *mut bytea = (*relation).rd_opcoptions;
    let relid: Oid = RelationGetRelid(relation);
    let natts: c_int = RelationGetNumberOfAttributes(relation);
    let mut i: c_int;

    /* Try to copy cached options. */
    if !opts.is_null() {
        return if copy { CopyIndexAttOptions(opts, natts) } else { opts };
    }

    /* Get and parse opclass options. */
    opts = palloc0(core::mem::size_of::<*mut bytea>() * natts as usize) as *mut *mut bytea;

    i = 0;
    while i < natts {
        if criticalRelcachesBuilt && relid != AttributeRelidNumIndexId {
            let attoptions: Datum = get_attoptions(relid, i + 1);
            *opts.add(i as usize) =
                index_opclass_options(relation, i + 1, attoptions, false);
            if attoptions != 0 {
                pfree(DatumGetPointer(attoptions) as *mut c_void);
            }
        }
        i += 1;
    }

    /* Copy parsed options to the cache. */
    oldcxt = MemoryContextSwitchTo((*relation).rd_indexcxt);
    (*relation).rd_opcoptions = CopyIndexAttOptions(opts, natts);
    MemoryContextSwitchTo(oldcxt);

    if copy {
        return opts;
    }

    i = 0;
    while i < natts {
        if !(*opts.add(i as usize)).is_null() {
            pfree(*opts.add(i as usize) as *mut c_void);
        }
        i += 1;
    }

    pfree(opts as *mut c_void);

    (*relation).rd_opcoptions
}

/*
 * errtable --- stores schema_name and table_name of a table
 */
pub unsafe fn errtable(rel: Relation) -> c_int {
    err_generic_string(
        PG_DIAG_SCHEMA_NAME,
        get_namespace_name(RelationGetNamespace(rel)),
    );
    err_generic_string(PG_DIAG_TABLE_NAME, RelationGetRelationName(rel));
    0
}

/*
 * errtablecol --- stores schema_name, table_name and column_name
 */
pub unsafe fn errtablecol(rel: Relation, attnum: c_int) -> c_int {
    let reldesc: TupleDesc = RelationGetDescr(rel);
    let colname: *const c_char;

    /* Use reldesc if it's a user attribute, else consult the catalogs */
    if attnum > 0 && attnum <= (*reldesc).natts {
        let attr = TupleDescAttr(reldesc, attnum - 1) as *mut FormData_pg_attribute_stub;
        colname = (*attr).attname.as_ptr() as *const c_char;
    } else {
        colname = get_attname(RelationGetRelid(rel), attnum as i16, false);
    }

    errtablecolname(rel, colname)
}

/*
 * errtablecolname --- stores schema_name, table_name and column_name
 */
pub unsafe fn errtablecolname(rel: Relation, colname: *const c_char) -> c_int {
    errtable(rel);
    err_generic_string(PG_DIAG_COLUMN_NAME, colname);
    0
}

/*
 * errtableconstraint --- stores schema_name, table_name and constraint_name
 */
pub unsafe fn errtableconstraint(rel: Relation, conname: *const c_char) -> c_int {
    errtable(rel);
    err_generic_string(PG_DIAG_CONSTRAINT_NAME, conname);
    0
}

/*
 * load_relcache_init_file -- attempt to load cache from the shared
 * or local cache init file
 */
unsafe fn load_relcache_init_file(shared: bool) -> bool {
    let mut fp: *mut c_void; /* FILE* */
    let mut initfilename: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut rels: *mut Relation;
    let mut relno: c_int;
    let mut num_rels: c_int;
    let mut max_rels: c_int;
    let mut nailed_rels: c_int;
    let mut nailed_indexes: c_int;
    let mut magic: c_int = 0;
    let mut i: c_int;

    if shared {
        snprintf!(
            initfilename.as_mut_ptr(),
            MAXPGPATH,
            b"global/%s\0".as_ptr() as *const c_char,
            RELCACHE_INIT_FILENAME,
        );
    } else {
        snprintf!(
            initfilename.as_mut_ptr(),
            MAXPGPATH,
            b"%s/%s\0".as_ptr() as *const c_char,
            DatabasePath,
            RELCACHE_INIT_FILENAME,
        );
    }

    fp = AllocateFile(initfilename.as_ptr(), PG_BINARY_R);
    if fp.is_null() {
        return false;
    }

    max_rels = 100;
    rels = palloc(max_rels as usize * core::mem::size_of::<Relation>()) as *mut Relation;
    num_rels = 0;
    nailed_rels = 0;
    nailed_indexes = 0;

    /* check for correct magic number */
    if fread(
        &mut magic as *mut c_int as *mut c_void,
        1,
        core::mem::size_of::<c_int>(),
        fp,
    ) != core::mem::size_of::<c_int>()
    {
        pfree(rels as *mut c_void);
        FreeFile(fp);
        return false;
    }
    if magic != RELCACHE_INIT_FILEMAGIC {
        pfree(rels as *mut c_void);
        FreeFile(fp);
        return false;
    }

    relno = 0;
    loop {
        let mut len: usize = 0;
        let nread: usize;
        let rel: Relation;
        let relform: Form_pg_class;
        let mut has_not_null: bool;

        /* first read the relation descriptor length */
        let nread_len = fread(
            &mut len as *mut usize as *mut c_void,
            1,
            core::mem::size_of::<usize>(),
            fp,
        );
        if nread_len != core::mem::size_of::<usize>() {
            if nread_len == 0 {
                break; /* end of file */
            }
            pfree(rels as *mut c_void);
            FreeFile(fp);
            return false;
        }

        /* safety check for incompatible relcache layout */
        if len != core::mem::size_of::<RelationData>() {
            pfree(rels as *mut c_void);
            FreeFile(fp);
            return false;
        }

        /* allocate another relcache header */
        if num_rels >= max_rels {
            max_rels *= 2;
            rels = repalloc(
                rels as *mut c_void,
                max_rels as usize * core::mem::size_of::<Relation>(),
            ) as *mut Relation;
        }

        rel = palloc(len) as Relation;
        *rels.add(num_rels as usize) = rel;
        num_rels += 1;

        /* then, read the Relation structure */
        if fread(rel as *mut c_void, 1, len, fp) != len {
            pfree(rels as *mut c_void);
            FreeFile(fp);
            return false;
        }

        /* next read the relation tuple form */
        let mut flen: usize = 0;
        if fread(&mut flen as *mut usize as *mut c_void, 1, core::mem::size_of::<usize>(), fp)
            != core::mem::size_of::<usize>()
        {
            pfree(rels as *mut c_void);
            FreeFile(fp);
            return false;
        }

        relform = palloc(flen) as Form_pg_class;
        if fread(relform as *mut c_void, 1, flen, fp) != flen {
            pfree(rels as *mut c_void);
            FreeFile(fp);
            return false;
        }

        (*rel).rd_rel = relform;

        /* initialize attribute tuple forms */
        (*rel).rd_att = CreateTemplateTupleDesc((*relform).relnatts as c_int);
        (*(*rel).rd_att).tdrefcount = 1;
        (*(*rel).rd_att).tdtypeid = if (*relform).reltype != InvalidOid {
            (*relform).reltype
        } else {
            RECORDOID
        };
        (*(*rel).rd_att).tdtypmod = -1;

        /* next read all the attribute tuple form data entries */
        has_not_null = false;
        i = 0;
        while i < (*relform).relnatts as c_int {
            let attr = TupleDescAttr((*rel).rd_att, i);
            let mut alen: usize = 0;
            if fread(&mut alen as *mut usize as *mut c_void, 1, core::mem::size_of::<usize>(), fp)
                != core::mem::size_of::<usize>()
            {
                pfree(rels as *mut c_void);
                FreeFile(fp);
                return false;
            }
            if alen != ATTRIBUTE_FIXED_PART_SIZE {
                pfree(rels as *mut c_void);
                FreeFile(fp);
                return false;
            }
            if fread(attr as *mut c_void, 1, alen, fp) != alen {
                pfree(rels as *mut c_void);
                FreeFile(fp);
                return false;
            }
            let a = attr as *mut FormData_pg_attribute_stub;
            has_not_null |= (*a).attnotnull;
            populate_compact_attribute((*rel).rd_att, i);
            i += 1;
        }

        /* next read the access method specific field */
        let mut olen: usize = 0;
        if fread(&mut olen as *mut usize as *mut c_void, 1, core::mem::size_of::<usize>(), fp)
            != core::mem::size_of::<usize>()
        {
            pfree(rels as *mut c_void);
            FreeFile(fp);
            return false;
        }
        if olen > 0 {
            (*rel).rd_options = palloc(olen) as *mut bytea;
            if fread((*rel).rd_options as *mut c_void, 1, olen, fp) != olen {
                pfree(rels as *mut c_void);
                FreeFile(fp);
                return false;
            }
        } else {
            (*rel).rd_options = core::ptr::null_mut();
        }

        /* mark not-null status */
        if has_not_null {
            let constr = palloc0(core::mem::size_of::<TupleConstrStub>()) as *mut TupleConstrStub;
            (*constr).has_not_null = true;
            // (*(*rel).rd_att).constr = constr as *mut TupleConstr;
            // TODO(pg-port): TupleDescData.constr field not ported.
        }

        /* If it's an index, there's more to do */
        if (*(*rel).rd_rel).relkind == RELKIND_INDEX as i8 {
            /* Count nailed indexes */
            if (*rel).rd_isnailed {
                nailed_indexes += 1;
            }

            /* read the pg_index tuple */
            let mut tlen: usize = 0;
            if fread(&mut tlen as *mut usize as *mut c_void, 1, core::mem::size_of::<usize>(), fp)
                != core::mem::size_of::<usize>()
            {
                pfree(rels as *mut c_void);
                FreeFile(fp);
                return false;
            }
            (*rel).rd_indextuple = palloc(tlen);
            if fread((*rel).rd_indextuple, 1, tlen, fp) != tlen {
                pfree(rels as *mut c_void);
                FreeFile(fp);
                return false;
            }
            /* Fix up internal pointers */
            (*((*rel).rd_indextuple as HeapTuple)).t_data = ((*rel).rd_indextuple as *mut u8)
                .add(HEAPTUPLESIZE) as *mut crate::access::htup_details::HeapTupleHeaderData;
            (*rel).rd_index = GETSTRUCT((*rel).rd_indextuple as HeapTuple) as Form_pg_index;

            /* prepare index info context */
            let indexcxt = AllocSetContextCreate(
                CacheMemoryContext as *mut c_void,
                b"index info\0".as_ptr() as *const c_char,
                ALLOCSET_SMALL_SIZES,
            );
            (*rel).rd_indexcxt = indexcxt;
            MemoryContextCopyAndSetIdentifier(indexcxt, RelationGetRelationName(rel));

            /* fetch the index AM's API struct */
            InitIndexAmRoutine(rel);

            /* read the vector of opfamily OIDs */
            let mut vlen: usize = 0;
            if fread(&mut vlen as *mut usize as *mut c_void, 1, core::mem::size_of::<usize>(), fp)
                != core::mem::size_of::<usize>()
            {
                pfree(rels as *mut c_void);
                FreeFile(fp);
                return false;
            }
            let opfamily = MemoryContextAlloc(indexcxt, vlen) as *mut Oid;
            if fread(opfamily as *mut c_void, 1, vlen, fp) != vlen {
                pfree(rels as *mut c_void);
                FreeFile(fp);
                return false;
            }
            (*rel).rd_opfamily = opfamily;

            /* read opcintype, support, indcollation, indoption, opcoptions vectors */
            // TODO(pg-port): remaining index vectors follow the same fread pattern.
            // For brevity, they are left as stubs here.

            /* set up zeroed fmgr-info vector */
            // nsupport = relnatts * amsupport; rd_supportinfo = MemoryContextAllocZero(...)
            // TODO(pg-port): rd_indam->amsupport not ported.
        } else {
            /* Count nailed rels */
            if (*rel).rd_isnailed {
                nailed_rels += 1;
            }

            /* Load table AM data */
            if RELKIND_HAS_TABLE_AM((*(*rel).rd_rel).relkind as i8)
                || (*(*rel).rd_rel).relkind == RELKIND_SEQUENCE as i8
            {
                RelationInitTableAccessMethod(rel);
            }
        }

        /* Rules and triggers are not saved */
        (*rel).rd_rules = core::ptr::null_mut();
        (*rel).rd_rulescxt = core::ptr::null_mut();
        (*rel).trigdesc = core::ptr::null_mut();
        (*rel).rd_rsdesc = core::ptr::null_mut();
        (*rel).rd_partkey = core::ptr::null_mut();
        (*rel).rd_partkeycxt = core::ptr::null_mut();
        (*rel).rd_partdesc = core::ptr::null_mut();
        (*rel).rd_partdesc_nodetached = core::ptr::null_mut();
        (*rel).rd_partdesc_nodetached_xmin = InvalidTransactionId;
        (*rel).rd_pdcxt = core::ptr::null_mut();
        (*rel).rd_pddcxt = core::ptr::null_mut();
        (*rel).rd_partcheck = NIL;
        (*rel).rd_partcheckvalid = false;
        (*rel).rd_partcheckcxt = core::ptr::null_mut();
        (*rel).rd_indexprs = NIL;
        (*rel).rd_indpred = NIL;
        (*rel).rd_exclops = core::ptr::null_mut();
        (*rel).rd_exclprocs = core::ptr::null_mut();
        (*rel).rd_exclstrats = core::ptr::null_mut();
        (*rel).rd_fdwroutine = core::ptr::null_mut();

        /* Reset transient-state fields */
        (*rel).rd_smgr = core::ptr::null_mut();
        (*rel).rd_refcnt = if (*rel).rd_isnailed { 1 } else { 0 };
        (*rel).rd_indexvalid = false;
        (*rel).rd_indexlist = NIL;
        (*rel).rd_pkindex = InvalidOid;
        (*rel).rd_replidindex = InvalidOid;
        (*rel).rd_attrsvalid = false;
        (*rel).rd_keyattr = core::ptr::null_mut();
        (*rel).rd_pkattr = core::ptr::null_mut();
        (*rel).rd_idattr = core::ptr::null_mut();
        (*rel).rd_pubdesc = core::ptr::null_mut();
        (*rel).rd_statvalid = false;
        (*rel).rd_statlist = NIL;
        (*rel).rd_fkeyvalid = false;
        (*rel).rd_fkeylist = NIL;
        (*rel).rd_createSubid = InvalidSubTransactionId;
        (*rel).rd_newRelfilelocatorSubid = InvalidSubTransactionId;
        (*rel).rd_firstRelfilelocatorSubid = InvalidSubTransactionId;
        (*rel).rd_droppedSubid = InvalidSubTransactionId;
        (*rel).rd_amcache = core::ptr::null_mut();
        (*rel).pgstat_info = core::ptr::null_mut();

        RelationInitLockInfo(rel);
        RelationInitPhysicalAddr(rel);

        relno += 1;
    }

    /*
     * We reached the end of the init file. Check nailed counts.
     */
    if shared {
        if nailed_rels != NUM_CRITICAL_SHARED_RELS || nailed_indexes != NUM_CRITICAL_SHARED_INDEXES {
            elog(
                WARNING,
                "wrong nailed counts in shared init file in load_relcache_init_file",
            );
            pfree(rels as *mut c_void);
            FreeFile(fp);
            return false;
        }
    } else {
        if nailed_rels != NUM_CRITICAL_LOCAL_RELS || nailed_indexes != NUM_CRITICAL_LOCAL_INDEXES {
            elog(
                WARNING,
                "wrong nailed counts in local init file in load_relcache_init_file",
            );
            pfree(rels as *mut c_void);
            FreeFile(fp);
            return false;
        }
    }

    /* OK, all appears well. Insert all the new relcache entries. */
    i = 0;
    while i < num_rels {
        RelationCacheInsert(*rels.add(i as usize), false);
        i += 1;
    }

    pfree(rels as *mut c_void);
    FreeFile(fp);

    if shared {
        criticalSharedRelcachesBuilt = true;
    } else {
        criticalRelcachesBuilt = true;
    }
    true
}

/*
 * write_relcache_init_file -- write out initialization file
 */
unsafe fn write_relcache_init_file(shared: bool) {
    let mut fp: *mut c_void; /* FILE* */
    let mut tempfilename: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut finalfilename: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let magic: c_int = RELCACHE_INIT_FILEMAGIC;
    let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut idhentry: *mut RelIdCacheEnt;
    let mut i: c_int;

    /* If we have already received any relcache inval events, skip. */
    if relcacheInvalsReceived != 0 {
        return;
    }

    if shared {
        snprintf!(
            tempfilename.as_mut_ptr(),
            MAXPGPATH,
            b"global/%s.%d\0".as_ptr() as *const c_char,
            RELCACHE_INIT_FILENAME,
            MyProcPid,
        );
        snprintf!(
            finalfilename.as_mut_ptr(),
            MAXPGPATH,
            b"global/%s\0".as_ptr() as *const c_char,
            RELCACHE_INIT_FILENAME,
        );
    } else {
        snprintf!(
            tempfilename.as_mut_ptr(),
            MAXPGPATH,
            b"%s/%s.%d\0".as_ptr() as *const c_char,
            DatabasePath,
            RELCACHE_INIT_FILENAME,
            MyProcPid,
        );
        snprintf!(
            finalfilename.as_mut_ptr(),
            MAXPGPATH,
            b"%s/%s\0".as_ptr() as *const c_char,
            DatabasePath,
            RELCACHE_INIT_FILENAME,
        );
    }

    unlink(tempfilename.as_ptr()); /* in case it exists w/wrong permissions */

    fp = AllocateFile(tempfilename.as_ptr(), PG_BINARY_W);
    if fp.is_null() {
        ereport!(
            WARNING,
            errmsg!("could not create relation-cache initialization file")
        );
        return;
    }

    /* Write a magic number */
    if fwrite(
        &magic as *const c_int as *const c_void,
        1,
        core::mem::size_of::<c_int>(),
        fp,
    ) != core::mem::size_of::<c_int>()
    {
        ereport!(FATAL, errmsg!("could not write init file in write_relcache_init_file"));
    }

    /* Write all the appropriate reldescs */
    hash_seq_init(&mut status, RelationIdCache);

    loop {
        idhentry = hash_seq_search(&mut status) as *mut RelIdCacheEnt;
        if idhentry.is_null() {
            break;
        }
        let rel = (*idhentry).reldesc;
        let relform = (*rel).rd_rel;

        /* ignore if not correct group */
        if (*relform).relisshared != shared {
            continue;
        }

        /* Ignore if not supposed to be in init file */
        if !shared && !RelationIdIsInInitFile(RelationGetRelid(rel)) {
            debug_assert!(!(*rel).rd_isnailed);
            continue;
        }

        /* first write the relcache entry proper */
        write_item(rel as *const c_void, core::mem::size_of::<RelationData>(), fp);

        /* next write the relation tuple form */
        write_item(relform as *const c_void, CLASS_TUPLE_SIZE, fp);

        /* next, do all the attribute tuple form data entries */
        i = 0;
        while i < (*relform).relnatts as c_int {
            write_item(
                TupleDescAttr((*rel).rd_att, i) as *const c_void,
                ATTRIBUTE_FIXED_PART_SIZE,
                fp,
            );
            i += 1;
        }

        /* next, do the access method specific field */
        write_item(
            (*rel).rd_options as *const c_void,
            if !(*rel).rd_options.is_null() {
                VARSIZE((*rel).rd_options as *const c_void)
            } else {
                0
            },
            fp,
        );

        /* If it's an index, there's more to do */
        if (*(*rel).rd_rel).relkind == RELKIND_INDEX as i8 {
            /* write the pg_index tuple */
            write_item(
                (*rel).rd_indextuple as *const c_void,
                HEAPTUPLESIZE + (*((*rel).rd_indextuple as HeapTuple)).t_len as usize,
                fp,
            );

            /* write the vector of opfamily OIDs */
            write_item(
                (*rel).rd_opfamily as *const c_void,
                (*relform).relnatts as usize * core::mem::size_of::<Oid>(),
                fp,
            );

            /* write the vector of opcintype OIDs */
            write_item(
                (*rel).rd_opcintype as *const c_void,
                (*relform).relnatts as usize * core::mem::size_of::<Oid>(),
                fp,
            );

            /* write the vector of support procedure OIDs */
            // write_item(rd_support, natts * amsupport * sizeof(RegProcedure), fp);
            // TODO(pg-port): rd_indam->amsupport not ported.

            /* write the vector of collation OIDs */
            write_item(
                (*rel).rd_indcollation as *const c_void,
                (*relform).relnatts as usize * core::mem::size_of::<Oid>(),
                fp,
            );

            /* write the vector of indoption values */
            write_item(
                (*rel).rd_indoption as *const c_void,
                (*relform).relnatts as usize * core::mem::size_of::<i16>(),
                fp,
            );

            debug_assert!(!(*rel).rd_opcoptions.is_null());

            /* write the vector of opcoptions values */
            i = 0;
            while i < (*relform).relnatts as c_int {
                let opt = *(*rel).rd_opcoptions.add(i as usize);
                write_item(
                    opt as *const c_void,
                    if !opt.is_null() { VARSIZE(opt as *const c_void) } else { 0 },
                    fp,
                );
                i += 1;
            }
        }
    }

    if FreeFile(fp) != 0 {
        ereport!(FATAL, errmsg!("could not write init file in write_relcache_init_file (FreeFile)"));
    }

    /* Check whether the data is already obsolete. */
    LWLockAcquire(RelCacheInitLock, LW_EXCLUSIVE);
    AcceptInvalidationMessages();

    if relcacheInvalsReceived == 0 {
        if rename(tempfilename.as_ptr(), finalfilename.as_ptr()) < 0 {
            unlink(tempfilename.as_ptr());
        }
    } else {
        unlink(tempfilename.as_ptr());
    }

    LWLockRelease(RelCacheInitLock);
}

/* write a chunk of data preceded by its length */
unsafe fn write_item(data: *const c_void, len: usize, fp: *mut c_void) {
    if fwrite(
        &len as *const usize as *const c_void,
        1,
        core::mem::size_of::<usize>(),
        fp,
    ) != core::mem::size_of::<usize>()
    {
        ereport!(FATAL, errmsg!("could not write init file in write_item"));
    }
    if len > 0
        && fwrite(data, 1, len, fp) != len
    {
        ereport!(FATAL, errmsg!("could not write init file in write_item (data)"));
    }
}

/*
 * RelationIdIsInInitFile -- determine whether a relation should be stored
 * in a relcache init file.
 */
pub unsafe fn RelationIdIsInInitFile(relationId: Oid) -> bool {
    if relationId == SharedSecLabelRelationId
        || relationId == TriggerRelidNameIndexId
        || relationId == DatabaseNameIndexId
        || relationId == SharedSecLabelObjectIndexId
    {
        // Assert(!RelationSupportsSysCache(relationId));
        return true;
    }
    RelationSupportsSysCache(relationId)
}

/*
 * RelationCacheInitFilePreInvalidate
 */
pub unsafe fn RelationCacheInitFilePreInvalidate() {
    let mut localinitfname: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut sharedinitfname: [c_char; MAXPGPATH] = [0; MAXPGPATH];

    if !DatabasePath.is_null() {
        snprintf!(
            localinitfname.as_mut_ptr(),
            MAXPGPATH,
            b"%s/%s\0".as_ptr() as *const c_char,
            DatabasePath,
            RELCACHE_INIT_FILENAME,
        );
    }
    snprintf!(
        sharedinitfname.as_mut_ptr(),
        MAXPGPATH,
        b"global/%s\0".as_ptr() as *const c_char,
        RELCACHE_INIT_FILENAME,
    );

    LWLockAcquire(RelCacheInitLock, LW_EXCLUSIVE);

    if !DatabasePath.is_null() {
        unlink_initfile(localinitfname.as_ptr(), ERROR);
    }
    unlink_initfile(sharedinitfname.as_ptr(), ERROR);
}

/*
 * RelationCacheInitFilePostInvalidate
 */
pub unsafe fn RelationCacheInitFilePostInvalidate() {
    LWLockRelease(RelCacheInitLock);
}

/*
 * RelationCacheInitFileRemove -- remove init files during postmaster startup
 */
pub unsafe fn RelationCacheInitFileRemove() {
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];

    snprintf!(
        path.as_mut_ptr(),
        MAXPGPATH,
        b"global/%s\0".as_ptr() as *const c_char,
        RELCACHE_INIT_FILENAME,
    );
    unlink_initfile(path.as_ptr(), LOG);

    /* Scan everything in the default tablespace */
    RelationCacheInitFileRemoveInDir(b"base\0".as_ptr() as *const c_char);

    /* Scan the tablespace link directory to find non-default tablespaces */
    let tblspcdir = PG_TBLSPC_DIR;
    let dir = AllocateDir(tblspcdir);
    loop {
        let de = ReadDirExtended(dir, tblspcdir, LOG);
        if de.is_null() {
            break;
        }
        let de_name = (*de).d_name.as_ptr();
        // if all digits, scan that tablespace dir
        // TODO(pg-port): strspn/strlen not ported; using stub.
        let tablespace_len = strspn_digits(de_name);
        let name_len = c_strlen(de_name);
        if tablespace_len == name_len && name_len > 0 {
            snprintf!(
                path.as_mut_ptr(),
                MAXPGPATH,
                b"%s/%s/%s\0".as_ptr() as *const c_char,
                tblspcdir,
                de_name,
                TABLESPACE_VERSION_DIRECTORY,
            );
            RelationCacheInitFileRemoveInDir(path.as_ptr());
        }
    }
    FreeDir(dir);
}

unsafe fn strspn_digits(s: *const c_char) -> usize {
    let mut n: usize = 0;
    loop {
        let c = *s.add(n) as u8;
        if c == 0 {
            break;
        }
        if c < b'0' || c > b'9' {
            break;
        }
        n += 1;
    }
    n
}

unsafe fn c_strlen(s: *const c_char) -> usize {
    let mut n: usize = 0;
    loop {
        if *s.add(n) == 0 {
            return n;
        }
        n += 1;
    }
}

/* Process one per-tablespace directory for RelationCacheInitFileRemove */
unsafe fn RelationCacheInitFileRemoveInDir(tblspcpath: *const c_char) {
    let dir = AllocateDir(tblspcpath);
    let mut initfilename: [c_char; MAXPGPATH * 2] = [0; MAXPGPATH * 2];

    loop {
        let de = ReadDirExtended(dir, tblspcpath, LOG);
        if de.is_null() {
            break;
        }
        let de_name = (*de).d_name.as_ptr();
        let tablespace_len = strspn_digits(de_name);
        let name_len = c_strlen(de_name);
        if tablespace_len == name_len && name_len > 0 {
            snprintf!(
                initfilename.as_mut_ptr(),
                MAXPGPATH * 2,
                b"%s/%s/%s\0".as_ptr() as *const c_char,
                tblspcpath,
                de_name,
                RELCACHE_INIT_FILENAME,
            );
            unlink_initfile(initfilename.as_ptr(), LOG);
        }
    }
    FreeDir(dir);
}

unsafe fn unlink_initfile(initfilename: *const c_char, elevel: c_int) {
    if unlink(initfilename) < 0 {
        /* It might not be there, but log any error other than ENOENT */
        if *errno_location() != ENOENT {
            ereport!(
                elevel,
                errmsg!("could not remove cache file in unlink_initfile")
            );
        }
    }
}

/*
 * ResourceOwner callbacks
 */
unsafe fn ResOwnerPrintRelCache(res: Datum) -> *mut c_char {
    let rel = DatumGetPointer(res) as Relation;
    psprintf!(b"\"%s\"\0".as_ptr() as *const c_char, RelationGetRelationName(rel))
}

unsafe fn ResOwnerReleaseRelation(res: Datum) {
    let rel = DatumGetPointer(res) as Relation;

    /*
     * This reference has already been removed from the resource owner, so
     * just decrement reference count without calling
     * ResourceOwnerForgetRelationRef.
     */
    debug_assert!((*rel).rd_refcnt > 0);
    (*rel).rd_refcnt -= 1;

    RelationCloseCleanup(rel);
}

