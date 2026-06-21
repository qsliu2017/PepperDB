//! relation.c - PostgreSQL logical replication relation mapping cache.
//!
//! Routines in this file mainly have to do with mapping the properties
//! of local replication target relations to the properties of their
//! remote counterpart.

use crate::prelude::*;

use core::ffi::CStr;

use crate::access::attnum::{
    AttrNumber, AttrNumberGetAttrOffset, AttrNumberIsForUserDefinedAttr,
    AttributeNumberIsValid,
};
use crate::access::htup_details::HeapTuple;
use crate::c::{int2vector, oidvector, NameStr};
use crate::access::common::attmap::{free_attrmap, make_attrmap, AttrMap};
use crate::access::common::heaptuple::heap_attisnull;
use crate::access::common::tupdesc::{TupleDesc, TupleDescAttr};
use crate::access::index::amapi::{
    GetIndexAmRoutineByAmId, IndexAmTranslateCompareType,
};
use crate::access::index::indexam::{index_close, index_open};
use crate::access::table::table::{table_close, table_open, try_table_open};
use crate::catalog::pg_attribute::Form_pg_attribute;
use crate::catalog::pg_index::FormData_pg_index;
use crate::catalog::pg_class::REPLICA_IDENTITY_FULL;
use crate::nodes::bitmapset::{
    bms_add_member, bms_add_range, bms_copy, bms_del_member, bms_free, bms_is_empty,
    bms_is_member, bms_next_member, Bitmapset,
};
use crate::nodes::makefuncs::makeRangeVar;
use crate::lib::stringinfo::{appendStringInfoString, initStringInfo, StringInfoData};
use crate::appendStringInfo;
use crate::replication::logicalproto::{LogicalRepRelation, LogicalRepRelId};
use crate::replication::logicalrelation::LogicalRepRelMapEntry;
use crate::storage::lockdefs::{AccessShareLock, NoLock, LOCKMODE};
use crate::utils::cache::inval::CacheRegisterRelcacheCallback;
use crate::utils::cache::lsyscache::get_opclass_family;
use crate::utils::cache::typcache::{lookup_type_cache, TypeCacheEntry, TYPECACHE_EQ_OPR_FINFO};
use crate::utils::hash::dynahash::{
    hash_create, hash_search, hash_seq_init, hash_seq_search, hash_seq_term, HASHCTL,
    HASH_BLOBS, HASH_CONTEXT, HASH_ELEM, HASH_SEQ_STATUS, HTAB,
    HASHACTION::{HASH_ENTER, HASH_FIND},
};
use crate::utils::mmgr::mcxt::CacheMemoryContext;
use crate::utils::rel::{Relation, RelationGetDescr, RelationGetRelid};

// ---------------------------------------------------------------------------
// Dependencies on functions defined in other .c files, stubbed for now.
// ---------------------------------------------------------------------------

// catalog/pg_class.h: relation kind for partitioned tables.
const RELKIND_PARTITIONED_TABLE: c_char = b'p' as c_char;

// catalog/pg_subscription_rel.h: sync state "ready".
const SUBREL_STATE_READY: c_char = b'r' as c_char;

// access/stratnum.h
const InvalidStrategy: u16 = 0;

// access/cmptype.h: CompareType enum value for equality.
const COMPARE_EQ: c_int = 1;

// access/sysattr.h
const FirstLowInvalidHeapAttributeNumber: c_int = -8;

// utils/rel.h: IndexAttrBitmapKind values.
const INDEX_ATTR_BITMAP_PRIMARY_KEY: c_int = 0;
const INDEX_ATTR_BITMAP_IDENTITY_KEY: c_int = 4;

// utils/syscache.h: SysCacheIdentifier for pg_index by index relid.
const INDEXRELID: c_int = 34;

// catalog/pg_index.h: 1-based attribute numbers.
const Anum_pg_index_indpred: AttrNumber = 21;
const Anum_pg_index_indclass: AttrNumber = 18;

// TODO(pg-port): real lives in utils/cache/relcache.c
pub unsafe fn RelationGetIndexList(relation: Relation) -> *mut crate::nodes::pg_list::List {
    crate::utils::cache::relcache::RelationGetIndexList(relation as _) as _
}

// TODO(pg-port): real lives in utils/cache/relcache.c
pub unsafe fn RelationGetIndexAttrBitmap(
    relation: Relation,
    attrKind: c_int,
) -> *mut Bitmapset {
    let _ = (relation, attrKind);
    core::ptr::null_mut()
}

// TODO(pg-port): real lives in utils/cache/relcache.c
pub unsafe fn RelationGetReplicaIndex(rel: Relation) -> Oid {
    let _ = rel;
    InvalidOid
}

// TODO(pg-port): real lives in utils/cache/relcache.c
pub unsafe fn RelationGetPrimaryKeyIndex(rel: Relation, deferrable_ok: bool) -> Oid {
    let _ = (rel, deferrable_ok);
    InvalidOid
}

// TODO(pg-port): real lives in catalog/pg_subscription_rel.c
pub unsafe fn GetSubscriptionRelState(
    subid: Oid,
    relid: Oid,
    sublsn: *mut crate::access::transam::xlogdefs::XLogRecPtr,
) -> c_char {
    let _ = (subid, relid, sublsn);
    SUBREL_STATE_READY
}

// TODO(pg-port): real lives in executor/execReplication.c
pub unsafe fn CheckSubscriptionRelkind(relkind: c_char, nspname: *const c_char, relname: *const c_char) {
    let _ = (relkind, nspname, relname);
}

// TODO(pg-port): real lives in utils/cache/syscache.c
pub unsafe fn SysCacheGetAttrNotNull(
    cacheId: c_int,
    tup: HeapTuple,
    attributeNumber: AttrNumber,
) -> Datum {
    let _ = (cacheId, tup, attributeNumber);
    0
}

// TODO(pg-port): real MySubscription lives in replication/worker_internal.h
pub static mut MySubscription: *mut MySubscriptionStub = core::ptr::null_mut();
#[repr(C)]
pub struct MySubscriptionStub {
    pub oid: Oid,
}

// gettext no-op: C `_(x)` marks a string for translation; identity here.
#[inline]
fn _(s: *const c_char) -> *const c_char {
    s
}

// ---------------------------------------------------------------------------

static mut LogicalRepRelMapContext: MemoryContext = core::ptr::null_mut();

static mut LogicalRepRelMap: *mut HTAB = core::ptr::null_mut();

/*
 * Partition map (LogicalRepPartMap)
 *
 * When a partitioned table is used as replication target, replicated
 * operations are actually performed on its leaf partitions, which requires
 * the partitions to also be mapped to the remote relation.  Parent's entry
 * (LogicalRepRelMapEntry) cannot be used as-is for all partitions, because
 * individual partitions may have different attribute numbers, which means
 * attribute mappings to remote relation's attributes must be maintained
 * separately for each partition.
 */
static mut LogicalRepPartMapContext: MemoryContext = core::ptr::null_mut();
static mut LogicalRepPartMap: *mut HTAB = core::ptr::null_mut();

#[repr(C)]
pub struct LogicalRepPartMapEntry {
    pub partoid: Oid, /* LogicalRepPartMap's key */
    pub relmapentry: LogicalRepRelMapEntry,
}

/*
 * Relcache invalidation callback for our relation map cache.
 */
unsafe extern "C" fn logicalrep_relmap_invalidate_cb(arg: Datum, reloid: Oid) {
    let _ = arg;
    let mut entry: *mut LogicalRepRelMapEntry;

    /* Just to be sure. */
    if LogicalRepRelMap.is_null() {
        return;
    }

    if reloid != InvalidOid {
        let mut status: HASH_SEQ_STATUS = core::mem::zeroed();

        hash_seq_init(&mut status, LogicalRepRelMap);

        /* TODO, use inverse lookup hashtable? */
        loop {
            entry = hash_seq_search(&mut status) as *mut LogicalRepRelMapEntry;
            if entry.is_null() {
                break;
            }
            if (*entry).localreloid == reloid {
                (*entry).localrelvalid = false;
                hash_seq_term(&mut status);
                break;
            }
        }
    } else {
        /* invalidate all cache entries */
        let mut status: HASH_SEQ_STATUS = core::mem::zeroed();

        hash_seq_init(&mut status, LogicalRepRelMap);

        loop {
            entry = hash_seq_search(&mut status) as *mut LogicalRepRelMapEntry;
            if entry.is_null() {
                break;
            }
            (*entry).localrelvalid = false;
        }
    }
}

/*
 * Initialize the relation map cache.
 */
unsafe fn logicalrep_relmap_init() {
    let mut ctl: HASHCTL = core::mem::zeroed();

    if LogicalRepRelMapContext.is_null() {
        LogicalRepRelMapContext = AllocSetContextCreate!(
            CacheMemoryContext,
            c"LogicalRepRelMapContext".as_ptr(),
            ALLOCSET_DEFAULT_SIZES
        );
    }

    /* Initialize the relation hash table. */
    ctl.keysize = core::mem::size_of::<LogicalRepRelId>();
    ctl.entrysize = core::mem::size_of::<LogicalRepRelMapEntry>();
    ctl.hcxt = LogicalRepRelMapContext;

    LogicalRepRelMap = hash_create(
        c"logicalrep relation map cache".as_ptr(),
        128,
        &mut ctl,
        HASH_ELEM | HASH_BLOBS | HASH_CONTEXT,
    );

    /* Watch for invalidation events. */
    CacheRegisterRelcacheCallback(logicalrep_relmap_invalidate_cb, 0 as Datum);
}

/*
 * Free the entry of a relation map cache.
 */
unsafe fn logicalrep_relmap_free_entry(entry: *mut LogicalRepRelMapEntry) {
    let remoterel: *mut LogicalRepRelation = &mut (*entry).remoterel;

    pfree((*remoterel).nspname as *mut c_void);
    pfree((*remoterel).relname as *mut c_void);

    if (*remoterel).natts > 0 {
        let mut i: c_int = 0;

        while i < (*remoterel).natts {
            pfree(*(*remoterel).attnames.add(i as usize) as *mut c_void);
            i += 1;
        }

        pfree((*remoterel).attnames as *mut c_void);
        pfree((*remoterel).atttyps as *mut c_void);
    }
    bms_free((*remoterel).attkeys);

    if !(*entry).attrmap.is_null() {
        free_attrmap((*entry).attrmap);
    }
}

/*
 * Add new entry or update existing entry in the relation map cache.
 *
 * Called when new relation mapping is sent by the publisher to update
 * our expected view of incoming data from said publisher.
 */
#[no_mangle]
pub unsafe fn logicalrep_relmap_update(remoterel: *mut LogicalRepRelation) {
    let oldctx: MemoryContext;
    let entry: *mut LogicalRepRelMapEntry;
    let mut found: bool = false;
    let mut i: c_int;

    if LogicalRepRelMap.is_null() {
        logicalrep_relmap_init();
    }

    /*
     * HASH_ENTER returns the existing entry if present or creates a new one.
     */
    entry = hash_search(
        LogicalRepRelMap,
        &mut (*remoterel).remoteid as *mut LogicalRepRelId as *const c_void,
        HASH_ENTER,
        &mut found,
    ) as *mut LogicalRepRelMapEntry;

    if found {
        logicalrep_relmap_free_entry(entry);
    }

    core::ptr::write_bytes(entry, 0, 1);

    /* Make cached copy of the data */
    oldctx = MemoryContextSwitchTo(LogicalRepRelMapContext);
    (*entry).remoterel.remoteid = (*remoterel).remoteid;
    (*entry).remoterel.nspname = pstrdup((*remoterel).nspname);
    (*entry).remoterel.relname = pstrdup((*remoterel).relname);
    (*entry).remoterel.natts = (*remoterel).natts;
    (*entry).remoterel.attnames =
        palloc((*remoterel).natts as usize * core::mem::size_of::<*mut c_char>()) as *mut *mut c_char;
    (*entry).remoterel.atttyps =
        palloc((*remoterel).natts as usize * core::mem::size_of::<Oid>()) as *mut Oid;
    i = 0;
    while i < (*remoterel).natts {
        *(*entry).remoterel.attnames.add(i as usize) =
            pstrdup(*(*remoterel).attnames.add(i as usize));
        *(*entry).remoterel.atttyps.add(i as usize) = *(*remoterel).atttyps.add(i as usize);
        i += 1;
    }
    (*entry).remoterel.replident = (*remoterel).replident;
    (*entry).remoterel.attkeys = bms_copy((*remoterel).attkeys);
    MemoryContextSwitchTo(oldctx);
}

/*
 * Find attribute index in TupleDesc struct by attribute name.
 *
 * Returns -1 if not found.
 */
unsafe fn logicalrep_rel_att_by_name(
    remoterel: *mut LogicalRepRelation,
    attname: *const c_char,
) -> c_int {
    let mut i: c_int = 0;

    while i < (*remoterel).natts {
        if libc_strcmp(*(*remoterel).attnames.add(i as usize), attname) == 0 {
            return i;
        }
        i += 1;
    }

    -1
}

#[inline]
unsafe fn libc_strcmp(a: *const c_char, b: *const c_char) -> c_int {
    extern "C" {
        fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    }
    strcmp(a, b)
}

/*
 * Returns a comma-separated string of attribute names based on the provided
 * relation and bitmap indicating which attributes to include.
 */
unsafe fn logicalrep_get_attrs_str(
    remoterel: *mut LogicalRepRelation,
    atts: *mut Bitmapset,
) -> *mut c_char {
    let mut attsbuf: StringInfoData = core::mem::zeroed();
    let mut attcnt: c_int = 0;
    let mut i: c_int = -1;

    Assert!(!bms_is_empty(atts));

    initStringInfo(&mut attsbuf);

    loop {
        i = bms_next_member(atts, i);
        if i < 0 {
            break;
        }
        attcnt += 1;
        if attcnt > 1 {
            appendStringInfoString(&mut attsbuf, _(c", ".as_ptr()));
        }

        appendStringInfo!(
            &mut attsbuf,
            "\"{}\"",
            CStr::from_ptr(*(*remoterel).attnames.add(i as usize)).to_string_lossy()
        );
    }

    attsbuf.data
}

/*
 * If attempting to replicate missing or generated columns, report an error.
 * Prioritize 'missing' errors if both occur though the prioritization is
 * arbitrary.
 */
unsafe fn logicalrep_report_missing_or_gen_attrs(
    remoterel: *mut LogicalRepRelation,
    missingatts: *mut Bitmapset,
    generatedatts: *mut Bitmapset,
) {
    if !bms_is_empty(missingatts) {
        // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        // C also: errmsg_plural singular: "logical replication target relation \"%s.%s\"
        //   is missing replicated column: %s"
        ereport!(
            ERROR,
            errmsg!(
                "logical replication target relation \"{}.{}\" is missing replicated columns: {}",
                CStr::from_ptr((*remoterel).nspname).to_string_lossy(),
                CStr::from_ptr((*remoterel).relname).to_string_lossy(),
                CStr::from_ptr(logicalrep_get_attrs_str(remoterel, missingatts)).to_string_lossy()
            )
        );
    }

    if !bms_is_empty(generatedatts) {
        // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        // C also: errmsg_plural singular: "logical replication target relation \"%s.%s\"
        //   has incompatible generated column: %s"
        ereport!(
            ERROR,
            errmsg!(
                "logical replication target relation \"{}.{}\" has incompatible generated columns: {}",
                CStr::from_ptr((*remoterel).nspname).to_string_lossy(),
                CStr::from_ptr((*remoterel).relname).to_string_lossy(),
                CStr::from_ptr(logicalrep_get_attrs_str(remoterel, generatedatts)).to_string_lossy()
            )
        );
    }
}

/*
 * Check if replica identity matches and mark the updatable flag.
 *
 * We allow for stricter replica identity (fewer columns) on subscriber as
 * that will not stop us from finding unique tuple. IE, if publisher has
 * identity (id,timestamp) and subscriber just (id) this will not be a
 * problem, but in the opposite scenario it will.
 *
 * We just mark the relation entry as not updatable here if the local
 * replica identity is found to be insufficient for applying
 * updates/deletes (inserts don't care!) and leave it to
 * check_relation_updatable() to throw the actual error if needed.
 */
unsafe fn logicalrep_rel_mark_updatable(entry: *mut LogicalRepRelMapEntry) {
    let mut idkey: *mut Bitmapset;
    let remoterel: *mut LogicalRepRelation = &mut (*entry).remoterel;
    let mut i: c_int;

    (*entry).updatable = true;

    idkey = RelationGetIndexAttrBitmap((*entry).localrel, INDEX_ATTR_BITMAP_IDENTITY_KEY);
    /* fallback to PK if no replica identity */
    if idkey.is_null() {
        idkey = RelationGetIndexAttrBitmap((*entry).localrel, INDEX_ATTR_BITMAP_PRIMARY_KEY);

        /*
         * If no replica identity index and no PK, the published table must
         * have replica identity FULL.
         */
        if idkey.is_null() && (*remoterel).replident != REPLICA_IDENTITY_FULL {
            (*entry).updatable = false;
        }
    }

    i = -1;
    loop {
        i = bms_next_member(idkey, i);
        if i < 0 {
            break;
        }
        let mut attnum: c_int = i + FirstLowInvalidHeapAttributeNumber;

        if !AttrNumberIsForUserDefinedAttr(attnum as AttrNumber) {
            // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            ereport!(
                ERROR,
                errmsg!(
                    "logical replication target relation \"{}.{}\" uses system columns in REPLICA IDENTITY index",
                    CStr::from_ptr((*remoterel).nspname).to_string_lossy(),
                    CStr::from_ptr((*remoterel).relname).to_string_lossy()
                )
            );
        }

        attnum = AttrNumberGetAttrOffset(attnum as AttrNumber) as c_int;

        if *(*(*entry).attrmap).attnums.add(attnum as usize) < 0
            || !bms_is_member(
                *(*(*entry).attrmap).attnums.add(attnum as usize) as c_int,
                (*remoterel).attkeys,
            )
        {
            (*entry).updatable = false;
            break;
        }
    }
}

/*
 * Open the local relation associated with the remote one.
 *
 * Rebuilds the Relcache mapping if it was invalidated by local DDL.
 */
#[no_mangle]
pub unsafe fn logicalrep_rel_open(
    remoteid: LogicalRepRelId,
    lockmode: LOCKMODE,
) -> *mut LogicalRepRelMapEntry {
    let entry: *mut LogicalRepRelMapEntry;
    let mut found: bool = false;
    let remoterel: *mut LogicalRepRelation;

    if LogicalRepRelMap.is_null() {
        logicalrep_relmap_init();
    }

    /* Search for existing entry. */
    entry = hash_search(
        LogicalRepRelMap,
        &remoteid as *const LogicalRepRelId as *const c_void,
        HASH_FIND,
        &mut found,
    ) as *mut LogicalRepRelMapEntry;

    if !found {
        elog!(ERROR, "no relation map entry for remote relation ID {}", remoteid);
    }

    remoterel = &mut (*entry).remoterel;

    /* Ensure we don't leak a relcache refcount. */
    if !(*entry).localrel.is_null() {
        elog!(ERROR, "remote relation ID {} is already open", remoteid);
    }

    /*
     * When opening and locking a relation, pending invalidation messages are
     * processed which can invalidate the relation.  Hence, if the entry is
     * currently considered valid, try to open the local relation by OID and
     * see if invalidation ensues.
     */
    if (*entry).localrelvalid {
        (*entry).localrel = try_table_open((*entry).localreloid, lockmode);
        if (*entry).localrel.is_null() {
            /* Table was renamed or dropped. */
            (*entry).localrelvalid = false;
        } else if !(*entry).localrelvalid {
            /* Note we release the no-longer-useful lock here. */
            table_close((*entry).localrel, lockmode);
            (*entry).localrel = core::ptr::null_mut();
        }
    }

    /*
     * If the entry has been marked invalid since we last had lock on it,
     * re-open the local relation by name and rebuild all derived data.
     */
    if !(*entry).localrelvalid {
        let relid: Oid;
        let desc: TupleDesc;
        let oldctx: MemoryContext;
        let mut i: c_int;
        let mut missingatts: *mut Bitmapset;
        let mut generatedattrs: *mut Bitmapset = core::ptr::null_mut();

        /* Release the no-longer-useful attrmap, if any. */
        if !(*entry).attrmap.is_null() {
            free_attrmap((*entry).attrmap);
            (*entry).attrmap = core::ptr::null_mut();
        }

        /* Try to find and lock the relation by name. */
        relid = RangeVarGetRelid(
            makeRangeVar((*remoterel).nspname, (*remoterel).relname, -1),
            lockmode,
            true,
        );
        if !OidIsValid(relid) {
            // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            ereport!(
                ERROR,
                errmsg!(
                    "logical replication target relation \"{}.{}\" does not exist",
                    CStr::from_ptr((*remoterel).nspname).to_string_lossy(),
                    CStr::from_ptr((*remoterel).relname).to_string_lossy()
                )
            );
        }
        (*entry).localrel = table_open(relid, NoLock);
        (*entry).localreloid = relid;

        /* Check for supported relkind. */
        CheckSubscriptionRelkind(
            (*(*(*entry).localrel).rd_rel).relkind,
            (*remoterel).nspname,
            (*remoterel).relname,
        );

        /*
         * Build the mapping of local attribute numbers to remote attribute
         * numbers and validate that we don't miss any replicated columns as
         * that would result in potentially unwanted data loss.
         */
        desc = RelationGetDescr((*entry).localrel);
        oldctx = MemoryContextSwitchTo(LogicalRepRelMapContext);
        (*entry).attrmap = make_attrmap((*desc).natts);
        MemoryContextSwitchTo(oldctx);

        /* check and report missing attrs, if any */
        missingatts = bms_add_range(core::ptr::null_mut(), 0, (*remoterel).natts - 1);
        i = 0;
        while i < (*desc).natts {
            let attnum: c_int;
            let attr: Form_pg_attribute = TupleDescAttr(desc, i);

            if (*attr).attisdropped {
                *(*(*entry).attrmap).attnums.add(i as usize) = -1;
                i += 1;
                continue;
            }

            attnum = logicalrep_rel_att_by_name(remoterel, NameStr(&(*attr).attname));

            *(*(*entry).attrmap).attnums.add(i as usize) = attnum as AttrNumber;
            if attnum >= 0 {
                /* Remember which subscriber columns are generated. */
                if (*attr).attgenerated != 0 {
                    generatedattrs = bms_add_member(generatedattrs, attnum);
                }

                missingatts = bms_del_member(missingatts, attnum);
            }
            i += 1;
        }

        logicalrep_report_missing_or_gen_attrs(remoterel, missingatts, generatedattrs);

        /* be tidy */
        bms_free(generatedattrs);
        bms_free(missingatts);

        /*
         * Set if the table's replica identity is enough to apply
         * update/delete.
         */
        logicalrep_rel_mark_updatable(entry);

        /*
         * Finding a usable index is an infrequent task. It occurs when an
         * operation is first performed on the relation, or after invalidation
         * of the relation cache entry (such as ANALYZE or CREATE/DROP index
         * on the relation).
         */
        (*entry).localindexoid =
            FindLogicalRepLocalIndex((*entry).localrel, remoterel, (*entry).attrmap);

        (*entry).localrelvalid = true;
    }

    if (*entry).state != SUBREL_STATE_READY {
        (*entry).state = GetSubscriptionRelState(
            (*MySubscription).oid,
            (*entry).localreloid,
            &mut (*entry).statelsn,
        );
    }

    entry
}

/*
 * Close the previously opened logical relation.
 */
#[no_mangle]
pub unsafe fn logicalrep_rel_close(rel: *mut LogicalRepRelMapEntry, lockmode: LOCKMODE) {
    table_close((*rel).localrel, lockmode);
    (*rel).localrel = core::ptr::null_mut();
}

/*
 * Partition cache: look up partition LogicalRepRelMapEntry's
 *
 * Unlike relation map cache, this is keyed by partition OID, not remote
 * relation OID, because we only have to use this cache in the case where
 * partitions are not directly mapped to any remote relation, such as when
 * replication is occurring with one of their ancestors as target.
 */

/*
 * Relcache invalidation callback
 */
unsafe extern "C" fn logicalrep_partmap_invalidate_cb(arg: Datum, reloid: Oid) {
    let _ = arg;
    let mut entry: *mut LogicalRepPartMapEntry;

    /* Just to be sure. */
    if LogicalRepPartMap.is_null() {
        return;
    }

    if reloid != InvalidOid {
        let mut status: HASH_SEQ_STATUS = core::mem::zeroed();

        hash_seq_init(&mut status, LogicalRepPartMap);

        /* TODO, use inverse lookup hashtable? */
        loop {
            entry = hash_seq_search(&mut status) as *mut LogicalRepPartMapEntry;
            if entry.is_null() {
                break;
            }
            if (*entry).relmapentry.localreloid == reloid {
                (*entry).relmapentry.localrelvalid = false;
                hash_seq_term(&mut status);
                break;
            }
        }
    } else {
        /* invalidate all cache entries */
        let mut status: HASH_SEQ_STATUS = core::mem::zeroed();

        hash_seq_init(&mut status, LogicalRepPartMap);

        loop {
            entry = hash_seq_search(&mut status) as *mut LogicalRepPartMapEntry;
            if entry.is_null() {
                break;
            }
            (*entry).relmapentry.localrelvalid = false;
        }
    }
}

/*
 * Reset the entries in the partition map that refer to remoterel.
 *
 * Called when new relation mapping is sent by the publisher to update our
 * expected view of incoming data from said publisher.
 *
 * Note that we don't update the remoterel information in the entry here,
 * we will update the information in logicalrep_partition_open to avoid
 * unnecessary work.
 */
#[no_mangle]
pub unsafe fn logicalrep_partmap_reset_relmap(remoterel: *mut LogicalRepRelation) {
    let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut part_entry: *mut LogicalRepPartMapEntry;
    let mut entry: *mut LogicalRepRelMapEntry;

    if LogicalRepPartMap.is_null() {
        return;
    }

    hash_seq_init(&mut status, LogicalRepPartMap);
    loop {
        part_entry = hash_seq_search(&mut status) as *mut LogicalRepPartMapEntry;
        if part_entry.is_null() {
            break;
        }
        entry = &mut (*part_entry).relmapentry;

        if (*entry).remoterel.remoteid != (*remoterel).remoteid {
            continue;
        }

        logicalrep_relmap_free_entry(entry);

        core::ptr::write_bytes(entry, 0, 1);
    }
}

/*
 * Initialize the partition map cache.
 */
unsafe fn logicalrep_partmap_init() {
    let mut ctl: HASHCTL = core::mem::zeroed();

    if LogicalRepPartMapContext.is_null() {
        LogicalRepPartMapContext = AllocSetContextCreate!(
            CacheMemoryContext,
            c"LogicalRepPartMapContext".as_ptr(),
            ALLOCSET_DEFAULT_SIZES
        );
    }

    /* Initialize the relation hash table. */
    ctl.keysize = core::mem::size_of::<Oid>(); /* partition OID */
    ctl.entrysize = core::mem::size_of::<LogicalRepPartMapEntry>();
    ctl.hcxt = LogicalRepPartMapContext;

    LogicalRepPartMap = hash_create(
        c"logicalrep partition map cache".as_ptr(),
        64,
        &mut ctl,
        HASH_ELEM | HASH_BLOBS | HASH_CONTEXT,
    );

    /* Watch for invalidation events. */
    CacheRegisterRelcacheCallback(logicalrep_partmap_invalidate_cb, 0 as Datum);
}

/*
 * logicalrep_partition_open
 *
 * Returned entry reuses most of the values of the root table's entry, save
 * the attribute map, which can be different for the partition.  However,
 * we must physically copy all the data, in case the root table's entry
 * gets freed/rebuilt.
 *
 * Note there's no logicalrep_partition_close, because the caller closes the
 * component relation.
 */
#[no_mangle]
pub unsafe fn logicalrep_partition_open(
    root: *mut LogicalRepRelMapEntry,
    partrel: Relation,
    map: *mut AttrMap,
) -> *mut LogicalRepRelMapEntry {
    let entry: *mut LogicalRepRelMapEntry;
    let part_entry: *mut LogicalRepPartMapEntry;
    let remoterel: *mut LogicalRepRelation = &mut (*root).remoterel;
    let partOid: Oid = RelationGetRelid(partrel);
    let attrmap: *mut AttrMap = (*root).attrmap;
    let mut found: bool = false;
    let oldctx: MemoryContext;

    if LogicalRepPartMap.is_null() {
        logicalrep_partmap_init();
    }

    /* Search for existing entry. */
    part_entry = hash_search(
        LogicalRepPartMap,
        &partOid as *const Oid as *const c_void,
        HASH_ENTER,
        &mut found,
    ) as *mut LogicalRepPartMapEntry;

    entry = &mut (*part_entry).relmapentry;

    /*
     * We must always overwrite entry->localrel with the latest partition
     * Relation pointer, because the Relation pointed to by the old value may
     * have been cleared after the caller would have closed the partition
     * relation after the last use of this entry.  Note that localrelvalid is
     * only updated by the relcache invalidation callback, so it may still be
     * true irrespective of whether the Relation pointed to by localrel has
     * been cleared or not.
     */
    if found && (*entry).localrelvalid {
        (*entry).localrel = partrel;
        return entry;
    }

    /* Switch to longer-lived context. */
    oldctx = MemoryContextSwitchTo(LogicalRepPartMapContext);

    if !found {
        core::ptr::write_bytes(part_entry, 0, 1);
        (*part_entry).partoid = partOid;
    }

    /* Release the no-longer-useful attrmap, if any. */
    if !(*entry).attrmap.is_null() {
        free_attrmap((*entry).attrmap);
        (*entry).attrmap = core::ptr::null_mut();
    }

    if (*entry).remoterel.remoteid == 0 {
        let mut i: c_int;

        /* Remote relation is copied as-is from the root entry. */
        (*entry).remoterel.remoteid = (*remoterel).remoteid;
        (*entry).remoterel.nspname = pstrdup((*remoterel).nspname);
        (*entry).remoterel.relname = pstrdup((*remoterel).relname);
        (*entry).remoterel.natts = (*remoterel).natts;
        (*entry).remoterel.attnames =
            palloc((*remoterel).natts as usize * core::mem::size_of::<*mut c_char>()) as *mut *mut c_char;
        (*entry).remoterel.atttyps =
            palloc((*remoterel).natts as usize * core::mem::size_of::<Oid>()) as *mut Oid;
        i = 0;
        while i < (*remoterel).natts {
            *(*entry).remoterel.attnames.add(i as usize) =
                pstrdup(*(*remoterel).attnames.add(i as usize));
            *(*entry).remoterel.atttyps.add(i as usize) = *(*remoterel).atttyps.add(i as usize);
            i += 1;
        }
        (*entry).remoterel.replident = (*remoterel).replident;
        (*entry).remoterel.attkeys = bms_copy((*remoterel).attkeys);
    }

    (*entry).localrel = partrel;
    (*entry).localreloid = partOid;

    /*
     * If the partition's attributes don't match the root relation's, we'll
     * need to make a new attrmap which maps partition attribute numbers to
     * remoterel's, instead of the original which maps root relation's
     * attribute numbers to remoterel's.
     *
     * Note that 'map' which comes from the tuple routing data structure
     * contains 1-based attribute numbers (of the parent relation).  However,
     * the map in 'entry', a logical replication data structure, contains
     * 0-based attribute numbers (of the remote relation).
     */
    if !map.is_null() {
        let mut attno: AttrNumber;

        (*entry).attrmap = make_attrmap((*map).maplen);
        attno = 0;
        while attno < (*(*entry).attrmap).maplen as AttrNumber {
            let root_attno: AttrNumber = *(*map).attnums.add(attno as usize);

            /* 0 means it's a dropped attribute.  See comments atop AttrMap. */
            if root_attno == 0 {
                *(*(*entry).attrmap).attnums.add(attno as usize) = -1;
            } else {
                *(*(*entry).attrmap).attnums.add(attno as usize) =
                    *(*attrmap).attnums.add((root_attno - 1) as usize);
            }
            attno += 1;
        }
    } else {
        /* Lacking copy_attmap, do this the hard way. */
        (*entry).attrmap = make_attrmap((*attrmap).maplen);
        core::ptr::copy_nonoverlapping(
            (*attrmap).attnums,
            (*(*entry).attrmap).attnums,
            (*attrmap).maplen as usize,
        );
    }

    /* Set if the table's replica identity is enough to apply update/delete. */
    logicalrep_rel_mark_updatable(entry);

    /* state and statelsn are left set to 0. */
    MemoryContextSwitchTo(oldctx);

    /*
     * Finding a usable index is an infrequent task. It occurs when an
     * operation is first performed on the relation, or after invalidation of
     * the relation cache entry (such as ANALYZE or CREATE/DROP index on the
     * relation).
     *
     * We also prefer to run this code on the oldctx so that we do not leak
     * anything in the LogicalRepPartMapContext (hence CacheMemoryContext).
     */
    (*entry).localindexoid = FindLogicalRepLocalIndex(partrel, remoterel, (*entry).attrmap);

    (*entry).localrelvalid = true;

    entry
}

/*
 * Returns the oid of an index that can be used by the apply worker to scan
 * the relation.
 *
 * We expect to call this function when REPLICA IDENTITY FULL is defined for
 * the remote relation.
 *
 * If no suitable index is found, returns InvalidOid.
 */
unsafe fn FindUsableIndexForReplicaIdentityFull(
    localrel: Relation,
    attrmap: *mut AttrMap,
) -> Oid {
    let idxlist: *mut crate::nodes::pg_list::List = RelationGetIndexList(localrel);

    crate::foreach_oid!(idxoid, idxlist, {
        let isUsableIdx: bool;
        let idxRel: Relation;

        idxRel = index_open(idxoid, AccessShareLock);
        isUsableIdx = IsIndexUsableForReplicaIdentityFull(idxRel, attrmap);
        index_close(idxRel, AccessShareLock);

        /* Return the first eligible index found */
        if isUsableIdx {
            return idxoid;
        }
    });

    InvalidOid
}

/*
 * Returns true if the index is usable for replica identity full.
 *
 * The index must have an equal strategy for each key column, be non-partial,
 * and the leftmost field must be a column (not an expression) that references
 * the remote relation column. These limitations help to keep the index scan
 * similar to PK/RI index scans.
 *
 * attrmap is a map of local attributes to remote ones. We can consult this
 * map to check whether the local index attribute has a corresponding remote
 * attribute.
 *
 * Note that the limitations of index scans for replica identity full only
 * adheres to a subset of the limitations of PK/RI. For example, we support
 * columns that are marked as [NULL] or we are not interested in the [NOT
 * DEFERRABLE] aspect of constraints here. It works for us because we always
 * compare the tuples for non-PK/RI index scans. See
 * RelationFindReplTupleByIndex().
 *
 * XXX: To support partial indexes, the required changes are likely to be larger.
 * If none of the tuples satisfy the expression for the index scan, we fall-back
 * to sequential execution, which might not be a good idea in some cases.
 */
pub unsafe fn IsIndexUsableForReplicaIdentityFull(
    idxrel: Relation,
    attrmap: *mut AttrMap,
) -> bool {
    let keycol: AttrNumber;
    let indclass: *mut oidvector;

    /* The index must not be a partial index */
    if !heap_attisnull(
        (*idxrel).rd_indextuple as HeapTuple,
        Anum_pg_index_indpred as c_int,
        core::ptr::null_mut(),
    ) {
        return false;
    }

    Assert!((*(*idxrel).rd_index).indnatts >= 1);

    indclass = DatumGetPointer(SysCacheGetAttrNotNull(
        INDEXRELID,
        (*idxrel).rd_indextuple as HeapTuple,
        Anum_pg_index_indclass,
    )) as *mut oidvector;

    /* Ensure that the index has a valid equal strategy for each key column */
    {
        let mut i: c_int = 0;
        while i < (*(*idxrel).rd_index).indnkeyatts as c_int {
            let opfamily: Oid;

            opfamily = get_opclass_family(*(*indclass).values.as_ptr().add(i as usize));
            if IndexAmTranslateCompareType(
                COMPARE_EQ,
                (*(*idxrel).rd_rel).relam,
                opfamily,
                true,
            ) == InvalidStrategy
            {
                return false;
            }
            i += 1;
        }
    }

    /*
     * For indexes other than PK and REPLICA IDENTITY, we need to match the
     * local and remote tuples.  The equality routine tuples_equal() cannot
     * accept a data type where the type cache cannot provide an equality
     * operator.
     */
    {
        let mut i: c_int = 0;
        while i < (*(*idxrel).rd_att).natts {
            let typentry: *mut TypeCacheEntry;

            typentry = lookup_type_cache(
                (*TupleDescAttr((*idxrel).rd_att, i)).atttypid,
                TYPECACHE_EQ_OPR_FINFO,
            );
            if !OidIsValid((*typentry).eq_opr_finfo.fn_oid) {
                return false;
            }
            i += 1;
        }
    }

    /* The leftmost index field must not be an expression */
    keycol = *indkey_values((*idxrel).rd_index).add(0);
    if !AttributeNumberIsValid(keycol) {
        return false;
    }

    /*
     * And the leftmost index field must reference the remote relation column.
     * This is because if it doesn't, the sequential scan is favorable over
     * index scan in most cases.
     */
    if (*attrmap).maplen <= AttrNumberGetAttrOffset(keycol) as c_int
        || *(*attrmap).attnums.add(AttrNumberGetAttrOffset(keycol) as usize) < 0
    {
        return false;
    }

    /*
     * The given index access method must implement "amgettuple", which will
     * be used later to fetch the tuples.  See RelationFindReplTupleByIndex().
     */
    if (*GetIndexAmRoutineByAmId((*(*idxrel).rd_rel).relam, false))
        .amgettuple
        .is_none()
    {
        return false;
    }

    true
}

/*
 * indkey.values accessor: the int2vector 'indkey' begins immediately after the
 * fixed part of FormData_pg_index (which ends at indisreplident).
 */
#[inline]
unsafe fn indkey_values(rd_index: *mut FormData_pg_index) -> *const AttrNumber {
    let indkey =
        (rd_index as *const u8).add(core::mem::size_of::<FormData_pg_index>()) as *const int2vector;
    (*indkey).values.as_ptr()
}

/*
 * Return the OID of the replica identity index if one is defined;
 * the OID of the PK if one exists and is not deferrable;
 * otherwise, InvalidOid.
 */
#[no_mangle]
pub unsafe fn GetRelationIdentityOrPK(rel: Relation) -> Oid {
    let mut idxoid: Oid;

    idxoid = RelationGetReplicaIndex(rel);

    if !OidIsValid(idxoid) {
        idxoid = RelationGetPrimaryKeyIndex(rel, false);
    }

    idxoid
}

/*
 * Returns the index oid if we can use an index for subscriber. Otherwise,
 * returns InvalidOid.
 */
unsafe fn FindLogicalRepLocalIndex(
    localrel: Relation,
    remoterel: *mut LogicalRepRelation,
    attrMap: *mut AttrMap,
) -> Oid {
    let idxoid: Oid;

    /*
     * We never need index oid for partitioned tables, always rely on leaf
     * partition's index.
     */
    if (*(*localrel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE {
        return InvalidOid;
    }

    /*
     * Simple case, we already have a primary key or a replica identity index.
     */
    idxoid = GetRelationIdentityOrPK(localrel);
    if OidIsValid(idxoid) {
        return idxoid;
    }

    if (*remoterel).replident == REPLICA_IDENTITY_FULL {
        /*
         * We are looking for one more opportunity for using an index. If
         * there are any indexes defined on the local relation, try to pick a
         * suitable index.
         *
         * The index selection safely assumes that all the columns are going
         * to be available for the index scan given that remote relation has
         * replica identity full.
         *
         * Note that we are not using the planner to find the cheapest method
         * to scan the relation as that would require us to either use lower
         * level planner functions which would be a maintenance burden in the
         * long run or use the full-fledged planner which could cause
         * overhead.
         */
        return FindUsableIndexForReplicaIdentityFull(localrel, attrMap);
    }

    InvalidOid
}
