//! evtcache.c - Special-purpose cache for event trigger data.

use crate::prelude::*;

use crate::access::htup_details::{HeapTuple, HeapTupleData, HeapTupleIsValid, GETSTRUCT};
use crate::access::sdir::ForwardScanDirection;
use crate::catalog::pg_event_trigger::Form_pg_event_trigger;
use crate::catalog::pg_type_d::TEXTOID;
use crate::c::NameStr;
use crate::nodes::bitmapset::{bms_add_member, Bitmapset};
use crate::nodes::pg_list::{lappend, List, NIL};
use crate::storage::lockdefs::{AccessShareLock, LOCKMODE};
use crate::tcop::cmdtag::GetCommandTagEnum;
use crate::utils::array::{ArrayType, ARR_ELEMTYPE, ARR_HASNULL, ARR_NDIM};
use crate::utils::builtins::TextDatumGetCString;
use crate::utils::hash::dynahash::{
    hash_create, hash_search, HASHACTION, HASHCTL, HASH_BLOBS, HASH_CONTEXT, HASH_ELEM, HTAB,
};
use crate::utils::mmgr::mcxt::CacheMemoryContext;
use crate::utils::rel::{Relation, RelationGetDescr};

use crate::access::common::relation::{relation_close, relation_open};
use crate::access::htup_details::heap_getattr;

use crate::list_make1;

use core::ffi::{c_int, c_void};

/* ==================================================================== */
/*  Stubs for not-yet-translated subsystems                            */
/* ==================================================================== */

// utils/evtcache.h - EventTriggerEvent enum.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub enum EventTriggerEvent {
    EVT_DDLCommandStart,
    EVT_DDLCommandEnd,
    EVT_SQLDrop,
    EVT_TableRewrite,
    EVT_Login,
}
use EventTriggerEvent::*;

// utils/evtcache.h - EventTriggerCacheItem.
#[repr(C)]
pub struct EventTriggerCacheItem {
    /// function to be called
    pub fnoid: Oid,
    /// as SESSION_REPLICATION_ROLE_*
    pub enabled: c_char,
    /// command tags, or NULL if empty
    pub tagset: *mut Bitmapset,
}

// commands/trigger.h - TRIGGER_DISABLED.
const TRIGGER_DISABLED: c_char = b'D' as c_char;

// catalog/pg_event_trigger.h - relation / index OIDs (STUB: catalog OIDs not
// yet generated).
// TODO: pull these from generated catalog OID tables.
const EventTriggerRelationId: Oid = 3466;
const EventTriggerNameIndexId: Oid = 3467;

// catalog/pg_event_trigger.h - evttags attribute number (STUB: catalog _d
// constants not yet generated). evttags is the 7th column.
// TODO: pull from generated pg_event_trigger_d.h.
const Anum_pg_event_trigger_evttags: c_int = 7;

// utils/syscache.h - EVENTTRIGGEROID cache id (STUB: syscache not yet ported).
const EVENTTRIGGEROID: c_int = 26;

// access/relscan.h - SysScanDesc opaque handle (STUB: genam not yet ported).
type SysScanDesc = *mut c_void;

// access/skey.h - ScanKey (STUB: genam not yet ported).
type ScanKey = *mut c_void;

// access/relation.h - index_open (STUB: not yet ported).
// TODO: port access/index/indexam.c index_open.
unsafe fn index_open(_relationId: Oid, _lockmode: LOCKMODE) -> Relation {
    unimplemented!()
}

// access/relation.h - index_close (STUB: not yet ported).
// TODO: port access/index/indexam.c index_close.
unsafe fn index_close(_relation: Relation, _lockmode: LOCKMODE) {
    unimplemented!()
}

// access/genam.h - systable_beginscan_ordered (STUB: genam.c not yet ported).
// TODO: port access/index/genam.c.
unsafe fn systable_beginscan_ordered(
    _heapRelation: Relation,
    _indexRelation: Relation,
    _snapshot: *mut c_void,
    _nkeys: c_int,
    _key: ScanKey,
) -> SysScanDesc {
    unimplemented!()
}

// access/genam.h - systable_getnext_ordered (STUB: genam.c not yet ported).
// TODO: port access/index/genam.c.
unsafe fn systable_getnext_ordered(_sysscan: SysScanDesc, _direction: c_int) -> HeapTuple {
    unimplemented!()
}

// access/genam.h - systable_endscan_ordered (STUB: genam.c not yet ported).
// TODO: port access/index/genam.c.
unsafe fn systable_endscan_ordered(_sysscan: SysScanDesc) {
    unimplemented!()
}

// utils/inval.h - SyscacheCallbackFunction signature.
type SyscacheCallbackFunction = unsafe fn(arg: Datum, cacheid: c_int, hashvalue: uint32);

// utils/inval.h - CacheRegisterSyscacheCallback (STUB: inval.c not yet ported).
// TODO: port inval.c.
unsafe fn CacheRegisterSyscacheCallback(
    _cacheid: c_int,
    _func: SyscacheCallbackFunction,
    _arg: Datum,
) {
    unimplemented!()
}

// utils/memutils.h - CreateCacheMemoryContext (STUB: not yet ported).
// TODO: port CreateCacheMemoryContext.
unsafe fn CreateCacheMemoryContext() {
    unimplemented!()
}

// utils/array.h - DatumGetArrayTypeP (STUB: detoasting path not yet ported).
// TODO: port utils/adt/array.h DatumGetArrayTypeP.
unsafe fn DatumGetArrayTypeP(_d: Datum) -> *mut ArrayType {
    unimplemented!()
}

// utils/array.h - deconstruct_array_builtin (STUB: array.c not yet ported).
// TODO: port utils/adt/arrayfuncs.c deconstruct_array_builtin.
unsafe fn deconstruct_array_builtin(
    _array: *mut ArrayType,
    _elmtype: Oid,
    _elemsp: *mut *mut Datum,
    _nullsp: *mut *mut bool,
    _nelemsp: *mut c_int,
) {
    unimplemented!()
}

/* ==================================================================== */

#[repr(C)]
struct EventTriggerCacheEntry {
    event: EventTriggerEvent,
    triggerlist: *mut List,
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(C)]
enum EventTriggerCacheStateType {
    ETCS_NEEDS_REBUILD,
    ETCS_REBUILD_STARTED,
    ETCS_VALID,
}
use EventTriggerCacheStateType::*;

static mut EventTriggerCache: *mut HTAB = core::ptr::null_mut();
static mut EventTriggerCacheContext: MemoryContext = core::ptr::null_mut();
static mut EventTriggerCacheState: EventTriggerCacheStateType = ETCS_NEEDS_REBUILD;

/*
 * Search the event cache by trigger event.
 *
 * Note that the caller had better copy any data it wants to keep around
 * across any operation that might touch a system catalog into some other
 * memory context, since a cache reset could blow the return value away.
 */
pub unsafe fn EventCacheLookup(event: EventTriggerEvent) -> *mut List {
    let entry: *mut EventTriggerCacheEntry;

    if EventTriggerCacheState != ETCS_VALID {
        BuildEventTriggerCache();
    }
    entry = hash_search(
        EventTriggerCache,
        &event as *const EventTriggerEvent as *const c_void,
        HASHACTION::HASH_FIND,
        core::ptr::null_mut(),
    ) as *mut EventTriggerCacheEntry;
    if !entry.is_null() {
        (*entry).triggerlist
    } else {
        NIL
    }
}

/*
 * Rebuild the event trigger cache.
 */
unsafe fn BuildEventTriggerCache() {
    let mut ctl: HASHCTL = core::mem::zeroed();
    let cache: *mut HTAB;
    let oldcontext: MemoryContext;
    let rel: Relation;
    let irel: Relation;
    let scan: SysScanDesc;

    if !EventTriggerCacheContext.is_null() {
        /*
         * Free up any memory already allocated in EventTriggerCacheContext.
         * This can happen either because a previous rebuild failed, or because
         * an invalidation happened before the rebuild was complete.
         */
        MemoryContextReset(EventTriggerCacheContext);
    } else {
        /*
         * This is our first time attempting to build the cache, so we need to
         * set up the memory context and register a syscache callback to
         * capture future invalidation events.
         */
        if CacheMemoryContext.is_null() {
            CreateCacheMemoryContext();
        }
        EventTriggerCacheContext = AllocSetContextCreate!(
            CacheMemoryContext,
            c"EventTriggerCache".as_ptr(),
            ALLOCSET_DEFAULT_SIZES
        ) as *mut _;
        CacheRegisterSyscacheCallback(EVENTTRIGGEROID, InvalidateEventCacheCallback, 0 as Datum);
    }

    /* Switch to correct memory context. */
    oldcontext = MemoryContextSwitchTo(EventTriggerCacheContext);

    /* Prevent the memory context from being nuked while we're rebuilding. */
    EventTriggerCacheState = ETCS_REBUILD_STARTED;

    /* Create new hash table. */
    ctl.keysize = core::mem::size_of::<EventTriggerEvent>();
    ctl.entrysize = core::mem::size_of::<EventTriggerCacheEntry>();
    ctl.hcxt = EventTriggerCacheContext;
    cache = hash_create(
        c"EventTriggerCacheHash".as_ptr(),
        32,
        &ctl,
        HASH_ELEM | HASH_BLOBS | HASH_CONTEXT,
    );

    /*
     * Prepare to scan pg_event_trigger in name order.
     */
    rel = relation_open(EventTriggerRelationId, AccessShareLock);
    irel = index_open(EventTriggerNameIndexId, AccessShareLock);
    scan = systable_beginscan_ordered(rel, irel, core::ptr::null_mut(), 0, core::ptr::null_mut());

    /*
     * Build a cache item for each pg_event_trigger tuple, and append each one
     * to the appropriate cache entry.
     */
    loop {
        let tup: HeapTuple;
        let form: Form_pg_event_trigger;
        let evtevent: *mut c_char;
        let event: EventTriggerEvent;
        let item: *mut EventTriggerCacheItem;
        let evttags: Datum;
        let mut evttags_isnull: bool = false;
        let entry: *mut EventTriggerCacheEntry;
        let mut found: bool = false;

        /* Get next tuple. */
        tup = systable_getnext_ordered(scan, ForwardScanDirection);
        if !HeapTupleIsValid(tup) {
            break;
        }

        /* Skip trigger if disabled. */
        form = GETSTRUCT(tup as *const HeapTupleData) as Form_pg_event_trigger;
        if (*form).evtenabled == TRIGGER_DISABLED {
            continue;
        }

        /* Decode event name. */
        evtevent = NameStr(&(*form).evtevent) as *mut c_char;
        if libc_strcmp(evtevent, c"ddl_command_start".as_ptr()) == 0 {
            event = EVT_DDLCommandStart;
        } else if libc_strcmp(evtevent, c"ddl_command_end".as_ptr()) == 0 {
            event = EVT_DDLCommandEnd;
        } else if libc_strcmp(evtevent, c"sql_drop".as_ptr()) == 0 {
            event = EVT_SQLDrop;
        } else if libc_strcmp(evtevent, c"table_rewrite".as_ptr()) == 0 {
            event = EVT_TableRewrite;
        } else if libc_strcmp(evtevent, c"login".as_ptr()) == 0 {
            event = EVT_Login;
        } else {
            continue;
        }

        /* Allocate new cache item. */
        item = palloc0(core::mem::size_of::<EventTriggerCacheItem>()) as *mut EventTriggerCacheItem;
        (*item).fnoid = (*form).evtfoid;
        (*item).enabled = (*form).evtenabled;

        /* Decode and sort tags array. */
        evttags = heap_getattr(
            tup,
            Anum_pg_event_trigger_evttags,
            RelationGetDescr(rel),
            &mut evttags_isnull,
        );
        if !evttags_isnull {
            (*item).tagset = DecodeTextArrayToBitmapset(evttags);
        }

        /* Add to cache entry. */
        entry = hash_search(
            cache,
            &event as *const EventTriggerEvent as *const c_void,
            HASHACTION::HASH_ENTER,
            &mut found,
        ) as *mut EventTriggerCacheEntry;
        if found {
            (*entry).triggerlist = lappend((*entry).triggerlist, item as *mut c_void);
        } else {
            (*entry).triggerlist = list_make1!(item);
        }
    }

    /* Done with pg_event_trigger scan. */
    systable_endscan_ordered(scan);
    index_close(irel, AccessShareLock);
    relation_close(rel, AccessShareLock);

    /* Restore previous memory context. */
    MemoryContextSwitchTo(oldcontext);

    /* Install new cache. */
    EventTriggerCache = cache;

    /*
     * If the cache has been invalidated since we entered this routine, we still
     * use and return the cache we just finished constructing, to avoid infinite
     * loops, but we leave the cache marked stale so that we'll rebuild it again
     * on next access.  Otherwise, we mark the cache valid.
     */
    if EventTriggerCacheState == ETCS_REBUILD_STARTED {
        EventTriggerCacheState = ETCS_VALID;
    }
}

/*
 * Decode text[] to a Bitmapset of CommandTags.
 *
 * We could avoid a bit of overhead here if we were willing to duplicate some of
 * the logic from deconstruct_array, but it doesn't seem worth the code
 * complexity.
 */
unsafe fn DecodeTextArrayToBitmapset(array: Datum) -> *mut Bitmapset {
    let arr: *mut ArrayType = DatumGetArrayTypeP(array);
    let mut elems: *mut Datum = core::ptr::null_mut();
    let mut bms: *mut Bitmapset;
    let mut i: c_int;
    let mut nelems: c_int = 0;

    if ARR_NDIM(arr) != 1 || ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != TEXTOID {
        elog!(ERROR, "expected 1-D text array");
    }
    deconstruct_array_builtin(
        arr,
        TEXTOID,
        &mut elems,
        core::ptr::null_mut(),
        &mut nelems,
    );

    bms = core::ptr::null_mut();
    i = 0;
    while i < nelems {
        let str: *mut c_char = TextDatumGetCString(*elems.add(i as usize));

        bms = bms_add_member(bms, GetCommandTagEnum(str) as c_int);
        pfree(str as *mut c_void);
        i += 1;
    }

    pfree(elems as *mut c_void);

    bms
}

/*
 * Flush all cache entries when pg_event_trigger is updated.
 *
 * This should be rare enough that we don't need to be very granular about it,
 * so we just blow away everything, which also avoids the possibility of memory
 * leaks.
 */
unsafe fn InvalidateEventCacheCallback(_arg: Datum, _cacheid: c_int, _hashvalue: uint32) {
    /*
     * If the cache isn't valid, then there might be a rebuild in progress, so
     * we can't immediately blow it away.  But it's advantageous to do this when
     * possible, so as to immediately free memory.
     */
    if EventTriggerCacheState == ETCS_VALID {
        MemoryContextReset(EventTriggerCacheContext);
        EventTriggerCache = core::ptr::null_mut();
    }

    /* Mark cache for rebuild. */
    EventTriggerCacheState = ETCS_NEEDS_REBUILD;
}

// libc strcmp on NUL-terminated C strings (the C source uses strcmp directly).
unsafe fn libc_strcmp(a: *const c_char, b: *const c_char) -> c_int {
    let mut pa = a;
    let mut pb = b;
    loop {
        let ca = *pa as u8;
        let cb = *pb as u8;
        if ca != cb {
            return (ca as c_int) - (cb as c_int);
        }
        if ca == 0 {
            return 0;
        }
        pa = pa.add(1);
        pb = pb.add(1);
    }
}
