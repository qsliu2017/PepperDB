//! spccache.c - Tablespace cache management.
//!
//! We cache the parsed version of spcoptions for each tablespace to avoid
//! needing to reparse on every lookup.

use crate::prelude::*;

use crate::miscadmin::MyDatabaseTableSpace;
use crate::optimizer::optimizer::{random_page_cost, seq_page_cost};
use crate::postgres::ObjectIdGetDatum;
use crate::postgres_ext::{InvalidOid, Oid};
use crate::utils::hash::dynahash::{
    hash_create, hash_search, hash_seq_init, hash_seq_search, HASHACTION, HASHCTL,
    HASH_BLOBS, HASH_ELEM, HASH_SEQ_STATUS, HTAB,
};
use crate::utils::mmgr::mcxt::{pfree, CacheMemoryContext, MemoryContextAlloc};
use crate::varatt::VARSIZE;

use std::ffi::{c_int, c_void};

/* ==================================================================== */
/*  Stubs for not-yet-translated subsystems                            */
/* ==================================================================== */

// access/htup_details.h - HeapTuple
use crate::access::htup_details::{HeapTuple, HeapTupleIsValid};

// c.h - bytea
use crate::c::bytea;

// catalog/pg_tablespace - attribute number (STUB: pg_tablespace catalog not yet ported)
const Anum_pg_tablespace_spcoptions: c_int = 4;

// utils/syscache.h - TABLESPACEOID cache id (STUB: syscache not yet ported)
const TABLESPACEOID: c_int = 0;

/*
 * access/reloptions.h - TableSpaceOpts
 *
 * STUB: reloptions.c not yet ported.  Layout mirrors the C struct so that
 * tablespace_reloptions can produce it.
 */
#[repr(C)]
pub struct TableSpaceOpts {
    /// varlena header (do not touch directly)
    pub vl_len_: int32,
    pub random_page_cost: f64,
    pub seq_page_cost: f64,
    pub effective_io_concurrency: c_int,
    pub maintenance_io_concurrency: c_int,
}

// storage/bufmgr.h - effective_io_concurrency (STUB: bufmgr GUC not yet ported)
static mut effective_io_concurrency: c_int = 0;

// commands/async.c / globals - maintenance_io_concurrency (STUB: not yet ported)
static mut maintenance_io_concurrency: c_int = 0;

// access/reloptions.h - tablespace_reloptions (STUB: reloptions.c not yet ported)
// TODO: port reloptions.c
unsafe fn tablespace_reloptions(_reloptions: Datum, _validate: bool) -> *mut bytea {
    unimplemented!()
}

// utils/syscache.h - SearchSysCache1 (STUB: syscache not yet ported)
// TODO: port syscache.c
unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    unimplemented!()
}

// utils/syscache.h - SysCacheGetAttr (STUB: syscache not yet ported)
// TODO: port syscache.c
unsafe fn SysCacheGetAttr(
    _cacheId: c_int,
    _tup: HeapTuple,
    _attributeNumber: c_int,
    _isNull: *mut bool,
) -> Datum {
    unimplemented!()
}

// utils/syscache.h - ReleaseSysCache (STUB: syscache not yet ported)
// TODO: port syscache.c
unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    unimplemented!()
}

// utils/inval.h - SyscacheCallbackFunction signature
type SyscacheCallbackFunction = unsafe fn(arg: Datum, cacheid: c_int, hashvalue: uint32);

// utils/inval.h - CacheRegisterSyscacheCallback (STUB: inval.c not yet ported)
// TODO: port inval.c
unsafe fn CacheRegisterSyscacheCallback(
    _cacheid: c_int,
    _func: SyscacheCallbackFunction,
    _arg: Datum,
) {
    unimplemented!()
}

// utils/memutils.h - CreateCacheMemoryContext (STUB: not yet ported)
// TODO: port CreateCacheMemoryContext
unsafe fn CreateCacheMemoryContext() {
    unimplemented!()
}

/* ==================================================================== */

/* Hash table for information about each tablespace */
static mut TableSpaceCacheHash: *mut HTAB = core::ptr::null_mut();

#[repr(C)]
struct TableSpaceCacheEntry {
    /// lookup key - must be first
    oid: Oid,
    /// options, or NULL if none
    opts: *mut TableSpaceOpts,
}

/*
 * InvalidateTableSpaceCacheCallback
 *		Flush all cache entries when pg_tablespace is updated.
 */
unsafe fn InvalidateTableSpaceCacheCallback(_arg: Datum, _cacheid: c_int, _hashvalue: uint32) {
    let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut spc: *mut TableSpaceCacheEntry;

    hash_seq_init(&mut status, TableSpaceCacheHash);
    loop {
        spc = hash_seq_search(&mut status) as *mut TableSpaceCacheEntry;
        if spc.is_null() {
            break;
        }
        if !(*spc).opts.is_null() {
            pfree((*spc).opts as *mut c_void);
        }
        if hash_search(
            TableSpaceCacheHash,
            &(*spc).oid as *const Oid as *const c_void,
            HASHACTION::HASH_REMOVE,
            core::ptr::null_mut(),
        )
        .is_null()
        {
            elog!(ERROR, "hash table corrupted");
        }
    }
}

/*
 * InitializeTableSpaceCache
 *		Initialize the tablespace cache.
 */
unsafe fn InitializeTableSpaceCache() {
    let mut ctl: HASHCTL = core::mem::zeroed();

    /* Initialize the hash table. */
    ctl.keysize = core::mem::size_of::<Oid>();
    ctl.entrysize = core::mem::size_of::<TableSpaceCacheEntry>();
    TableSpaceCacheHash = hash_create(
        c"TableSpace cache".as_ptr(),
        16,
        &ctl,
        HASH_ELEM | HASH_BLOBS,
    );

    /* Make sure we've initialized CacheMemoryContext. */
    if CacheMemoryContext.is_null() {
        CreateCacheMemoryContext();
    }

    /* Watch for invalidation events. */
    CacheRegisterSyscacheCallback(
        TABLESPACEOID,
        InvalidateTableSpaceCacheCallback,
        0 as Datum,
    );
}

/*
 * get_tablespace
 *		Fetch TableSpaceCacheEntry structure for a specified table OID.
 *
 * Pointers returned by this function should not be stored, since a cache
 * flush will invalidate them.
 */
unsafe fn get_tablespace(spcid: Oid) -> *mut TableSpaceCacheEntry {
    let mut spc: *mut TableSpaceCacheEntry;
    let tp: HeapTuple;
    let opts: *mut TableSpaceOpts;

    let mut spcid = spcid;

    /*
     * Since spcid is always from a pg_class tuple, InvalidOid implies the
     * default.
     */
    if spcid == InvalidOid {
        spcid = MyDatabaseTableSpace;
    }

    /* Find existing cache entry, if any. */
    if TableSpaceCacheHash.is_null() {
        InitializeTableSpaceCache();
    }
    spc = hash_search(
        TableSpaceCacheHash,
        &spcid as *const Oid as *const c_void,
        HASHACTION::HASH_FIND,
        core::ptr::null_mut(),
    ) as *mut TableSpaceCacheEntry;
    if !spc.is_null() {
        return spc;
    }

    /*
     * Not found in TableSpace cache.  Check catcache.  If we don't find a
     * valid HeapTuple, it must mean someone has managed to request tablespace
     * details for a non-existent tablespace.  We'll just treat that case as
     * if no options were specified.
     */
    tp = SearchSysCache1(TABLESPACEOID, ObjectIdGetDatum(spcid));
    if !HeapTupleIsValid(tp) {
        opts = core::ptr::null_mut();
    } else {
        let datum: Datum;
        let mut isNull: bool = false;

        datum = SysCacheGetAttr(
            TABLESPACEOID,
            tp,
            Anum_pg_tablespace_spcoptions,
            &mut isNull,
        );
        if isNull {
            opts = core::ptr::null_mut();
        } else {
            let bytea_opts: *mut bytea = tablespace_reloptions(datum, false);

            opts = MemoryContextAlloc(
                CacheMemoryContext,
                VARSIZE(bytea_opts as *const c_char) as Size,
            ) as *mut TableSpaceOpts;
            core::ptr::copy_nonoverlapping(
                bytea_opts as *const u8,
                opts as *mut u8,
                VARSIZE(bytea_opts as *const c_char) as usize,
            );
        }
        ReleaseSysCache(tp);
    }

    /*
     * Now create the cache entry.  It's important to do this only after
     * reading the pg_tablespace entry, since doing so could cause a cache
     * flush.
     */
    spc = hash_search(
        TableSpaceCacheHash,
        &spcid as *const Oid as *const c_void,
        HASHACTION::HASH_ENTER,
        core::ptr::null_mut(),
    ) as *mut TableSpaceCacheEntry;
    (*spc).opts = opts;
    spc
}

/*
 * get_tablespace_page_costs
 *		Return random and/or sequential page costs for a given tablespace.
 */
pub unsafe fn get_tablespace_page_costs(
    spcid: Oid,
    spc_random_page_cost: *mut f64,
    spc_seq_page_cost: *mut f64,
) {
    let spc: *mut TableSpaceCacheEntry = get_tablespace(spcid);

    Assert!(!spc.is_null());

    if !spc_random_page_cost.is_null() {
        if (*spc).opts.is_null() || (*(*spc).opts).random_page_cost < 0.0 {
            *spc_random_page_cost = random_page_cost;
        } else {
            *spc_random_page_cost = (*(*spc).opts).random_page_cost;
        }
    }

    if !spc_seq_page_cost.is_null() {
        if (*spc).opts.is_null() || (*(*spc).opts).seq_page_cost < 0.0 {
            *spc_seq_page_cost = seq_page_cost;
        } else {
            *spc_seq_page_cost = (*(*spc).opts).seq_page_cost;
        }
    }
}

/*
 * get_tablespace_io_concurrency
 */
pub unsafe fn get_tablespace_io_concurrency(spcid: Oid) -> c_int {
    let spc: *mut TableSpaceCacheEntry = get_tablespace(spcid);

    if (*spc).opts.is_null() || (*(*spc).opts).effective_io_concurrency < 0 {
        effective_io_concurrency
    } else {
        (*(*spc).opts).effective_io_concurrency
    }
}

/*
 * get_tablespace_maintenance_io_concurrency
 */
pub unsafe fn get_tablespace_maintenance_io_concurrency(spcid: Oid) -> c_int {
    let spc: *mut TableSpaceCacheEntry = get_tablespace(spcid);

    if (*spc).opts.is_null() || (*(*spc).opts).maintenance_io_concurrency < 0 {
        maintenance_io_concurrency
    } else {
        (*(*spc).opts).maintenance_io_concurrency
    }
}
