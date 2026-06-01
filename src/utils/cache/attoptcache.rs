//! attoptcache.c - Attribute options cache management.
//!
//! Attribute options are cached separately from the fixed-size portion of
//! pg_attribute entries, which are handled by the relcache.

use crate::prelude::*;

use crate::access::htup_details::{HeapTuple, HeapTupleIsValid};
use crate::utils::hash::dynahash::{
    hash_create, hash_search, hash_seq_init, hash_seq_init_with_hash_value, hash_seq_search,
    HASHACTION, HASHCTL, HASH_ELEM, HASH_FUNCTION, HASH_SEQ_STATUS,
};
use crate::utils::mmgr::mcxt::CacheMemoryContext;
use crate::varatt::VARSIZE;

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: Size) -> *mut c_void;
}

// ---------------------------------------------------------------------------
// Unported dependencies (local stubs)
// ---------------------------------------------------------------------------

// TODO: not ported - utils/attoptcache.h. Attribute options varlena struct.
#[repr(C)]
pub struct AttributeOpts {
    pub vl_len_: int32, // varlena header (do not touch directly!)
    pub n_distinct: float8,
    pub n_distinct_inherited: float8,
}

// TODO: not ported - utils/syscache.h. SysCacheIdentifier for pg_attribute by (attrelid, attnum).
const ATTNUM: c_int = 0;

// TODO: not ported - catalog/pg_attribute.h.
const Anum_pg_attribute_attoptions: c_int = 0;

// TODO: not ported - utils/syscache.h.
unsafe fn SearchSysCache2(_cacheId: c_int, _key1: Datum, _key2: Datum) -> HeapTuple {
    unimplemented!()
}

// TODO: not ported - utils/syscache.h.
unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    unimplemented!()
}

// TODO: not ported - utils/syscache.h.
unsafe fn SysCacheGetAttr(
    _cacheId: c_int,
    _tup: HeapTuple,
    _attributeNumber: c_int,
    _isNull: *mut bool,
) -> Datum {
    unimplemented!()
}

// TODO: not ported - utils/catcache.h. GetSysCacheHashValue2 macro.
unsafe fn GetSysCacheHashValue2(_cacheId: c_int, _key1: Datum, _key2: Datum) -> uint32 {
    unimplemented!()
}

// TODO: not ported - utils/inval.h.
type SyscacheCallbackFunction = unsafe extern "C" fn(arg: Datum, cacheid: c_int, hashvalue: uint32);

// TODO: not ported - utils/inval.h.
unsafe fn CacheRegisterSyscacheCallback(
    _cacheid: c_int,
    _func: SyscacheCallbackFunction,
    _arg: Datum,
) {
    unimplemented!()
}

// TODO: not ported - utils/memutils.h.
unsafe fn CreateCacheMemoryContext() {
    unimplemented!()
}

// TODO: not ported - access/reloptions.h.
unsafe fn attribute_reloptions(_reloptions: Datum, _validate: bool) -> *mut bytea {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// attoptcache
// ---------------------------------------------------------------------------

/* Hash table for information about each attribute's options */
static mut AttoptCacheHash: *mut crate::utils::hash::dynahash::HTAB = null_mut();

/* attrelid and attnum form the lookup key, and must appear first */
#[repr(C)]
struct AttoptCacheKey {
    attrelid: Oid,
    attnum: c_int,
}

#[repr(C)]
struct AttoptCacheEntry {
    key: AttoptCacheKey, // lookup key - must be first
    opts: *mut AttributeOpts, // options, or NULL if none
}

/*
 * InvalidateAttoptCacheCallback
 *		Flush cache entry (or entries) when pg_attribute is updated.
 *
 * When pg_attribute is updated, we must flush the cache entry at least
 * for that attribute.
 */
unsafe extern "C" fn InvalidateAttoptCacheCallback(_arg: Datum, _cacheid: c_int, hashvalue: uint32) {
    let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut attopt: *mut AttoptCacheEntry;

    /*
     * By convention, zero hash value is passed to the callback as a sign that
     * it's time to invalidate the whole cache. See sinval.c, inval.c and
     * InvalidateSystemCachesExtended().
     */
    if hashvalue == 0 {
        hash_seq_init(&mut status, AttoptCacheHash);
    } else {
        hash_seq_init_with_hash_value(&mut status, AttoptCacheHash, hashvalue);
    }

    loop {
        attopt = hash_seq_search(&mut status) as *mut AttoptCacheEntry;
        if attopt.is_null() {
            break;
        }
        if !(*attopt).opts.is_null() {
            pfree((*attopt).opts as *mut c_void);
        }
        if hash_search(
            AttoptCacheHash,
            &mut (*attopt).key as *mut AttoptCacheKey as *const c_void,
            HASHACTION::HASH_REMOVE,
            null_mut(),
        )
        .is_null()
        {
            elog!(ERROR, "hash table corrupted");
        }
    }
}

/*
 * Hash function compatible with two-arg system cache hash function.
 */
unsafe extern "C" fn relatt_cache_syshash(key: *const c_void, keysize: Size) -> uint32 {
    let ckey = key as *const AttoptCacheKey;

    Assert!(keysize == core::mem::size_of::<AttoptCacheKey>());
    GetSysCacheHashValue2(
        ATTNUM,
        ObjectIdGetDatum((*ckey).attrelid),
        Int32GetDatum((*ckey).attnum),
    )
}

/*
 * InitializeAttoptCache
 *		Initialize the attribute options cache.
 */
unsafe fn InitializeAttoptCache() {
    let mut ctl: HASHCTL = core::mem::zeroed();

    /* Initialize the hash table. */
    ctl.keysize = core::mem::size_of::<AttoptCacheKey>();
    ctl.entrysize = core::mem::size_of::<AttoptCacheEntry>();

    /*
     * AttoptCacheEntry takes hash value from the system cache. For
     * AttoptCacheHash we use the same hash in order to speedup search by hash
     * value. This is used by hash_seq_init_with_hash_value().
     */
    ctl.hash = Some(relatt_cache_syshash);

    AttoptCacheHash = hash_create(
        c"Attopt cache".as_ptr(),
        256,
        &ctl,
        HASH_ELEM | HASH_FUNCTION,
    );

    /* Make sure we've initialized CacheMemoryContext. */
    if CacheMemoryContext.is_null() {
        CreateCacheMemoryContext();
    }

    /* Watch for invalidation events. */
    CacheRegisterSyscacheCallback(ATTNUM, InvalidateAttoptCacheCallback, 0 as Datum);
}

/*
 * get_attribute_options
 *		Fetch attribute options for a specified table OID.
 */
pub unsafe fn get_attribute_options(attrelid: Oid, attnum: c_int) -> *mut AttributeOpts {
    let mut key: AttoptCacheKey = core::mem::zeroed();
    let mut attopt: *mut AttoptCacheEntry;
    let result: *mut AttributeOpts;
    let tp: HeapTuple;

    /* Find existing cache entry, if any. */
    if AttoptCacheHash.is_null() {
        InitializeAttoptCache();
    }
    /* make sure any padding bits are unset */
    core::ptr::write_bytes(&mut key as *mut AttoptCacheKey, 0, 1);
    key.attrelid = attrelid;
    key.attnum = attnum;
    attopt = hash_search(
        AttoptCacheHash,
        &key as *const AttoptCacheKey as *const c_void,
        HASHACTION::HASH_FIND,
        null_mut(),
    ) as *mut AttoptCacheEntry;

    /* Not found in Attopt cache.  Construct new cache entry. */
    if attopt.is_null() {
        let opts: *mut AttributeOpts;

        tp = SearchSysCache2(
            ATTNUM,
            ObjectIdGetDatum(attrelid),
            Int16GetDatum(attnum as int16),
        );

        /*
         * If we don't find a valid HeapTuple, it must mean someone has
         * managed to request attribute details for a non-existent attribute.
         * We treat that case as if no options were specified.
         */
        if !HeapTupleIsValid(tp) {
            opts = null_mut();
        } else {
            let datum: Datum;
            let mut isNull: bool = false;

            datum = SysCacheGetAttr(ATTNUM, tp, Anum_pg_attribute_attoptions, &mut isNull);
            if isNull {
                opts = null_mut();
            } else {
                let bytea_opts: *mut bytea = attribute_reloptions(datum, false);

                opts = MemoryContextAlloc(
                    CacheMemoryContext as *mut _,
                    VARSIZE(bytea_opts as *const c_char) as Size,
                ) as *mut AttributeOpts;
                memcpy(
                    opts as *mut c_void,
                    bytea_opts as *const c_void,
                    VARSIZE(bytea_opts as *const c_char) as Size,
                );
            }
            ReleaseSysCache(tp);
        }

        /*
         * It's important to create the actual cache entry only after reading
         * pg_attribute, since the read could cause a cache flush.
         */
        attopt = hash_search(
            AttoptCacheHash,
            &key as *const AttoptCacheKey as *const c_void,
            HASHACTION::HASH_ENTER,
            null_mut(),
        ) as *mut AttoptCacheEntry;
        (*attopt).opts = opts;
    }

    /* Return results in caller's memory context. */
    if (*attopt).opts.is_null() {
        return null_mut();
    }
    result = palloc(VARSIZE((*attopt).opts as *const c_char) as Size) as *mut AttributeOpts;
    memcpy(
        result as *mut c_void,
        (*attopt).opts as *const c_void,
        VARSIZE((*attopt).opts as *const c_char) as Size,
    );
    result
}
