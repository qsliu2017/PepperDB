//! src/backend/utils/cache/ts_cache.c
//!
//! ts_cache.c
//!   Tsearch related object caches.
//!
//! Tsearch performance is very sensitive to performance of parsers,
//! dictionaries and mapping, so lookups should be cached as much
//! as possible.
//!
//! Once a backend has created a cache entry for a particular TS object OID,
//! the cache entry will exist for the life of the backend; hence it is
//! safe to hold onto a pointer to the cache entry while doing things that
//! might result in recognizing a cache invalidation.  Beware however that
//! subsidiary information might be deleted and reallocated somewhere else
//! if a cache inval and reval happens!  This does not look like it will be
//! a big problem as long as parser and dictionary methods do not attempt
//! any database access.
//!
//! Copyright (c) 2006-2025, PostgreSQL Global Development Group
//!
//! IDENTIFICATION
//!   src/backend/utils/cache/ts_cache.c

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

use crate::c::uint32;
use crate::postgres_ext::Oid;

use crate::utils::fmgr::FmgrInfo;
use crate::utils::mmgr::mcxt::CacheMemoryContext;
use crate::utils::init::globals::MyDatabaseId;
use crate::nodes::nodes::{Node, NodeTag};
use crate::nodes::pg_list::{List, NIL};

/*
 * ---- ts_cache.h (src/include/tsearch/ts_cache.h) ----
 *
 * All TS*CacheEntry structs must share this common header
 * (see InvalidateTSCacheCallBack)
 */
#[repr(C)]
pub struct TSAnyCacheEntry {
    pub objId: Oid,
    pub isvalid: bool,
}

#[repr(C)]
pub struct TSParserCacheEntry {
    /* prsId is the hash lookup key and MUST BE FIRST */
    pub prsId: Oid, /* OID of the parser */
    pub isvalid: bool,

    pub startOid: Oid,
    pub tokenOid: Oid,
    pub endOid: Oid,
    pub headlineOid: Oid,
    pub lextypeOid: Oid,

    /*
     * Pre-set-up fmgr call of most needed parser's methods
     */
    pub prsstart: FmgrInfo,
    pub prstoken: FmgrInfo,
    pub prsend: FmgrInfo,
    pub prsheadline: FmgrInfo,
}

#[repr(C)]
pub struct TSDictionaryCacheEntry {
    /* dictId is the hash lookup key and MUST BE FIRST */
    pub dictId: Oid,
    pub isvalid: bool,

    /* most frequent fmgr call */
    pub lexizeOid: Oid,
    pub lexize: FmgrInfo,

    pub dictCtx: MemoryContext, /* memory context to store private data */
    pub dictData: *mut c_void,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ListDictionary {
    pub len: c_int,
    pub dictIds: *mut Oid,
}

#[repr(C)]
pub struct TSConfigCacheEntry {
    /* cfgId is the hash lookup key and MUST BE FIRST */
    pub cfgId: Oid,
    pub isvalid: bool,

    pub prsId: Oid,

    pub lenmap: c_int,
    pub map: *mut ListDictionary,
}

/* ---- end ts_cache.h ---- */

/*
 * MAXTOKENTYPE/MAXDICTSPERTT are arbitrary limits on the workspace size
 * used in lookup_ts_config_cache().  We could avoid hardwiring a limit
 * by making the workspace dynamically enlargeable, but it seems unlikely
 * to be worth the trouble.
 */
const MAXTOKENTYPE: usize = 256;
const MAXDICTSPERTT: usize = 100;

static mut TSParserCacheHash: *mut HTAB = std::ptr::null_mut();
static mut lastUsedParser: *mut TSParserCacheEntry = std::ptr::null_mut();

static mut TSDictionaryCacheHash: *mut HTAB = std::ptr::null_mut();
static mut lastUsedDictionary: *mut TSDictionaryCacheEntry = std::ptr::null_mut();

static mut TSConfigCacheHash: *mut HTAB = std::ptr::null_mut();
static mut lastUsedConfig: *mut TSConfigCacheEntry = std::ptr::null_mut();

/*
 * GUC default_text_search_config, and a cache of the current config's OID
 */
#[no_mangle]
pub static mut TSCurrentConfig: *mut c_char = std::ptr::null_mut();

static mut TSCurrentConfigCache: Oid = InvalidOid;

/*
 * We use this syscache callback to detect when a visible change to a TS
 * catalog entry has been made, by either our own backend or another one.
 *
 * In principle we could just flush the specific cache entry that changed,
 * but given that TS configuration changes are probably infrequent, it
 * doesn't seem worth the trouble to determine that; we just flush all the
 * entries of the related hash table.
 *
 * We can use the same function for all TS caches by passing the hash
 * table address as the "arg".
 */
unsafe extern "C" fn InvalidateTSCacheCallBack(arg: Datum, _cacheid: c_int, _hashvalue: uint32) {
    let hash = DatumGetPointer(arg) as *mut HTAB;
    let mut status: HASH_SEQ_STATUS = std::mem::zeroed();
    let mut entry: *mut TSAnyCacheEntry;

    hash_seq_init(&mut status, hash);
    loop {
        entry = hash_seq_search(&mut status) as *mut TSAnyCacheEntry;
        if entry.is_null() {
            break;
        }
        (*entry).isvalid = false;
    }

    /* Also invalidate the current-config cache if it's pg_ts_config */
    if hash == TSConfigCacheHash {
        TSCurrentConfigCache = InvalidOid;
    }
}

/*
 * Fetch parser cache entry
 */
pub unsafe fn lookup_ts_parser_cache(prsId: Oid) -> *mut TSParserCacheEntry {
    let mut entry: *mut TSParserCacheEntry;

    if TSParserCacheHash.is_null() {
        /* First time through: initialize the hash table */
        let mut ctl: HASHCTL = std::mem::zeroed();

        ctl.keysize = std::mem::size_of::<Oid>() as Size;
        ctl.entrysize = std::mem::size_of::<TSParserCacheEntry>() as Size;
        TSParserCacheHash = hash_create(
            c"Tsearch parser cache".as_ptr(),
            4,
            &mut ctl,
            (HASH_ELEM | HASH_BLOBS) as c_int,
        );
        /* Flush cache on pg_ts_parser changes */
        CacheRegisterSyscacheCallback(
            TSPARSEROID,
            InvalidateTSCacheCallBack,
            PointerGetDatum(TSParserCacheHash as *const c_void),
        );

        /* Also make sure (CacheMemoryContext as *mut _) exists */
        if CacheMemoryContext.is_null() {
            CreateCacheMemoryContext();
        }
    }

    /* Check single-entry cache */
    if !lastUsedParser.is_null()
        && (*lastUsedParser).prsId == prsId
        && (*lastUsedParser).isvalid
    {
        return lastUsedParser;
    }

    /* Try to look up an existing entry */
    entry = hash_search(
        TSParserCacheHash,
        &prsId as *const Oid as *const c_void,
        HASHACTION_HASH_FIND,
        std::ptr::null_mut(),
    ) as *mut TSParserCacheEntry;
    if entry.is_null() || !(*entry).isvalid {
        /*
         * If we didn't find one, we want to make one. But first look up the
         * object to be sure the OID is real.
         */
        let tp: HeapTuple;
        let prs: Form_pg_ts_parser;

        tp = SearchSysCache1(TSPARSEROID, ObjectIdGetDatum(prsId));
        if !HeapTupleIsValid(tp) {
            elog!(
                ERROR,
                "cache lookup failed for text search parser {}",
                prsId
            );
        }
        prs = GETSTRUCT(tp) as Form_pg_ts_parser;

        /*
         * Sanity checks
         */
        if !OidIsValid((*prs).prsstart) {
            elog!(ERROR, "text search parser {} has no prsstart method", prsId);
        }
        if !OidIsValid((*prs).prstoken) {
            elog!(ERROR, "text search parser {} has no prstoken method", prsId);
        }
        if !OidIsValid((*prs).prsend) {
            elog!(ERROR, "text search parser {} has no prsend method", prsId);
        }

        if entry.is_null() {
            let mut found: bool = false;

            /* Now make the cache entry */
            entry = hash_search(
                TSParserCacheHash,
                &prsId as *const Oid as *const c_void,
                HASHACTION_HASH_ENTER,
                &mut found,
            ) as *mut TSParserCacheEntry;
            Assert!(!found); /* it wasn't there a moment ago */
        }

        MemSet(
            entry as *mut c_void,
            0,
            std::mem::size_of::<TSParserCacheEntry>(),
        );
        (*entry).prsId = prsId;
        (*entry).startOid = (*prs).prsstart;
        (*entry).tokenOid = (*prs).prstoken;
        (*entry).endOid = (*prs).prsend;
        (*entry).headlineOid = (*prs).prsheadline;
        (*entry).lextypeOid = (*prs).prslextype;

        ReleaseSysCache(tp);

        fmgr_info_cxt((*entry).startOid, &mut (*entry).prsstart, (CacheMemoryContext as *mut _));
        fmgr_info_cxt((*entry).tokenOid, &mut (*entry).prstoken, (CacheMemoryContext as *mut _));
        fmgr_info_cxt((*entry).endOid, &mut (*entry).prsend, (CacheMemoryContext as *mut _));
        if OidIsValid((*entry).headlineOid) {
            fmgr_info_cxt(
                (*entry).headlineOid,
                &mut (*entry).prsheadline,
                (CacheMemoryContext as *mut _),
            );
        }

        (*entry).isvalid = true;
    }

    lastUsedParser = entry;

    entry
}

/*
 * Fetch dictionary cache entry
 */
pub unsafe fn lookup_ts_dictionary_cache(dictId: Oid) -> *mut TSDictionaryCacheEntry {
    let mut entry: *mut TSDictionaryCacheEntry;

    if TSDictionaryCacheHash.is_null() {
        /* First time through: initialize the hash table */
        let mut ctl: HASHCTL = std::mem::zeroed();

        ctl.keysize = std::mem::size_of::<Oid>() as Size;
        ctl.entrysize = std::mem::size_of::<TSDictionaryCacheEntry>() as Size;
        TSDictionaryCacheHash = hash_create(
            c"Tsearch dictionary cache".as_ptr(),
            8,
            &mut ctl,
            (HASH_ELEM | HASH_BLOBS) as c_int,
        );
        /* Flush cache on pg_ts_dict and pg_ts_template changes */
        CacheRegisterSyscacheCallback(
            TSDICTOID,
            InvalidateTSCacheCallBack,
            PointerGetDatum(TSDictionaryCacheHash as *const c_void),
        );
        CacheRegisterSyscacheCallback(
            TSTEMPLATEOID,
            InvalidateTSCacheCallBack,
            PointerGetDatum(TSDictionaryCacheHash as *const c_void),
        );

        /* Also make sure (CacheMemoryContext as *mut _) exists */
        if CacheMemoryContext.is_null() {
            CreateCacheMemoryContext();
        }
    }

    /* Check single-entry cache */
    if !lastUsedDictionary.is_null()
        && (*lastUsedDictionary).dictId == dictId
        && (*lastUsedDictionary).isvalid
    {
        return lastUsedDictionary;
    }

    /* Try to look up an existing entry */
    entry = hash_search(
        TSDictionaryCacheHash,
        &dictId as *const Oid as *const c_void,
        HASHACTION_HASH_FIND,
        std::ptr::null_mut(),
    ) as *mut TSDictionaryCacheEntry;
    if entry.is_null() || !(*entry).isvalid {
        /*
         * If we didn't find one, we want to make one. But first look up the
         * object to be sure the OID is real.
         */
        let tpdict: HeapTuple;
        let tptmpl: HeapTuple;
        let dict: Form_pg_ts_dict;
        let template: Form_pg_ts_template;
        let saveCtx: MemoryContext;

        tpdict = SearchSysCache1(TSDICTOID, ObjectIdGetDatum(dictId));
        if !HeapTupleIsValid(tpdict) {
            elog!(
                ERROR,
                "cache lookup failed for text search dictionary {}",
                dictId
            );
        }
        dict = GETSTRUCT(tpdict) as Form_pg_ts_dict;

        /*
         * Sanity checks
         */
        if !OidIsValid((*dict).dicttemplate) {
            elog!(ERROR, "text search dictionary {} has no template", dictId);
        }

        /*
         * Retrieve dictionary's template
         */
        tptmpl = SearchSysCache1(TSTEMPLATEOID, ObjectIdGetDatum((*dict).dicttemplate));
        if !HeapTupleIsValid(tptmpl) {
            elog!(
                ERROR,
                "cache lookup failed for text search template {}",
                (*dict).dicttemplate
            );
        }
        template = GETSTRUCT(tptmpl) as Form_pg_ts_template;

        /*
         * Sanity checks
         */
        if !OidIsValid((*template).tmpllexize) {
            elog!(
                ERROR,
                "text search template {} has no lexize method",
                (*template).tmpllexize
            );
        }

        if entry.is_null() {
            let mut found: bool = false;

            /* Now make the cache entry */
            entry = hash_search(
                TSDictionaryCacheHash,
                &dictId as *const Oid as *const c_void,
                HASHACTION_HASH_ENTER,
                &mut found,
            ) as *mut TSDictionaryCacheEntry;
            Assert!(!found); /* it wasn't there a moment ago */

            /* Create private memory context the first time through */
            saveCtx = AllocSetContextCreateInternal(
                (CacheMemoryContext as *mut _),
                c"TS dictionary".as_ptr(),
                ALLOCSET_SMALL_MINSIZE,
                ALLOCSET_SMALL_INITSIZE,
                ALLOCSET_SMALL_MAXSIZE,
            );
            MemoryContextCopyAndSetIdentifier(saveCtx, NameStr(&(*dict).dictname));
        } else {
            /* Clear the existing entry's private context */
            saveCtx = (*entry).dictCtx;
            /* Don't let context's ident pointer dangle while we reset it */
            MemoryContextSetIdentifier(saveCtx, std::ptr::null());
            MemoryContextReset(saveCtx);
            MemoryContextCopyAndSetIdentifier(saveCtx, NameStr(&(*dict).dictname));
        }

        MemSet(
            entry as *mut c_void,
            0,
            std::mem::size_of::<TSDictionaryCacheEntry>(),
        );
        (*entry).dictId = dictId;
        (*entry).dictCtx = saveCtx;

        (*entry).lexizeOid = (*template).tmpllexize;

        if OidIsValid((*template).tmplinit) {
            let dictoptions: *mut List;
            let opt: Datum;
            let mut isnull: bool = false;
            let oldcontext: MemoryContext;

            /*
             * Init method runs in dictionary's private memory context, and we
             * make sure the options are stored there too
             */
            oldcontext = MemoryContextSwitchTo((*entry).dictCtx);

            opt = SysCacheGetAttr(
                TSDICTOID,
                tpdict,
                Anum_pg_ts_dict_dictinitoption,
                &mut isnull,
            );
            if isnull {
                dictoptions = NIL;
            } else {
                dictoptions = deserialize_deflist(opt);
            }

            (*entry).dictData = DatumGetPointer(OidFunctionCall1(
                (*template).tmplinit,
                PointerGetDatum(dictoptions as *const c_void),
            )) as *mut c_void;

            MemoryContextSwitchTo(oldcontext);
        }

        ReleaseSysCache(tptmpl);
        ReleaseSysCache(tpdict);

        fmgr_info_cxt((*entry).lexizeOid, &mut (*entry).lexize, (*entry).dictCtx);

        (*entry).isvalid = true;
    }

    lastUsedDictionary = entry;

    entry
}

/*
 * Initialize config cache and prepare callbacks.  This is split out of
 * lookup_ts_config_cache because we need to activate the callback before
 * caching TSCurrentConfigCache, too.
 */
unsafe fn init_ts_config_cache() {
    let mut ctl: HASHCTL = std::mem::zeroed();

    ctl.keysize = std::mem::size_of::<Oid>() as Size;
    ctl.entrysize = std::mem::size_of::<TSConfigCacheEntry>() as Size;
    TSConfigCacheHash = hash_create(
        c"Tsearch configuration cache".as_ptr(),
        16,
        &mut ctl,
        (HASH_ELEM | HASH_BLOBS) as c_int,
    );
    /* Flush cache on pg_ts_config and pg_ts_config_map changes */
    CacheRegisterSyscacheCallback(
        TSCONFIGOID,
        InvalidateTSCacheCallBack,
        PointerGetDatum(TSConfigCacheHash as *const c_void),
    );
    CacheRegisterSyscacheCallback(
        TSCONFIGMAP,
        InvalidateTSCacheCallBack,
        PointerGetDatum(TSConfigCacheHash as *const c_void),
    );

    /* Also make sure (CacheMemoryContext as *mut _) exists */
    if CacheMemoryContext.is_null() {
        CreateCacheMemoryContext();
    }
}

/*
 * Fetch configuration cache entry
 */
pub unsafe fn lookup_ts_config_cache(cfgId: Oid) -> *mut TSConfigCacheEntry {
    let mut entry: *mut TSConfigCacheEntry;

    if TSConfigCacheHash.is_null() {
        /* First time through: initialize the hash table */
        init_ts_config_cache();
    }

    /* Check single-entry cache */
    if !lastUsedConfig.is_null()
        && (*lastUsedConfig).cfgId == cfgId
        && (*lastUsedConfig).isvalid
    {
        return lastUsedConfig;
    }

    /* Try to look up an existing entry */
    entry = hash_search(
        TSConfigCacheHash,
        &cfgId as *const Oid as *const c_void,
        HASHACTION_HASH_FIND,
        std::ptr::null_mut(),
    ) as *mut TSConfigCacheEntry;
    if entry.is_null() || !(*entry).isvalid {
        /*
         * If we didn't find one, we want to make one. But first look up the
         * object to be sure the OID is real.
         */
        let tp: HeapTuple;
        let cfg: Form_pg_ts_config;
        let maprel: Relation;
        let mapidx: Relation;
        let mut mapskey: ScanKeyData = std::mem::zeroed();
        let mapscan: SysScanDesc;
        let mut maptup: HeapTuple;
        let mut maplists: [ListDictionary; MAXTOKENTYPE + 1] = std::mem::zeroed();
        let mut mapdicts: [Oid; MAXDICTSPERTT] = std::mem::zeroed();
        let mut maxtokentype: c_int;
        let mut ndicts: c_int;
        let mut i: c_int;

        tp = SearchSysCache1(TSCONFIGOID, ObjectIdGetDatum(cfgId));
        if !HeapTupleIsValid(tp) {
            elog!(
                ERROR,
                "cache lookup failed for text search configuration {}",
                cfgId
            );
        }
        cfg = GETSTRUCT(tp) as Form_pg_ts_config;

        /*
         * Sanity checks
         */
        if !OidIsValid((*cfg).cfgparser) {
            elog!(ERROR, "text search configuration {} has no parser", cfgId);
        }

        if entry.is_null() {
            let mut found: bool = false;

            /* Now make the cache entry */
            entry = hash_search(
                TSConfigCacheHash,
                &cfgId as *const Oid as *const c_void,
                HASHACTION_HASH_ENTER,
                &mut found,
            ) as *mut TSConfigCacheEntry;
            Assert!(!found); /* it wasn't there a moment ago */
        } else {
            /* Cleanup old contents */
            if !(*entry).map.is_null() {
                i = 0;
                while i < (*entry).lenmap {
                    if !(*(*entry).map.add(i as usize)).dictIds.is_null() {
                        pfree((*(*entry).map.add(i as usize)).dictIds as *mut c_void);
                    }
                    i += 1;
                }
                pfree((*entry).map as *mut c_void);
            }
        }

        MemSet(
            entry as *mut c_void,
            0,
            std::mem::size_of::<TSConfigCacheEntry>(),
        );
        (*entry).cfgId = cfgId;
        (*entry).prsId = (*cfg).cfgparser;

        ReleaseSysCache(tp);

        /*
         * Scan pg_ts_config_map to gather dictionary list for each token type
         *
         * Because the index is on (mapcfg, maptokentype, mapseqno), we will
         * see the entries in maptokentype order, and in mapseqno order for
         * each token type, even though we didn't explicitly ask for that.
         */
        MemSet(
            maplists.as_mut_ptr() as *mut c_void,
            0,
            std::mem::size_of::<[ListDictionary; MAXTOKENTYPE + 1]>(),
        );
        maxtokentype = 0;
        ndicts = 0;

        ScanKeyInit(
            &mut mapskey,
            Anum_pg_ts_config_map_mapcfg,
            BTEqualStrategyNumber as StrategyNumber,
            F_OIDEQ,
            ObjectIdGetDatum(cfgId),
        );

        maprel = table_open(TSConfigMapRelationId, AccessShareLock as LOCKMODE);
        mapidx = index_open(TSConfigMapIndexId, AccessShareLock as LOCKMODE);
        mapscan = systable_beginscan_ordered(maprel, mapidx, std::ptr::null_mut(), 1, &mut mapskey);

        loop {
            maptup = systable_getnext_ordered(mapscan, ForwardScanDirection);
            if maptup.is_null() {
                break;
            }
            let cfgmap: Form_pg_ts_config_map = GETSTRUCT(maptup) as Form_pg_ts_config_map;
            let toktype: c_int = (*cfgmap).maptokentype;

            if toktype <= 0 || toktype > MAXTOKENTYPE as c_int {
                elog!(ERROR, "maptokentype value {} is out of range", toktype);
            }
            if toktype < maxtokentype {
                elog!(ERROR, "maptokentype entries are out of order");
            }
            if toktype > maxtokentype {
                /* starting a new token type, but first save the prior data */
                if ndicts > 0 {
                    maplists[maxtokentype as usize].len = ndicts;
                    maplists[maxtokentype as usize].dictIds = MemoryContextAlloc(
                        (CacheMemoryContext as *mut _),
                        (std::mem::size_of::<Oid>() * ndicts as usize) as Size,
                    ) as *mut Oid;
                    memcpy(
                        maplists[maxtokentype as usize].dictIds as *mut c_void,
                        mapdicts.as_ptr() as *const c_void,
                        std::mem::size_of::<Oid>() * ndicts as usize,
                    );
                }
                maxtokentype = toktype;
                mapdicts[0] = (*cfgmap).mapdict;
                ndicts = 1;
            } else {
                /* continuing data for current token type */
                if ndicts >= MAXDICTSPERTT as c_int {
                    elog!(
                        ERROR,
                        "too many pg_ts_config_map entries for one token type"
                    );
                }
                mapdicts[ndicts as usize] = (*cfgmap).mapdict;
                ndicts += 1;
            }
        }

        systable_endscan_ordered(mapscan);
        index_close(mapidx, AccessShareLock as LOCKMODE);
        table_close(maprel, AccessShareLock as LOCKMODE);

        if ndicts > 0 {
            /* save the last token type's dictionaries */
            maplists[maxtokentype as usize].len = ndicts;
            maplists[maxtokentype as usize].dictIds = MemoryContextAlloc(
                (CacheMemoryContext as *mut _),
                (std::mem::size_of::<Oid>() * ndicts as usize) as Size,
            ) as *mut Oid;
            memcpy(
                maplists[maxtokentype as usize].dictIds as *mut c_void,
                mapdicts.as_ptr() as *const c_void,
                std::mem::size_of::<Oid>() * ndicts as usize,
            );
            /* and save the overall map */
            (*entry).lenmap = maxtokentype + 1;
            (*entry).map = MemoryContextAlloc(
                (CacheMemoryContext as *mut _),
                (std::mem::size_of::<ListDictionary>() * (*entry).lenmap as usize) as Size,
            ) as *mut ListDictionary;
            memcpy(
                (*entry).map as *mut c_void,
                maplists.as_ptr() as *const c_void,
                std::mem::size_of::<ListDictionary>() * (*entry).lenmap as usize,
            );
        }

        (*entry).isvalid = true;
    }

    lastUsedConfig = entry;

    entry
}

/*---------------------------------------------------
 * GUC variable "default_text_search_config"
 *---------------------------------------------------
 */

pub unsafe fn getTSCurrentConfig(emitError: bool) -> Oid {
    let namelist: *mut List;

    /* if we have a cached value, return it */
    if OidIsValid(TSCurrentConfigCache) {
        return TSCurrentConfigCache;
    }

    /* fail if GUC hasn't been set up yet */
    if TSCurrentConfig.is_null() || *TSCurrentConfig == 0 {
        if emitError {
            elog!(ERROR, "text search configuration isn't set");
        } else {
            return InvalidOid;
        }
    }

    if TSConfigCacheHash.is_null() {
        /* First time through: initialize the tsconfig inval callback */
        init_ts_config_cache();
    }

    /* Look up the config */
    if emitError {
        namelist = stringToQualifiedNameList(TSCurrentConfig, std::ptr::null_mut());
        TSCurrentConfigCache = get_ts_config_oid(namelist, false);
    } else {
        let mut escontext: ErrorSaveContext = std::mem::zeroed();
        escontext.type_ = NodeTag::T_ErrorSaveContext;

        namelist =
            stringToQualifiedNameList(TSCurrentConfig, &mut escontext as *mut _ as *mut Node);
        if namelist != NIL {
            TSCurrentConfigCache = get_ts_config_oid(namelist, true);
        } else {
            TSCurrentConfigCache = InvalidOid; /* bad name list syntax */
        }
    }

    TSCurrentConfigCache
}

/* GUC check_hook for default_text_search_config */
pub unsafe fn check_default_text_search_config(
    newval: *mut *mut c_char,
    _extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    /*
     * If we aren't inside a transaction, or connected to a database, we
     * cannot do the catalog accesses necessary to verify the config name.
     * Must accept it on faith.
     */
    if IsTransactionState() && MyDatabaseId != InvalidOid {
        let mut escontext: ErrorSaveContext = std::mem::zeroed();
        escontext.type_ = NodeTag::T_ErrorSaveContext;
        let namelist: *mut List;
        let cfgId: Oid;
        let tuple: HeapTuple;
        let cfg: Form_pg_ts_config;
        let buf: *mut c_char;

        namelist = stringToQualifiedNameList(*newval, &mut escontext as *mut _ as *mut Node);
        if namelist != NIL {
            cfgId = get_ts_config_oid(namelist, true);
        } else {
            cfgId = InvalidOid; /* bad name list syntax */
        }

        /*
         * When source == PGC_S_TEST, don't throw a hard error for a
         * nonexistent configuration, only a NOTICE.  See comments in guc.h.
         */
        if !OidIsValid(cfgId) {
            if source == GucSource_PGC_S_TEST {
                ereport!(
                    NOTICE,
                    "text search configuration does not exist"
                );
                return true;
            } else {
                return false;
            }
        }

        /*
         * Modify the actually stored value to be fully qualified, to ensure
         * later changes of search_path don't affect it.
         */
        tuple = SearchSysCache1(TSCONFIGOID, ObjectIdGetDatum(cfgId));
        if !HeapTupleIsValid(tuple) {
            elog!(
                ERROR,
                "cache lookup failed for text search configuration {}",
                cfgId
            );
        }
        cfg = GETSTRUCT(tuple) as Form_pg_ts_config;

        buf = quote_qualified_identifier(
            get_namespace_name((*cfg).cfgnamespace),
            NameStr(&(*cfg).cfgname),
        );

        ReleaseSysCache(tuple);

        /* GUC wants it guc_malloc'd not palloc'd */
        guc_free(*newval as *mut c_void);
        *newval = guc_strdup(LOG, buf);
        pfree(buf as *mut c_void);
        if (*newval).is_null() {
            return false;
        }
    }

    true
}

/* GUC assign_hook for default_text_search_config */
pub unsafe fn assign_default_text_search_config(_newval: *const c_char, _extra: *mut c_void) {
    /* Just reset the cache to force a lookup on first use */
    TSCurrentConfigCache = InvalidOid;
}

/* ---- local stubs for unported helpers ---- */

#[repr(C)]
pub struct HTAB {
    _private: [u8; 0],
}

#[repr(C)]
pub struct HASHCTL {
    pub keysize: Size,
    pub entrysize: Size,
}

#[repr(C)]
pub struct HASH_SEQ_STATUS {
    _private: [u8; 0],
}

pub type HeapTuple = *mut c_void;
pub type Form_pg_ts_parser = *mut Pg_ts_parser_data;
pub type Form_pg_ts_dict = *mut Pg_ts_dict_data;
pub type Form_pg_ts_template = *mut Pg_ts_template_data;
pub type Form_pg_ts_config = *mut Pg_ts_config_data;
pub type Form_pg_ts_config_map = *mut Pg_ts_config_map_data;

#[repr(C)]
pub struct Pg_ts_parser_data {
    pub prsstart: Oid,
    pub prstoken: Oid,
    pub prsend: Oid,
    pub prsheadline: Oid,
    pub prslextype: Oid,
}

#[repr(C)]
pub struct Pg_ts_dict_data {
    pub dictname: NameData,
    pub dicttemplate: Oid,
}

#[repr(C)]
pub struct Pg_ts_template_data {
    pub tmplinit: Oid,
    pub tmpllexize: Oid,
}

#[repr(C)]
pub struct Pg_ts_config_data {
    pub cfgname: NameData,
    pub cfgnamespace: Oid,
    pub cfgparser: Oid,
}

#[repr(C)]
pub struct Pg_ts_config_map_data {
    pub mapcfg: Oid,
    pub maptokentype: c_int,
    pub mapseqno: c_int,
    pub mapdict: Oid,
}

#[repr(C)]
pub struct NameData {
    pub data: [c_char; 64],
}

pub type Relation = *mut c_void;
pub type SysScanDesc = *mut c_void;
pub type StrategyNumber = u16;
pub type LOCKMODE = c_int;
pub type GucSource = c_int;
pub type ScanDirection = c_int;

#[repr(C)]
pub struct ScanKeyData {
    _private: [u8; 0],
}

#[repr(C)]
pub struct ErrorSaveContext {
    pub type_: NodeTag,
    pub error_occurred: bool,
    pub details_wanted: bool,
}

const HASH_ELEM: u32 = 0x0008;
const HASH_BLOBS: u32 = 0x0010;

pub const HASHACTION_HASH_FIND: c_int = 0;
pub const HASHACTION_HASH_ENTER: c_int = 1;

const ALLOCSET_SMALL_MINSIZE: Size = 0;
const ALLOCSET_SMALL_INITSIZE: Size = 1 * 1024;
const ALLOCSET_SMALL_MAXSIZE: Size = 8 * 1024 * 1024;

const BTEqualStrategyNumber: c_int = 3;
const AccessShareLock: c_int = 1;
const ForwardScanDirection: ScanDirection = 1;
const F_OIDEQ: Oid = 184;
const GucSource_PGC_S_TEST: GucSource = 17;

const Anum_pg_ts_dict_dictinitoption: c_int = 4;
const Anum_pg_ts_config_map_mapcfg: c_int = 1;

const TSPARSEROID: c_int = 0;
const TSDICTOID: c_int = 0;
const TSTEMPLATEOID: c_int = 0;
const TSCONFIGOID: c_int = 0;
const TSCONFIGMAP: c_int = 0;

const TSConfigMapRelationId: Oid = 3603;
const TSConfigMapIndexId: Oid = 3609;

pub type SyscacheCallbackFunction = unsafe extern "C" fn(Datum, c_int, uint32);

unsafe fn hash_create(
    _tabname: *const c_char,
    _nelem: c_long,
    _info: *mut HASHCTL,
    _flags: c_int,
) -> *mut HTAB {
    unimplemented!() // TODO: utils/hash/dynahash.c
}

unsafe fn hash_search(
    _hashp: *mut HTAB,
    _key: *const c_void,
    _action: c_int,
    _foundPtr: *mut bool,
) -> *mut c_void {
    unimplemented!() // TODO: utils/hash/dynahash.c
}

unsafe fn hash_seq_init(_status: *mut HASH_SEQ_STATUS, _hashp: *mut HTAB) {
    unimplemented!() // TODO: utils/hash/dynahash.c
}

unsafe fn hash_seq_search(_status: *mut HASH_SEQ_STATUS) -> *mut c_void {
    unimplemented!() // TODO: utils/hash/dynahash.c
}

unsafe fn CacheRegisterSyscacheCallback(
    _cacheid: c_int,
    _func: SyscacheCallbackFunction,
    _arg: Datum,
) {
    unimplemented!() // TODO: utils/cache/inval.c
}

unsafe fn CreateCacheMemoryContext() {
    unimplemented!() // TODO: utils/cache/catcache.c
}

unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    unimplemented!() // TODO: utils/cache/syscache.c
}

unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    unimplemented!() // TODO: utils/cache/syscache.c
}

unsafe fn SysCacheGetAttr(
    _cacheId: c_int,
    _tup: HeapTuple,
    _attributeNumber: c_int,
    _isNull: *mut bool,
) -> Datum {
    unimplemented!() // TODO: utils/cache/syscache.c
}

unsafe fn HeapTupleIsValid(tuple: HeapTuple) -> bool {
    !tuple.is_null()
}

unsafe fn GETSTRUCT(_tuple: HeapTuple) -> *mut c_void {
    unimplemented!() // TODO: access/htup_details.h
}

unsafe fn fmgr_info_cxt(_functionId: Oid, _finfo: *mut FmgrInfo, _mcxt: MemoryContext) {
    unimplemented!() // TODO: utils/fmgr.c
}

unsafe fn OidFunctionCall1(_functionId: Oid, _arg1: Datum) -> Datum {
    unimplemented!() // TODO: utils/fmgr.c
}

unsafe fn AllocSetContextCreateInternal(
    _parent: MemoryContext,
    _name: *const c_char,
    _minContextSize: Size,
    _initBlockSize: Size,
    _maxBlockSize: Size,
) -> MemoryContext {
    unimplemented!() // TODO: utils/mmgr/aset.c
}

unsafe fn MemoryContextCopyAndSetIdentifier(_context: MemoryContext, _id: *const c_char) {
    unimplemented!() // TODO: utils/mmgr/mcxt.c
}

unsafe fn NameStr(_name: *const NameData) -> *const c_char {
    unimplemented!() // TODO: c.h (NameStr macro)
}

unsafe fn deserialize_deflist(_in: Datum) -> *mut List {
    unimplemented!() // TODO: commands/tsearchcmds.c
}

unsafe fn ScanKeyInit(
    _entry: *mut ScanKeyData,
    _attributeNumber: c_int,
    _strategy: StrategyNumber,
    _procedure: Oid,
    _argument: Datum,
) {
    unimplemented!() // TODO: access/common/scankey.c
}

unsafe fn table_open(_relationId: Oid, _lockmode: LOCKMODE) -> Relation {
    unimplemented!() // TODO: access/table/table.c
}

unsafe fn table_close(_relation: Relation, _lockmode: LOCKMODE) {
    unimplemented!() // TODO: access/table/table.c
}

unsafe fn index_open(_relationId: Oid, _lockmode: LOCKMODE) -> Relation {
    unimplemented!() // TODO: access/index/indexam.c
}

unsafe fn index_close(_relation: Relation, _lockmode: LOCKMODE) {
    unimplemented!() // TODO: access/index/indexam.c
}

unsafe fn systable_beginscan_ordered(
    _heapRelation: Relation,
    _indexRelation: Relation,
    _snapshot: *mut c_void,
    _nkeys: c_int,
    _key: *mut ScanKeyData,
) -> SysScanDesc {
    unimplemented!() // TODO: access/index/genam.c
}

unsafe fn systable_getnext_ordered(_sysscan: SysScanDesc, _direction: ScanDirection) -> HeapTuple {
    unimplemented!() // TODO: access/index/genam.c
}

unsafe fn systable_endscan_ordered(_sysscan: SysScanDesc) {
    unimplemented!() // TODO: access/index/genam.c
}

unsafe fn stringToQualifiedNameList(_string: *const c_char, _escontext: *mut Node) -> *mut List {
    unimplemented!() // TODO: catalog/namespace.c
}

unsafe fn get_ts_config_oid(_names: *mut List, _missing_ok: bool) -> Oid {
    unimplemented!() // TODO: catalog/namespace.c
}

unsafe fn get_namespace_name(_nspid: Oid) -> *mut c_char {
    unimplemented!() // TODO: utils/cache/lsyscache.c
}

unsafe fn quote_qualified_identifier(
    _qualifier: *const c_char,
    _ident: *const c_char,
) -> *mut c_char {
    unimplemented!() // TODO: utils/adt/ruleutils.c
}

unsafe fn IsTransactionState() -> bool {
    unimplemented!() // TODO: access/transam/xact.c
}

unsafe fn guc_free(_ptr: *mut c_void) {
    unimplemented!() // TODO: utils/misc/guc.c
}

unsafe fn guc_strdup(_elevel: c_int, _src: *const c_char) -> *mut c_char {
    unimplemented!() // TODO: utils/misc/guc.c
}

unsafe fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void {
    std::ptr::copy_nonoverlapping(src as *const u8, dest as *mut u8, n);
    dest
}

#[allow(non_snake_case)]
unsafe fn MemSet(ptr: *mut c_void, val: c_int, len: usize) {
    std::ptr::write_bytes(ptr as *mut u8, val as u8, len);
}
