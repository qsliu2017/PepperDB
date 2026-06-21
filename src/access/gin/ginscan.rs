//! src/backend/access/gin/ginscan.c
//!   routines to manage scans of inverted index relations
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use crate::utils::fmgr::FmgrInfo;
use crate::access::stratnum::{StrategyNumber, InvalidStrategy};
use crate::storage::off::{OffsetNumber, FirstOffsetNumber, InvalidOffsetNumber};
use crate::storage::itemptr::{ItemPointerData, ItemPointer};
use crate::storage::buf::{Buffer, InvalidBuffer};
use crate::storage::block::InvalidBlockNumber;
use crate::utils::rel::Relation;
use crate::access::relscan::IndexScanDescData;
use crate::access::common::scankey::{ScanKey, ScanKeyData, SK_ISNULL};
use crate::nodes::tidbitmap::{TIDBitmap, TBMPrivateIterator, TBMIterateResult, TBM_MAX_TUPLES_PER_PAGE};
use crate::access::gin::ginblock::{
    GinNullCategory, ItemPointerSetMin, GIN_CAT_NULL_KEY, GIN_CAT_EMPTY_ITEM, GIN_CAT_EMPTY_QUERY,
};
use crate::access::gin::gin::{
    GinTernaryValue, GinStatsData, GIN_SEARCH_MODE_DEFAULT, GIN_SEARCH_MODE_INCLUDE_EMPTY,
    GIN_SEARCH_MODE_ALL, GIN_SEARCH_MODE_EVERYTHING,
};
use crate::access::gin::gin_private::{GinState, GinBtreeData};
use crate::pg_config_manual::INDEX_MAX_KEYS;

// `IndexScanDesc` pointer alias over the real relscan struct (the canonical
// alias in access/index/amapi.rs is an opaque `*mut c_void`).
pub type IndexScanDesc = *mut IndexScanDescData;

// `Max!` / `ALLOCSET_DEFAULT_SIZES!` are written as macros in the translated
// source.  `Max` exists only as a generic fn (c.rs) and ALLOCSET_DEFAULT_SIZES
// only as a const tuple (memutils.rs); provide local macro wrappers so the
// call sites resolve without changing the translated logic.
macro_rules! Max {
    ($x:expr, $y:expr $(,)?) => {
        crate::c::Max($x, $y)
    };
}

macro_rules! ALLOCSET_DEFAULT_SIZES {
    () => {
        crate::utils::memutils::ALLOCSET_DEFAULT_SIZES
    };
}

// Merged decls from access/gin/gin_private.h (ginscan.c-relevant portions).

/*
 * GinScanKeyData describes a single GIN index qualifier expression.
 *
 * From each qual expression, we extract one or more specific index search
 * conditions, which are represented by GinScanEntryData.  It's quite
 * possible for identical search conditions to be requested by more than
 * one qual expression, in which case we merge such conditions to have just
 * one unique GinScanEntry --- this is particularly important for efficiency
 * when dealing with full-index-scan entries.  So there can be multiple
 * GinScanKeyData.scanEntry pointers to the same GinScanEntryData.
 *
 * In each GinScanKeyData, nentries is the true number of entries, while
 * nuserentries is the number that extractQueryFn returned (which is what
 * we report to consistentFn).  The "user" entries must come first.
 */
pub type GinScanKey = *mut GinScanKeyData;

pub type GinScanEntry = *mut GinScanEntryData;

#[repr(C)]
pub struct GinScanKeyData {
    /* Real number of entries in scanEntry[] (always > 0) */
    pub nentries: uint32,
    /* Number of entries that extractQueryFn and consistentFn know about */
    pub nuserentries: uint32,

    /* array of GinScanEntry pointers, one per extracted search condition */
    pub scanEntry: *mut GinScanEntry,

    /*
     * At least one of the entries in requiredEntries must be present for a
     * tuple to match the overall qual.
     *
     * additionalEntries contains entries that are needed by the consistent
     * function to decide if an item matches, but are not sufficient to
     * satisfy the qual without entries from requiredEntries.
     */
    pub requiredEntries: *mut GinScanEntry,
    pub nrequired: c_int,
    pub additionalEntries: *mut GinScanEntry,
    pub nadditional: c_int,

    /* array of check flags, reported to consistentFn */
    pub entryRes: *mut GinTernaryValue,
    pub boolConsistentFn: Option<unsafe extern "C" fn(key: GinScanKey) -> bool>,
    pub triConsistentFn: Option<unsafe extern "C" fn(key: GinScanKey) -> GinTernaryValue>,
    pub consistentFmgrInfo: *mut FmgrInfo,
    pub triConsistentFmgrInfo: *mut FmgrInfo,
    pub collation: Oid,

    /* other data needed for calling consistentFn */
    pub query: Datum,
    /* NB: these three arrays have only nuserentries elements! */
    pub queryValues: *mut Datum,
    pub queryCategories: *mut GinNullCategory,
    pub extra_data: *mut Pointer,
    pub strategy: StrategyNumber,
    pub searchMode: int32,
    pub attnum: OffsetNumber,

    /*
     * An excludeOnly scan key is not able to enumerate all matching tuples.
     * That is, to be semantically correct on its own, it would need to have a
     * GIN_CAT_EMPTY_QUERY scanEntry, but it doesn't.  Such a key can still be
     * used to filter tuples returned by other scan keys, so we will get the
     * right answers as long as there's at least one non-excludeOnly scan key
     * for each index attribute considered by the search.  For efficiency
     * reasons we don't want to have unnecessary GIN_CAT_EMPTY_QUERY entries,
     * so we will convert an excludeOnly scan key to non-excludeOnly (by
     * adding a GIN_CAT_EMPTY_QUERY scanEntry) only if there are no other
     * non-excludeOnly scan keys.
     */
    pub excludeOnly: bool,

    /*
     * Match status data.  curItem is the TID most recently tested (could be a
     * lossy-page pointer).  curItemMatches is true if it passes the
     * consistentFn test; if so, recheckCurItem is the recheck flag.
     * isFinished means that all the input entry streams are finished, so this
     * key cannot succeed for any later TIDs.
     */
    pub curItem: ItemPointerData,
    pub curItemMatches: bool,
    pub recheckCurItem: bool,
    pub isFinished: bool,
}

#[repr(C)]
pub struct GinScanEntryData {
    /* query key and other information from extractQueryFn */
    pub queryKey: Datum,
    pub queryCategory: GinNullCategory,
    pub isPartialMatch: bool,
    pub extra_data: Pointer,
    pub strategy: StrategyNumber,
    pub searchMode: int32,
    pub attnum: OffsetNumber,

    /* Current page in posting tree */
    pub buffer: Buffer,

    /* current ItemPointer to heap */
    pub curItem: ItemPointerData,

    /* for a partial-match or full-scan query, we accumulate all TIDs here */
    pub matchBitmap: *mut TIDBitmap,
    pub matchIterator: *mut TBMPrivateIterator,

    /*
     * If blockno is InvalidBlockNumber, all of the other fields in the
     * matchResult are meaningless.
     */
    pub matchResult: TBMIterateResult,
    pub matchOffsets: [OffsetNumber; TBM_MAX_TUPLES_PER_PAGE as usize],
    pub matchNtuples: c_int,

    /* used for Posting list and one page in Posting tree */
    pub list: *mut ItemPointerData,
    pub nlist: c_int,
    pub offset: OffsetNumber,

    pub isFinished: bool,
    pub reduceResult: bool,
    pub predictNumberResult: uint32,
    pub btree: GinBtreeData,
}

#[repr(C)]
pub struct GinScanOpaqueData {
    pub tempCtx: MemoryContext,
    pub ginstate: GinState,

    pub keys: GinScanKey,    /* one per scan qualifier expr */
    pub nkeys: uint32,

    pub entries: *mut GinScanEntry, /* one per index search condition */
    pub totalentries: uint32,
    pub allocentries: uint32, /* allocated length of entries[] */

    pub keyCtx: MemoryContext, /* used to hold key and entry data */

    pub isVoidRes: bool, /* true if query is unsatisfiable */
}

pub type GinScanOpaque = *mut GinScanOpaqueData;

// ---- Local stubs for unported dependencies ----

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

unsafe fn RelationGetIndexScan(rel: Relation, nkeys: c_int, norderbys: c_int) -> IndexScanDesc { crate::access::index::genam::RelationGetIndexScan(rel, nkeys, norderbys) }

unsafe fn AllocSetContextCreateInternal(
    parent: MemoryContext,
    name: *const c_char,
    minContextSize: Size,
    initBlockSize: Size,
    maxBlockSize: Size,
) -> MemoryContext { crate::utils::mmgr::aset::AllocSetContextCreateInternal(parent, name, minContextSize, initBlockSize, maxBlockSize) }

unsafe fn initGinState(state: *mut GinState, index: Relation) { crate::access::gin::ginutil::initGinState(state, index) }

unsafe fn ginCompareEntries(
    ginstate: *mut GinState,
    attnum: OffsetNumber,
    a: Datum,
    categorya: GinNullCategory,
    b: Datum,
    categoryb: GinNullCategory,
) -> c_int { crate::access::gin::ginutil::ginCompareEntries(ginstate, attnum, a, categorya, b, categoryb) }

unsafe fn ginInitConsistentFunction(ginstate: *mut GinState, key: GinScanKey) { crate::access::gin::ginlogic::ginInitConsistentFunction(ginstate, key) }

unsafe fn ginGetStats(index: Relation, stats: *mut GinStatsData) { crate::access::gin::ginutil::ginGetStats(index, stats) }

unsafe fn ReleaseBuffer(buffer: Buffer) {
    unimplemented!() // TODO: storage/buffer/bufmgr.c
}

unsafe fn tbm_end_private_iterate(iterator: *mut TBMPrivateIterator) { crate::nodes::tidbitmap::tbm_end_private_iterate(iterator) }

unsafe fn tbm_free(tbm: *mut TIDBitmap) { crate::nodes::tidbitmap::tbm_free(tbm) }

unsafe fn pgstat_count_index_scan(rel: Relation) {
    unimplemented!() // TODO: pgstat.h
}

unsafe fn FunctionCall7Coll(
    flinfo: *mut FmgrInfo,
    collation: Oid,
    arg1: Datum,
    arg2: Datum,
    arg3: Datum,
    arg4: Datum,
    arg5: Datum,
    arg6: Datum,
    arg7: Datum,
) -> Datum { crate::utils::fmgr::FunctionCall7Coll(flinfo, collation, arg1, arg2, arg3, arg4, arg5, arg6, arg7) }

unsafe fn RelationGetRelationName(relation: Relation) -> *const c_char {
    unimplemented!() // TODO: utils/rel.h
}

// ---- Translated functions ----

#[no_mangle]
pub unsafe extern "C" fn ginbeginscan(
    rel: Relation,
    nkeys: c_int,
    norderbys: c_int,
) -> IndexScanDesc {
    let scan: IndexScanDesc;
    let so: GinScanOpaque;

    /* no order by operators allowed */
    Assert!(norderbys == 0);

    scan = RelationGetIndexScan(rel, nkeys, norderbys);

    /* allocate private workspace */
    so = palloc(size_of::<GinScanOpaqueData>()) as GinScanOpaque;
    (*so).keys = std::ptr::null_mut();
    (*so).nkeys = 0;
    (*so).tempCtx = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"Gin scan temporary context".as_ptr(),
        ALLOCSET_DEFAULT_SIZES!(),
    );
    (*so).keyCtx = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"Gin scan key context".as_ptr(),
        ALLOCSET_DEFAULT_SIZES!(),
    );
    initGinState(&mut (*so).ginstate, (*scan).indexRelation);

    (*scan).opaque = so as *mut c_void;

    scan
}

/*
 * Create a new GinScanEntry, unless an equivalent one already exists,
 * in which case just return it
 */
unsafe fn ginFillScanEntry(
    so: GinScanOpaque,
    attnum: OffsetNumber,
    strategy: StrategyNumber,
    searchMode: int32,
    queryKey: Datum,
    queryCategory: GinNullCategory,
    isPartialMatch: bool,
    extra_data: Pointer,
) -> GinScanEntry {
    let ginstate: *mut GinState = &mut (*so).ginstate;
    let scanEntry: GinScanEntry;
    let mut i: uint32;

    /*
     * Look for an existing equivalent entry.
     *
     * Entries with non-null extra_data are never considered identical, since
     * we can't know exactly what the opclass might be doing with that.
     *
     * Also, give up de-duplication once we have 100 entries.  That avoids
     * spending O(N^2) time on probably-fruitless de-duplication of large
     * search-key sets.  The threshold of 100 is arbitrary but matches
     * predtest.c's threshold for what's a large array.
     */
    if extra_data.is_null() && (*so).totalentries < 100 {
        i = 0;
        while i < (*so).totalentries {
            let prevEntry: GinScanEntry = *(*so).entries.add(i as usize);

            if (*prevEntry).extra_data.is_null()
                && (*prevEntry).isPartialMatch == isPartialMatch
                && (*prevEntry).strategy == strategy
                && (*prevEntry).searchMode == searchMode
                && (*prevEntry).attnum == attnum
                && ginCompareEntries(
                    ginstate,
                    attnum,
                    (*prevEntry).queryKey,
                    (*prevEntry).queryCategory,
                    queryKey,
                    queryCategory,
                ) == 0
            {
                /* Successful match */
                return prevEntry;
            }
            i += 1;
        }
    }

    /* Nope, create a new entry */
    scanEntry = palloc(size_of::<GinScanEntryData>()) as GinScanEntry;
    (*scanEntry).queryKey = queryKey;
    (*scanEntry).queryCategory = queryCategory;
    (*scanEntry).isPartialMatch = isPartialMatch;
    (*scanEntry).extra_data = extra_data;
    (*scanEntry).strategy = strategy;
    (*scanEntry).searchMode = searchMode;
    (*scanEntry).attnum = attnum;

    (*scanEntry).buffer = InvalidBuffer as Buffer;
    ItemPointerSetMin(&mut (*scanEntry).curItem);
    (*scanEntry).matchBitmap = std::ptr::null_mut();
    (*scanEntry).matchIterator = std::ptr::null_mut();
    (*scanEntry).matchResult.blockno = InvalidBlockNumber;
    (*scanEntry).matchNtuples = -1;
    (*scanEntry).list = std::ptr::null_mut();
    (*scanEntry).nlist = 0;
    (*scanEntry).offset = InvalidOffsetNumber;
    (*scanEntry).isFinished = false;
    (*scanEntry).reduceResult = false;

    /* Add it to so's array */
    if (*so).totalentries >= (*so).allocentries {
        (*so).allocentries *= 2;
        (*so).entries = repalloc(
            (*so).entries as *mut c_void,
            (*so).allocentries as usize * size_of::<GinScanEntry>(),
        ) as *mut GinScanEntry;
    }
    *(*so).entries.add((*so).totalentries as usize) = scanEntry;
    (*so).totalentries += 1;

    scanEntry
}

/*
 * Append hidden scan entry of given category to the scan key.
 *
 * NB: this had better be called at most once per scan key, since
 * ginFillScanKey leaves room for only one hidden entry.  Currently,
 * it seems sufficiently clear that this is true that we don't bother
 * with any cross-check logic.
 */
unsafe fn ginScanKeyAddHiddenEntry(
    so: GinScanOpaque,
    key: GinScanKey,
    queryCategory: GinNullCategory,
) {
    let i: c_int = (*key).nentries as c_int;
    (*key).nentries += 1;

    /* strategy is of no interest because this is not a partial-match item */
    *(*key).scanEntry.add(i as usize) = ginFillScanEntry(
        so,
        (*key).attnum,
        InvalidStrategy as StrategyNumber,
        (*key).searchMode,
        0 as Datum,
        queryCategory,
        false,
        std::ptr::null_mut(),
    );
}

/*
 * Initialize the next GinScanKey using the output from the extractQueryFn
 */
unsafe fn ginFillScanKey(
    so: GinScanOpaque,
    attnum: OffsetNumber,
    strategy: StrategyNumber,
    searchMode: int32,
    query: Datum,
    nQueryValues: uint32,
    queryValues: *mut Datum,
    queryCategories: *mut GinNullCategory,
    partial_matches: *mut bool,
    extra_data: *mut Pointer,
) {
    let key: GinScanKey = (*so).keys.add((*so).nkeys as usize);
    (*so).nkeys += 1;
    let ginstate: *mut GinState = &mut (*so).ginstate;
    let mut i: uint32;

    (*key).nentries = nQueryValues;
    (*key).nuserentries = nQueryValues;

    /* Allocate one extra array slot for possible "hidden" entry */
    (*key).scanEntry =
        palloc(size_of::<GinScanEntry>() * (nQueryValues + 1) as usize) as *mut GinScanEntry;
    (*key).entryRes =
        palloc0(size_of::<GinTernaryValue>() * (nQueryValues + 1) as usize) as *mut GinTernaryValue;

    (*key).query = query;
    (*key).queryValues = queryValues;
    (*key).queryCategories = queryCategories;
    (*key).extra_data = extra_data;
    (*key).strategy = strategy;
    (*key).searchMode = searchMode;
    (*key).attnum = attnum;

    /*
     * Initially, scan keys of GIN_SEARCH_MODE_ALL mode are marked
     * excludeOnly.  This might get changed later.
     */
    (*key).excludeOnly = searchMode == GIN_SEARCH_MODE_ALL;

    ItemPointerSetMin(&mut (*key).curItem);
    (*key).curItemMatches = false;
    (*key).recheckCurItem = false;
    (*key).isFinished = false;
    (*key).nrequired = 0;
    (*key).nadditional = 0;
    (*key).requiredEntries = std::ptr::null_mut();
    (*key).additionalEntries = std::ptr::null_mut();

    ginInitConsistentFunction(ginstate, key);

    /* Set up normal scan entries using extractQueryFn's outputs */
    i = 0;
    while i < nQueryValues {
        let queryKey: Datum;
        let queryCategory: GinNullCategory;
        let isPartialMatch: bool;
        let this_extra: Pointer;

        queryKey = *queryValues.add(i as usize);
        queryCategory = *queryCategories.add(i as usize);
        isPartialMatch = if (*ginstate).canPartialMatch[(attnum - 1) as usize]
            && !partial_matches.is_null()
        {
            *partial_matches.add(i as usize)
        } else {
            false
        };
        this_extra = if !extra_data.is_null() {
            *extra_data.add(i as usize)
        } else {
            std::ptr::null_mut()
        };

        *(*key).scanEntry.add(i as usize) = ginFillScanEntry(
            so,
            attnum,
            strategy,
            searchMode,
            queryKey,
            queryCategory,
            isPartialMatch,
            this_extra,
        );
        i += 1;
    }

    /*
     * For GIN_SEARCH_MODE_INCLUDE_EMPTY and GIN_SEARCH_MODE_EVERYTHING search
     * modes, we add the "hidden" entry immediately.  GIN_SEARCH_MODE_ALL is
     * handled later, since we might be able to omit the hidden entry for it.
     */
    if searchMode == GIN_SEARCH_MODE_INCLUDE_EMPTY {
        ginScanKeyAddHiddenEntry(so, key, GIN_CAT_EMPTY_ITEM as GinNullCategory);
    } else if searchMode == GIN_SEARCH_MODE_EVERYTHING {
        ginScanKeyAddHiddenEntry(so, key, GIN_CAT_EMPTY_QUERY as GinNullCategory);
    }
}

/*
 * Release current scan keys, if any.
 */
#[no_mangle]
pub unsafe extern "C" fn ginFreeScanKeys(so: GinScanOpaque) {
    let mut i: uint32;

    if (*so).keys.is_null() {
        return;
    }

    i = 0;
    while i < (*so).totalentries {
        let entry: GinScanEntry = *(*so).entries.add(i as usize);

        if (*entry).buffer != InvalidBuffer as Buffer {
            ReleaseBuffer((*entry).buffer);
        }
        if !(*entry).list.is_null() {
            pfree((*entry).list as *mut c_void);
        }
        if !(*entry).matchIterator.is_null() {
            tbm_end_private_iterate((*entry).matchIterator);
        }
        if !(*entry).matchBitmap.is_null() {
            tbm_free((*entry).matchBitmap);
        }
        i += 1;
    }

    MemoryContextReset((*so).keyCtx);

    (*so).keys = std::ptr::null_mut();
    (*so).nkeys = 0;
    (*so).entries = std::ptr::null_mut();
    (*so).totalentries = 0;
}

#[no_mangle]
pub unsafe extern "C" fn ginNewScanKey(scan: IndexScanDesc) {
    let scankey: ScanKey = (*scan).keyData as ScanKey;
    let so: GinScanOpaque = (*scan).opaque as GinScanOpaque;
    let mut i: c_int;
    let mut numExcludeOnly: c_int;
    let mut hasNullQuery: bool = false;
    let mut attrHasNormalScan: [bool; INDEX_MAX_KEYS as usize] =
        [false; INDEX_MAX_KEYS as usize];
    let oldCtx: MemoryContext;

    /*
     * Allocate all the scan key information in the key context. (If
     * extractQuery leaks anything there, it won't be reset until the end of
     * scan or rescan, but that's OK.)
     */
    oldCtx = MemoryContextSwitchTo((*so).keyCtx);

    /* if no scan keys provided, allocate extra EVERYTHING GinScanKey */
    (*so).keys = palloc(
        Max!((*scan).numberOfKeys as c_int, 1) as usize * size_of::<GinScanKeyData>(),
    ) as GinScanKey;
    (*so).nkeys = 0;

    /* initialize expansible array of GinScanEntry pointers */
    (*so).totalentries = 0;
    (*so).allocentries = 32;
    (*so).entries =
        palloc((*so).allocentries as usize * size_of::<GinScanEntry>()) as *mut GinScanEntry;

    (*so).isVoidRes = false;

    i = 0;
    while i < (*scan).numberOfKeys as c_int {
        let skey: ScanKey = scankey.add(i as usize);
        let queryValues: *mut Datum;
        let mut nQueryValues: int32 = 0;
        let mut partial_matches: *mut bool = std::ptr::null_mut();
        let mut extra_data: *mut Pointer = std::ptr::null_mut();
        let mut nullFlags: *mut bool = std::ptr::null_mut();
        let categories: *mut GinNullCategory;
        let mut searchMode: int32 = GIN_SEARCH_MODE_DEFAULT;

        /*
         * We assume that GIN-indexable operators are strict, so a null query
         * argument means an unsatisfiable query.
         */
        if (*skey).sk_flags & SK_ISNULL as c_int != 0 {
            (*so).isVoidRes = true;
            break;
        }

        /* OK to call the extractQueryFn */
        queryValues = DatumGetPointer(FunctionCall7Coll(
            &mut (*so).ginstate.extractQueryFn[((*skey).sk_attno - 1) as usize],
            (*so).ginstate.supportCollation[((*skey).sk_attno - 1) as usize],
            (*skey).sk_argument,
            PointerGetDatum(&mut nQueryValues as *mut int32 as *const c_void),
            UInt16GetDatum((*skey).sk_strategy),
            PointerGetDatum(&mut partial_matches as *mut *mut bool as *const c_void),
            PointerGetDatum(&mut extra_data as *mut *mut Pointer as *const c_void),
            PointerGetDatum(&mut nullFlags as *mut *mut bool as *const c_void),
            PointerGetDatum(&mut searchMode as *mut int32 as *const c_void),
        )) as *mut Datum;

        /*
         * If bogus searchMode is returned, treat as GIN_SEARCH_MODE_ALL; note
         * in particular we don't allow extractQueryFn to select
         * GIN_SEARCH_MODE_EVERYTHING.
         */
        if searchMode < GIN_SEARCH_MODE_DEFAULT || searchMode > GIN_SEARCH_MODE_ALL {
            searchMode = GIN_SEARCH_MODE_ALL;
        }

        /* Non-default modes require the index to have placeholders */
        if searchMode != GIN_SEARCH_MODE_DEFAULT {
            hasNullQuery = true;
        }

        /*
         * In default mode, no keys means an unsatisfiable query.
         */
        if queryValues.is_null() || nQueryValues <= 0 {
            if searchMode == GIN_SEARCH_MODE_DEFAULT {
                (*so).isVoidRes = true;
                break;
            }
            nQueryValues = 0; /* ensure sane value */
        }

        /*
         * Create GinNullCategory representation.  If the extractQueryFn
         * didn't create a nullFlags array, we assume everything is non-null.
         * While at it, detect whether any null keys are present.
         */
        categories = palloc0(nQueryValues as usize * size_of::<GinNullCategory>())
            as *mut GinNullCategory;
        if !nullFlags.is_null() {
            let mut j: int32;

            j = 0;
            while j < nQueryValues {
                if *nullFlags.add(j as usize) {
                    *categories.add(j as usize) = GIN_CAT_NULL_KEY as GinNullCategory;
                    hasNullQuery = true;
                }
                j += 1;
            }
        }

        ginFillScanKey(
            so,
            (*skey).sk_attno,
            (*skey).sk_strategy,
            searchMode,
            (*skey).sk_argument,
            nQueryValues as uint32,
            queryValues,
            categories,
            partial_matches,
            extra_data,
        );

        /* Remember if we had any non-excludeOnly keys */
        if searchMode != GIN_SEARCH_MODE_ALL {
            attrHasNormalScan[((*skey).sk_attno - 1) as usize] = true;
        }

        i += 1;
    }

    /*
     * Processing GIN_SEARCH_MODE_ALL scan keys requires us to make a second
     * pass over the scan keys.  Above we marked each such scan key as
     * excludeOnly.  If the involved column has any normal (not excludeOnly)
     * scan key as well, then we can leave it like that.  Otherwise, one
     * excludeOnly scan key must receive a GIN_CAT_EMPTY_QUERY hidden entry
     * and be set to normal (excludeOnly = false).
     */
    numExcludeOnly = 0;
    i = 0;
    while (i as uint32) < (*so).nkeys {
        let key: GinScanKey = (*so).keys.add(i as usize);

        if (*key).searchMode != GIN_SEARCH_MODE_ALL {
            i += 1;
            continue;
        }

        if !attrHasNormalScan[((*key).attnum - 1) as usize] {
            (*key).excludeOnly = false;
            ginScanKeyAddHiddenEntry(so, key, GIN_CAT_EMPTY_QUERY as GinNullCategory);
            attrHasNormalScan[((*key).attnum - 1) as usize] = true;
        } else {
            numExcludeOnly += 1;
        }
        i += 1;
    }

    /*
     * If we left any excludeOnly scan keys as-is, move them to the end of the
     * scan key array: they must appear after normal key(s).
     */
    if numExcludeOnly > 0 {
        let tmpkeys: GinScanKey;
        let mut iNormalKey: c_int;
        let mut iExcludeOnly: c_int;

        /* We'd better have made at least one normal key */
        Assert!((numExcludeOnly as uint32) < (*so).nkeys);
        /* Make a temporary array to hold the re-ordered scan keys */
        tmpkeys = palloc((*so).nkeys as usize * size_of::<GinScanKeyData>()) as GinScanKey;
        /* Re-order the keys ... */
        iNormalKey = 0;
        iExcludeOnly = (*so).nkeys as c_int - numExcludeOnly;
        i = 0;
        while (i as uint32) < (*so).nkeys {
            let key: GinScanKey = (*so).keys.add(i as usize);

            if (*key).excludeOnly {
                memcpy(
                    tmpkeys.add(iExcludeOnly as usize) as *mut c_void,
                    key as *const c_void,
                    size_of::<GinScanKeyData>(),
                );
                iExcludeOnly += 1;
            } else {
                memcpy(
                    tmpkeys.add(iNormalKey as usize) as *mut c_void,
                    key as *const c_void,
                    size_of::<GinScanKeyData>(),
                );
                iNormalKey += 1;
            }
            i += 1;
        }
        Assert!(iNormalKey == (*so).nkeys as c_int - numExcludeOnly);
        Assert!(iExcludeOnly == (*so).nkeys as c_int);
        /* ... and copy them back to so->keys[] */
        memcpy(
            (*so).keys as *mut c_void,
            tmpkeys as *const c_void,
            (*so).nkeys as usize * size_of::<GinScanKeyData>(),
        );
        pfree(tmpkeys as *mut c_void);
    }

    /*
     * If there are no regular scan keys, generate an EVERYTHING scankey to
     * drive a full-index scan.
     */
    if (*so).nkeys == 0 && !(*so).isVoidRes {
        hasNullQuery = true;
        ginFillScanKey(
            so,
            FirstOffsetNumber as OffsetNumber,
            InvalidStrategy as StrategyNumber,
            GIN_SEARCH_MODE_EVERYTHING,
            0 as Datum,
            0,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        );
    }

    /*
     * If the index is version 0, it may be missing null and placeholder
     * entries, which would render searches for nulls and full-index scans
     * unreliable.  Throw an error if so.
     */
    if hasNullQuery && !(*so).isVoidRes {
        let mut ginStats: GinStatsData = std::mem::zeroed();

        ginGetStats((*scan).indexRelation, &mut ginStats);
        if ginStats.ginVersion < 1 {
            elog!(
                ERROR,
                "old GIN indexes do not support whole-index scans nor searches for nulls"
            );
            unreachable!();
        }
    }

    MemoryContextSwitchTo(oldCtx);

    pgstat_count_index_scan((*scan).indexRelation);
    if !(*scan).instrument.is_null() {
        (*(*scan).instrument).nsearches += 1;
    }
}

#[no_mangle]
pub unsafe extern "C" fn ginrescan(
    scan: IndexScanDesc,
    scankey: ScanKey,
    nscankeys: c_int,
    orderbys: ScanKey,
    norderbys: c_int,
) {
    let so: GinScanOpaque = (*scan).opaque as GinScanOpaque;

    ginFreeScanKeys(so);

    if !scankey.is_null() && (*scan).numberOfKeys > 0 {
        memcpy(
            (*scan).keyData as *mut c_void,
            scankey as *const c_void,
            (*scan).numberOfKeys as usize * size_of::<ScanKeyData>(),
        );
    }
}

#[no_mangle]
pub unsafe extern "C" fn ginendscan(scan: IndexScanDesc) {
    let so: GinScanOpaque = (*scan).opaque as GinScanOpaque;

    ginFreeScanKeys(so);

    MemoryContextDelete((*so).tempCtx);
    MemoryContextDelete((*so).keyCtx);

    pfree(so as *mut c_void);
}
