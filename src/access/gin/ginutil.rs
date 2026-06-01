//! src/backend/access/gin/ginutil.c
//!
//! Utility routines for the Postgres inverted index access method.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;
use crate::{list_make1, list_make2};
use crate::storage::block::{BlockNumber, InvalidBlockNumber};
use crate::access::attnum::AttrNumber;
use crate::access::transam::xlogdefs::XLogRecPtr;

// makeNode! is a #[macro_export] macro living at the crate root.
use crate::makeNode;
// FunctionCallInfo is the fmgr call interface pointer.
use crate::utils::fmgr::FunctionCallInfo;

use std::ffi::{c_int, c_void};

/*
 * GIN handler function: return IndexAmRoutine with access method parameters
 * and callbacks.
 */
#[allow(non_snake_case)]
pub unsafe fn ginhandler(fcinfo: FunctionCallInfo) -> Datum {
    let amroutine: *mut IndexAmRoutine = makeNode!(IndexAmRoutine, T_IndexAmRoutine);

    (*amroutine).amstrategies = 0;
    (*amroutine).amsupport = GINNProcs;
    (*amroutine).amoptsprocnum = GIN_OPTIONS_PROC;
    (*amroutine).amcanorder = false;
    (*amroutine).amcanorderbyop = false;
    (*amroutine).amcanhash = false;
    (*amroutine).amconsistentequality = false;
    (*amroutine).amconsistentordering = false;
    (*amroutine).amcanbackward = false;
    (*amroutine).amcanunique = false;
    (*amroutine).amcanmulticol = true;
    (*amroutine).amoptionalkey = true;
    (*amroutine).amsearcharray = false;
    (*amroutine).amsearchnulls = false;
    (*amroutine).amstorage = true;
    (*amroutine).amclusterable = false;
    (*amroutine).ampredlocks = true;
    (*amroutine).amcanparallel = false;
    (*amroutine).amcanbuildparallel = true;
    (*amroutine).amcaninclude = false;
    (*amroutine).amusemaintenanceworkmem = true;
    (*amroutine).amsummarizing = false;
    (*amroutine).amparallelvacuumoptions =
        VACUUM_OPTION_PARALLEL_BULKDEL | VACUUM_OPTION_PARALLEL_CLEANUP;
    (*amroutine).amkeytype = InvalidOid;

    (*amroutine).ambuild = Some(ginbuild);
    (*amroutine).ambuildempty = Some(ginbuildempty);
    (*amroutine).aminsert = Some(gininsert);
    (*amroutine).aminsertcleanup = None;
    (*amroutine).ambulkdelete = Some(ginbulkdelete);
    (*amroutine).amvacuumcleanup = Some(ginvacuumcleanup);
    (*amroutine).amcanreturn = None;
    (*amroutine).amcostestimate = Some(gincostestimate);
    (*amroutine).amgettreeheight = None;
    (*amroutine).amoptions = Some(ginoptions);
    (*amroutine).amproperty = None;
    (*amroutine).ambuildphasename = Some(ginbuildphasename);
    (*amroutine).amvalidate = Some(ginvalidate);
    (*amroutine).amadjustmembers = Some(ginadjustmembers);
    (*amroutine).ambeginscan = Some(ginbeginscan);
    (*amroutine).amrescan = Some(ginrescan);
    (*amroutine).amgettuple = None;
    (*amroutine).amgetbitmap = Some(gingetbitmap);
    (*amroutine).amendscan = Some(ginendscan);
    (*amroutine).ammarkpos = None;
    (*amroutine).amrestrpos = None;
    (*amroutine).amestimateparallelscan = None;
    (*amroutine).aminitparallelscan = None;
    (*amroutine).amparallelrescan = None;

    PointerGetDatum(amroutine as *mut c_void)
}

/*
 * initGinState: fill in an empty GinState struct to describe the index
 *
 * Note: assorted subsidiary data is allocated in the CurrentMemoryContext.
 */
#[allow(non_snake_case)]
pub unsafe fn initGinState(state: *mut GinState, index: Relation) {
    let origTupdesc: TupleDesc = RelationGetDescr(index);

    MemSet(state as *mut c_void, 0, std::mem::size_of::<GinState>());

    (*state).index = index;
    (*state).oneCol = (*origTupdesc).natts == 1;
    (*state).origTupdesc = origTupdesc;

    let mut i: c_int = 0;
    while i < (*origTupdesc).natts {
        let attr: Form_pg_attribute = TupleDescAttr(origTupdesc, i);

        if (*state).oneCol {
            (*state).tupdesc[i as usize] = (*state).origTupdesc;
        } else {
            (*state).tupdesc[i as usize] = CreateTemplateTupleDesc(2);

            TupleDescInitEntry(
                (*state).tupdesc[i as usize],
                1 as AttrNumber,
                std::ptr::null(),
                INT2OID,
                -1,
                0,
            );
            TupleDescInitEntry(
                (*state).tupdesc[i as usize],
                2 as AttrNumber,
                std::ptr::null(),
                (*attr).atttypid,
                (*attr).atttypmod,
                (*attr).attndims as c_int,
            );
            TupleDescInitEntryCollation(
                (*state).tupdesc[i as usize],
                2 as AttrNumber,
                (*attr).attcollation,
            );
        }

        /*
         * If the compare proc isn't specified in the opclass definition, look
         * up the index key type's default btree comparator.
         */
        if index_getprocid(index, i + 1, GIN_COMPARE_PROC) != InvalidOid {
            fmgr_info_copy(
                &mut (*state).compareFn[i as usize],
                index_getprocinfo(index, i + 1, GIN_COMPARE_PROC),
                CurrentMemoryContext,
            );
        } else {
            let typentry: *mut TypeCacheEntry =
                lookup_type_cache((*attr).atttypid, TYPECACHE_CMP_PROC_FINFO);
            if !OidIsValid((*typentry).cmp_proc_finfo.fn_oid) {
                ereport!(
                    ERROR,
                    "could not identify a comparison function for type"
                );
            }
            fmgr_info_copy(
                &mut (*state).compareFn[i as usize],
                &mut (*typentry).cmp_proc_finfo,
                CurrentMemoryContext,
            );
        }

        /* Opclass must always provide extract procs */
        fmgr_info_copy(
            &mut (*state).extractValueFn[i as usize],
            index_getprocinfo(index, i + 1, GIN_EXTRACTVALUE_PROC),
            CurrentMemoryContext,
        );
        fmgr_info_copy(
            &mut (*state).extractQueryFn[i as usize],
            index_getprocinfo(index, i + 1, GIN_EXTRACTQUERY_PROC),
            CurrentMemoryContext,
        );

        /*
         * Check opclass capability to do tri-state or binary logic consistent
         * check.
         */
        if index_getprocid(index, i + 1, GIN_TRICONSISTENT_PROC) != InvalidOid {
            fmgr_info_copy(
                &mut (*state).triConsistentFn[i as usize],
                index_getprocinfo(index, i + 1, GIN_TRICONSISTENT_PROC),
                CurrentMemoryContext,
            );
        }

        if index_getprocid(index, i + 1, GIN_CONSISTENT_PROC) != InvalidOid {
            fmgr_info_copy(
                &mut (*state).consistentFn[i as usize],
                index_getprocinfo(index, i + 1, GIN_CONSISTENT_PROC),
                CurrentMemoryContext,
            );
        }

        if (*state).consistentFn[i as usize].fn_oid == InvalidOid
            && (*state).triConsistentFn[i as usize].fn_oid == InvalidOid
        {
            elog!(
                ERROR,
                "missing GIN support function ({} or {}) for attribute {} of index \"{}\"",
                GIN_CONSISTENT_PROC,
                GIN_TRICONSISTENT_PROC,
                i + 1,
                CStr_to_str(RelationGetRelationName(index))
            );
        }

        /*
         * Check opclass capability to do partial match.
         */
        if index_getprocid(index, i + 1, GIN_COMPARE_PARTIAL_PROC) != InvalidOid {
            fmgr_info_copy(
                &mut (*state).comparePartialFn[i as usize],
                index_getprocinfo(index, i + 1, GIN_COMPARE_PARTIAL_PROC),
                CurrentMemoryContext,
            );
            (*state).canPartialMatch[i as usize] = true;
        } else {
            (*state).canPartialMatch[i as usize] = false;
        }

        /*
         * If the index column has a specified collation, we should honor that
         * while doing comparisons.  However, we may have a collatable storage
         * type for a noncollatable indexed data type (for instance, hstore
         * uses text index entries).  If there's no index collation then
         * specify default collation in case the support functions need
         * collation.  This is harmless if the support functions don't care
         * about collation, so we just do it unconditionally.  (We could
         * alternatively call get_typcollation, but that seems like expensive
         * overkill --- there aren't going to be any cases where a GIN storage
         * type has a nondefault collation.)
         */
        if OidIsValid(*(*index).rd_indcollation.add(i as usize)) {
            (*state).supportCollation[i as usize] = *(*index).rd_indcollation.add(i as usize);
        } else {
            (*state).supportCollation[i as usize] = DEFAULT_COLLATION_OID;
        }

        i += 1;
    }
}

/*
 * Extract attribute (column) number of stored entry from GIN tuple
 */
#[allow(non_snake_case)]
pub unsafe fn gintuple_get_attrnum(ginstate: *mut GinState, tuple: IndexTuple) -> OffsetNumber {
    let colN: OffsetNumber;

    if (*ginstate).oneCol {
        /* column number is not stored explicitly */
        colN = FirstOffsetNumber;
    } else {
        let mut isnull: bool = false;

        /*
         * First attribute is always int16, so we can safely use any tuple
         * descriptor to obtain first attribute of tuple
         */
        let res: Datum = index_getattr(
            tuple,
            FirstOffsetNumber,
            (*ginstate).tupdesc[0],
            &mut isnull,
        );
        Assert!(!isnull);

        colN = DatumGetUInt16(res);
        Assert!(colN >= FirstOffsetNumber && colN <= (*(*ginstate).origTupdesc).natts as OffsetNumber);
    }

    colN
}

/*
 * Extract stored datum (and possible null category) from GIN tuple
 */
#[allow(non_snake_case)]
pub unsafe fn gintuple_get_key(
    ginstate: *mut GinState,
    tuple: IndexTuple,
    category: *mut GinNullCategory,
) -> Datum {
    let res: Datum;
    let mut isnull: bool = false;

    if (*ginstate).oneCol {
        /*
         * Single column index doesn't store attribute numbers in tuples
         */
        res = index_getattr(
            tuple,
            FirstOffsetNumber,
            (*ginstate).origTupdesc,
            &mut isnull,
        );
    } else {
        /*
         * Since the datum type depends on which index column it's from, we
         * must be careful to use the right tuple descriptor here.
         */
        let colN: OffsetNumber = gintuple_get_attrnum(ginstate, tuple);

        res = index_getattr(
            tuple,
            OffsetNumberNext(FirstOffsetNumber),
            (*ginstate).tupdesc[(colN - 1) as usize],
            &mut isnull,
        );
    }

    if isnull {
        *category = GinGetNullCategory(tuple, ginstate);
    } else {
        *category = GIN_CAT_NORM_KEY as GinNullCategory;
    }

    res
}

/*
 * Allocate a new page (either by recycling, or by extending the index file)
 * The returned buffer is already pinned and exclusive-locked
 * Caller is responsible for initializing the page by calling GinInitBuffer
 */
#[allow(non_snake_case)]
pub unsafe fn GinNewBuffer(index: Relation) -> Buffer {
    let mut buffer: Buffer;

    /* First, try to get a page from FSM */
    loop {
        let blkno: BlockNumber = GetFreeIndexPage(index);

        if blkno == InvalidBlockNumber {
            break;
        }

        buffer = ReadBuffer(index, blkno);

        /*
         * We have to guard against the possibility that someone else already
         * recycled this page; the buffer may be locked if so.
         */
        if ConditionalLockBuffer(buffer) {
            if GinPageIsRecyclable(BufferGetPage(buffer)) {
                return buffer; /* OK to use */
            }

            LockBuffer(buffer, GIN_UNLOCK);
        }

        /* Can't use it, so release buffer and try again */
        ReleaseBuffer(buffer);
    }

    /* Must extend the file */
    buffer = ExtendBufferedRel(
        BMR_REL(index),
        MAIN_FORKNUM,
        std::ptr::null_mut(),
        EB_LOCK_FIRST,
    );

    buffer
}

#[allow(non_snake_case)]
pub unsafe fn GinInitPage(page: Page, f: uint32, pageSize: Size) {
    PageInit(page, pageSize, std::mem::size_of::<GinPageOpaqueData>());

    let opaque: GinPageOpaque = GinPageGetOpaque(page);
    (*opaque).flags = f;
    (*opaque).rightlink = InvalidBlockNumber;
}

#[allow(non_snake_case)]
pub unsafe fn GinInitBuffer(b: Buffer, f: uint32) {
    GinInitPage(BufferGetPage(b), f, BufferGetPageSize(b));
}

#[allow(non_snake_case)]
pub unsafe fn GinInitMetabuffer(b: Buffer) {
    let page: Page = BufferGetPage(b);

    GinInitPage(page, GIN_META, BufferGetPageSize(b));

    let metadata: *mut GinMetaPageData = GinPageGetMeta(page);

    (*metadata).tail = InvalidBlockNumber;
    (*metadata).head = InvalidBlockNumber;
    (*metadata).tailFreeSize = 0;
    (*metadata).nPendingPages = 0;
    (*metadata).nPendingHeapTuples = 0;
    (*metadata).nTotalPages = 0;
    (*metadata).nEntryPages = 0;
    (*metadata).nDataPages = 0;
    (*metadata).nEntries = 0;
    (*metadata).ginVersion = GIN_CURRENT_VERSION;

    /*
     * Set pd_lower just past the end of the metadata.  This is essential,
     * because without doing so, metadata will be lost if xlog.c compresses
     * the page.
     */
    (*(page as PageHeader)).pd_lower =
        ((metadata as *mut c_char).add(std::mem::size_of::<GinMetaPageData>()) as isize
            - page as isize) as u16;
}

/*
 * Compare two keys of the same index column
 */
#[allow(non_snake_case)]
pub unsafe fn ginCompareEntries(
    ginstate: *mut GinState,
    attnum: OffsetNumber,
    a: Datum,
    categorya: GinNullCategory,
    b: Datum,
    categoryb: GinNullCategory,
) -> c_int {
    /* if not of same null category, sort by that first */
    if categorya != categoryb {
        return if categorya < categoryb { -1 } else { 1 };
    }

    /* all null items in same category are equal */
    if categorya != GIN_CAT_NORM_KEY as GinNullCategory {
        return 0;
    }

    /* both not null, so safe to call the compareFn */
    DatumGetInt32(FunctionCall2Coll(
        &mut (*ginstate).compareFn[(attnum - 1) as usize],
        (*ginstate).supportCollation[(attnum - 1) as usize],
        a,
        b,
    ))
}

/*
 * Compare two keys of possibly different index columns
 */
#[allow(non_snake_case)]
pub unsafe fn ginCompareAttEntries(
    ginstate: *mut GinState,
    attnuma: OffsetNumber,
    a: Datum,
    categorya: GinNullCategory,
    attnumb: OffsetNumber,
    b: Datum,
    categoryb: GinNullCategory,
) -> c_int {
    /* attribute number is the first sort key */
    if attnuma != attnumb {
        return if attnuma < attnumb { -1 } else { 1 };
    }

    ginCompareEntries(ginstate, attnuma, a, categorya, b, categoryb)
}

/*
 * Support for sorting key datums in ginExtractEntries
 *
 * Note: we only have to worry about null and not-null keys here;
 * ginExtractEntries never generates more than one placeholder null,
 * so it doesn't have to sort those.
 */
#[repr(C)]
struct keyEntryData {
    datum: Datum,
    isnull: bool,
}

#[repr(C)]
struct cmpEntriesArg {
    cmpDatumFunc: *mut FmgrInfo,
    collation: Oid,
    haveDups: bool,
}

unsafe extern "C" fn cmpEntries(a: *const c_void, b: *const c_void, arg: *mut c_void) -> c_int {
    let aa: *const keyEntryData = a as *const keyEntryData;
    let bb: *const keyEntryData = b as *const keyEntryData;
    let data: *mut cmpEntriesArg = arg as *mut cmpEntriesArg;
    let res: c_int;

    if (*aa).isnull {
        if (*bb).isnull {
            res = 0; /* NULL "=" NULL */
        } else {
            res = 1; /* NULL ">" not-NULL */
        }
    } else if (*bb).isnull {
        res = -1; /* not-NULL "<" NULL */
    } else {
        res = DatumGetInt32(FunctionCall2Coll(
            (*data).cmpDatumFunc,
            (*data).collation,
            (*aa).datum,
            (*bb).datum,
        ));
    }

    /*
     * Detect if we have any duplicates.  If there are equal keys, qsort must
     * compare them at some point, else it wouldn't know whether one should go
     * before or after the other.
     */
    if res == 0 {
        (*data).haveDups = true;
    }

    res
}

/*
 * Extract the index key values from an indexable item
 *
 * The resulting key values are sorted, and any duplicates are removed.
 * This avoids generating redundant index entries.
 */
#[allow(non_snake_case)]
pub unsafe fn ginExtractEntries(
    ginstate: *mut GinState,
    attnum: OffsetNumber,
    value: Datum,
    isNull: bool,
    nentries: *mut int32,
    categories: *mut *mut GinNullCategory,
) -> *mut Datum {
    let mut entries: *mut Datum;
    let mut nullFlags: *mut bool;
    let mut i: int32;

    /*
     * We don't call the extractValueFn on a null item.  Instead generate a
     * placeholder.
     */
    if isNull {
        *nentries = 1;
        entries = palloc(std::mem::size_of::<Datum>()) as *mut Datum;
        *entries.add(0) = 0 as Datum;
        *categories = palloc(std::mem::size_of::<GinNullCategory>()) as *mut GinNullCategory;
        *(*categories).add(0) = GIN_CAT_NULL_ITEM as GinNullCategory;
        return entries;
    }

    /* OK, call the opclass's extractValueFn */
    nullFlags = std::ptr::null_mut(); /* in case extractValue doesn't set it */
    entries = DatumGetPointer(FunctionCall3Coll(
        &mut (*ginstate).extractValueFn[(attnum - 1) as usize],
        (*ginstate).supportCollation[(attnum - 1) as usize],
        value,
        PointerGetDatum(nentries as *mut c_void),
        PointerGetDatum(&mut nullFlags as *mut *mut bool as *mut c_void),
    )) as *mut Datum;

    /*
     * Generate a placeholder if the item contained no keys.
     */
    if entries.is_null() || *nentries <= 0 {
        *nentries = 1;
        entries = palloc(std::mem::size_of::<Datum>()) as *mut Datum;
        *entries.add(0) = 0 as Datum;
        *categories = palloc(std::mem::size_of::<GinNullCategory>()) as *mut GinNullCategory;
        *(*categories).add(0) = GIN_CAT_EMPTY_ITEM as GinNullCategory;
        return entries;
    }

    /*
     * If the extractValueFn didn't create a nullFlags array, create one,
     * assuming that everything's non-null.
     */
    if nullFlags.is_null() {
        nullFlags = palloc0((*nentries as usize) * std::mem::size_of::<bool>()) as *mut bool;
    }

    /*
     * If there's more than one key, sort and unique-ify.
     *
     * XXX Using qsort here is notationally painful, and the overhead is
     * pretty bad too.  For small numbers of keys it'd likely be better to use
     * a simple insertion sort.
     */
    if *nentries > 1 {
        let keydata: *mut keyEntryData =
            palloc((*nentries as usize) * std::mem::size_of::<keyEntryData>()) as *mut keyEntryData;
        i = 0;
        while i < *nentries {
            (*keydata.add(i as usize)).datum = *entries.add(i as usize);
            (*keydata.add(i as usize)).isnull = *nullFlags.add(i as usize);
            i += 1;
        }

        let mut arg = cmpEntriesArg {
            cmpDatumFunc: &mut (*ginstate).compareFn[(attnum - 1) as usize],
            collation: (*ginstate).supportCollation[(attnum - 1) as usize],
            haveDups: false,
        };
        qsort_arg(
            keydata as *mut c_void,
            *nentries as Size,
            std::mem::size_of::<keyEntryData>(),
            Some(cmpEntries),
            &mut arg as *mut cmpEntriesArg as *mut c_void,
        );

        if arg.haveDups {
            /* there are duplicates, must get rid of 'em */
            let mut j: int32;

            *entries.add(0) = (*keydata.add(0)).datum;
            *nullFlags.add(0) = (*keydata.add(0)).isnull;
            j = 1;
            i = 1;
            while i < *nentries {
                if cmpEntries(
                    keydata.add((i - 1) as usize) as *const c_void,
                    keydata.add(i as usize) as *const c_void,
                    &mut arg as *mut cmpEntriesArg as *mut c_void,
                ) != 0
                {
                    *entries.add(j as usize) = (*keydata.add(i as usize)).datum;
                    *nullFlags.add(j as usize) = (*keydata.add(i as usize)).isnull;
                    j += 1;
                }
                i += 1;
            }
            *nentries = j;
        } else {
            /* easy, no duplicates */
            i = 0;
            while i < *nentries {
                *entries.add(i as usize) = (*keydata.add(i as usize)).datum;
                *nullFlags.add(i as usize) = (*keydata.add(i as usize)).isnull;
                i += 1;
            }
        }

        pfree(keydata as *mut c_void);
    }

    /*
     * Create GinNullCategory representation from nullFlags.
     */
    *categories =
        palloc0((*nentries as usize) * std::mem::size_of::<GinNullCategory>()) as *mut GinNullCategory;
    i = 0;
    while i < *nentries {
        *(*categories).add(i as usize) = if *nullFlags.add(i as usize) {
            GIN_CAT_NULL_KEY as GinNullCategory
        } else {
            GIN_CAT_NORM_KEY as GinNullCategory
        };
        i += 1;
    }

    entries
}

#[allow(non_snake_case)]
pub unsafe extern "C" fn ginoptions(reloptions: Datum, validate: bool) -> *mut bytea {
    static tab: [relopt_parse_elt; 2] = [
        relopt_parse_elt {
            optname: c"fastupdate".as_ptr(),
            opttype: RELOPT_TYPE_BOOL,
            offset: core::mem::offset_of!(GinOptions, useFastUpdate) as c_int,
        },
        relopt_parse_elt {
            optname: c"gin_pending_list_limit".as_ptr(),
            opttype: RELOPT_TYPE_INT,
            offset: core::mem::offset_of!(GinOptions, pendingListCleanupSize) as c_int,
        },
    ];

    build_reloptions(
        reloptions,
        validate,
        RELOPT_KIND_GIN,
        std::mem::size_of::<GinOptions>(),
        tab.as_ptr(),
        tab.len() as c_int,
    ) as *mut bytea
}

/*
 * Fetch index's statistical data into *stats
 *
 * Note: in the result, nPendingPages can be trusted to be up-to-date,
 * as can ginVersion; but the other fields are as of the last VACUUM.
 */
#[allow(non_snake_case)]
pub unsafe fn ginGetStats(index: Relation, stats: *mut GinStatsData) {
    let metabuffer: Buffer = ReadBuffer(index, GIN_METAPAGE_BLKNO);
    LockBuffer(metabuffer, GIN_SHARE);
    let metapage: Page = BufferGetPage(metabuffer);
    let metadata: *mut GinMetaPageData = GinPageGetMeta(metapage);

    (*stats).nPendingPages = (*metadata).nPendingPages;
    (*stats).nTotalPages = (*metadata).nTotalPages;
    (*stats).nEntryPages = (*metadata).nEntryPages;
    (*stats).nDataPages = (*metadata).nDataPages;
    (*stats).nEntries = (*metadata).nEntries;
    (*stats).ginVersion = (*metadata).ginVersion;

    UnlockReleaseBuffer(metabuffer);
}

/*
 * Write the given statistics to the index's metapage
 *
 * Note: nPendingPages and ginVersion are *not* copied over
 */
#[allow(non_snake_case)]
pub unsafe fn ginUpdateStats(index: Relation, stats: *const GinStatsData, is_build: bool) {
    let metabuffer: Buffer = ReadBuffer(index, GIN_METAPAGE_BLKNO);
    LockBuffer(metabuffer, GIN_EXCLUSIVE);
    let metapage: Page = BufferGetPage(metabuffer);
    let metadata: *mut GinMetaPageData = GinPageGetMeta(metapage);

    START_CRIT_SECTION();

    (*metadata).nTotalPages = (*stats).nTotalPages;
    (*metadata).nEntryPages = (*stats).nEntryPages;
    (*metadata).nDataPages = (*stats).nDataPages;
    (*metadata).nEntries = (*stats).nEntries;

    /*
     * Set pd_lower just past the end of the metadata.  This is essential,
     * because without doing so, metadata will be lost if xlog.c compresses
     * the page.  (We must do this here because pre-v11 versions of PG did not
     * set the metapage's pd_lower correctly, so a pg_upgraded index might
     * contain the wrong value.)
     */
    (*(metapage as PageHeader)).pd_lower =
        ((metadata as *mut c_char).add(std::mem::size_of::<GinMetaPageData>()) as isize
            - metapage as isize) as u16;

    MarkBufferDirty(metabuffer);

    if RelationNeedsWAL(index) && !is_build {
        let recptr: XLogRecPtr;
        let mut data: ginxlogUpdateMeta = std::mem::zeroed();

        data.locator = (*index).rd_locator;
        data.ntuples = 0;
        data.prevTail = InvalidBlockNumber;
        data.newRightlink = InvalidBlockNumber;
        std::ptr::copy_nonoverlapping(
            metadata as *const u8,
            &mut data.metadata as *mut GinMetaPageData as *mut u8,
            std::mem::size_of::<GinMetaPageData>(),
        );

        XLogBeginInsert();
        XLogRegisterData(
            &mut data as *mut ginxlogUpdateMeta as *mut c_char,
            std::mem::size_of::<ginxlogUpdateMeta>() as c_int,
        );
        XLogRegisterBuffer(0, metabuffer, REGBUF_WILL_INIT | REGBUF_STANDARD);

        recptr = XLogInsert(RM_GIN_ID, XLOG_GIN_UPDATE_META_PAGE);
        PageSetLSN(metapage, recptr);
    }

    UnlockReleaseBuffer(metabuffer);

    END_CRIT_SECTION();
}

/*
 *	ginbuildphasename() -- Return name of index build phase.
 */
#[allow(non_snake_case)]
pub unsafe extern "C" fn ginbuildphasename(phasenum: int64) -> *mut c_char {
    match phasenum {
        x if x == PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE as int64 => {
            c"initializing".as_ptr() as *mut c_char
        }
        x if x == PROGRESS_GIN_PHASE_INDEXBUILD_TABLESCAN as int64 => {
            c"scanning table".as_ptr() as *mut c_char
        }
        x if x == PROGRESS_GIN_PHASE_PERFORMSORT_1 as int64 => {
            c"sorting tuples (workers)".as_ptr() as *mut c_char
        }
        x if x == PROGRESS_GIN_PHASE_MERGE_1 as int64 => {
            c"merging tuples (workers)".as_ptr() as *mut c_char
        }
        x if x == PROGRESS_GIN_PHASE_PERFORMSORT_2 as int64 => {
            c"sorting tuples".as_ptr() as *mut c_char
        }
        x if x == PROGRESS_GIN_PHASE_MERGE_2 as int64 => {
            c"merging tuples".as_ptr() as *mut c_char
        }
        _ => std::ptr::null_mut(),
    }
}

// ----------------------------------------------------------------------------
// Local stubs for unported dependencies.
// ----------------------------------------------------------------------------

unsafe fn CStr_to_str(_p: *const c_char) -> &'static str {
    "" // TODO: src/include/utils/rel.h (RelationGetRelationName)
}

unsafe fn RelationGetDescr(_index: Relation) -> TupleDesc {
    unimplemented!() // TODO: src/include/utils/rel.h
}
unsafe fn RelationGetRelationName(_index: Relation) -> *const c_char {
    unimplemented!() // TODO: src/include/utils/rel.h
}
unsafe fn RelationNeedsWAL(_index: Relation) -> bool {
    unimplemented!() // TODO: src/include/utils/rel.h
}
unsafe fn TupleDescAttr(_desc: TupleDesc, _i: c_int) -> Form_pg_attribute {
    unimplemented!() // TODO: src/include/access/tupdesc.h
}
unsafe fn CreateTemplateTupleDesc(_natts: c_int) -> TupleDesc {
    unimplemented!() // TODO: src/backend/access/common/tupdesc.c
}
unsafe fn TupleDescInitEntry(
    _desc: TupleDesc,
    _attributeNumber: AttrNumber,
    _attributeName: *const c_char,
    _oidtypeid: Oid,
    _typmod: int32,
    _attdim: c_int,
) {
    unimplemented!() // TODO: src/backend/access/common/tupdesc.c
}
unsafe fn TupleDescInitEntryCollation(
    _desc: TupleDesc,
    _attributeNumber: AttrNumber,
    _collationid: Oid,
) {
    unimplemented!() // TODO: src/backend/access/common/tupdesc.c
}
unsafe fn index_getprocid(_irel: Relation, _attnum: AttrNumber, _procnum: uint16) -> RegProcedure {
    unimplemented!() // TODO: src/backend/access/index/indexam.c
}
unsafe fn index_getprocinfo(
    _irel: Relation,
    _attnum: AttrNumber,
    _procnum: uint16,
) -> *mut FmgrInfo {
    unimplemented!() // TODO: src/backend/access/index/indexam.c
}
unsafe fn fmgr_info_copy(_dstinfo: *mut FmgrInfo, _srcinfo: *mut FmgrInfo, _destcxt: MemoryContext) {
    unimplemented!() // TODO: src/backend/utils/fmgr/fmgr.c
}
unsafe fn lookup_type_cache(_type_id: Oid, _flags: c_int) -> *mut TypeCacheEntry {
    unimplemented!() // TODO: src/backend/utils/cache/typcache.c
}
unsafe fn index_getattr(
    _tup: IndexTuple,
    _attnum: OffsetNumber,
    _tupleDesc: TupleDesc,
    _isnull: *mut bool,
) -> Datum {
    unimplemented!() // TODO: src/include/access/itup.h
}
unsafe fn GinGetNullCategory(_tuple: IndexTuple, _ginstate: *mut GinState) -> GinNullCategory {
    unimplemented!() // TODO: src/include/access/gin_private.h
}
unsafe fn GetFreeIndexPage(_rel: Relation) -> BlockNumber {
    unimplemented!() // TODO: src/backend/storage/freespace/indexfsm.c
}
unsafe fn ReadBuffer(_reln: Relation, _blockNum: BlockNumber) -> Buffer {
    unimplemented!() // TODO: src/backend/storage/buffer/bufmgr.c
}
unsafe fn ConditionalLockBuffer(_buffer: Buffer) -> bool {
    unimplemented!() // TODO: src/backend/storage/buffer/bufmgr.c
}
unsafe fn GinPageIsRecyclable(_page: Page) -> bool {
    unimplemented!() // TODO: src/include/access/gin_private.h
}
unsafe fn BufferGetPage(_buffer: Buffer) -> Page {
    unimplemented!() // TODO: src/include/storage/bufmgr.h
}
unsafe fn LockBuffer(_buffer: Buffer, _mode: c_int) {
    unimplemented!() // TODO: src/backend/storage/buffer/bufmgr.c
}
unsafe fn ReleaseBuffer(_buffer: Buffer) {
    unimplemented!() // TODO: src/backend/storage/buffer/bufmgr.c
}
unsafe fn ExtendBufferedRel(
    _bmr: BufferManagerRelation,
    _forkNum: ForkNumber,
    _strategy: BufferAccessStrategy,
    _flags: uint32,
) -> Buffer {
    unimplemented!() // TODO: src/backend/storage/buffer/bufmgr.c
}
unsafe fn BMR_REL(_rel: Relation) -> BufferManagerRelation {
    unimplemented!() // TODO: src/include/storage/bufmgr.h
}
unsafe fn PageInit(_page: Page, _pageSize: Size, _specialSize: Size) {
    unimplemented!() // TODO: src/backend/storage/page/bufpage.c
}
unsafe fn GinPageGetOpaque(_page: Page) -> GinPageOpaque {
    unimplemented!() // TODO: src/include/access/gin_private.h
}
unsafe fn BufferGetPageSize(_buffer: Buffer) -> Size {
    unimplemented!() // TODO: src/include/storage/bufmgr.h
}
unsafe fn GinPageGetMeta(_page: Page) -> *mut GinMetaPageData {
    unimplemented!() // TODO: src/include/access/gin_private.h
}
unsafe fn FunctionCall2Coll(_flinfo: *mut FmgrInfo, _collation: Oid, _arg1: Datum, _arg2: Datum) -> Datum {
    unimplemented!() // TODO: src/backend/utils/fmgr/fmgr.c
}
unsafe fn FunctionCall3Coll(
    _flinfo: *mut FmgrInfo,
    _collation: Oid,
    _arg1: Datum,
    _arg2: Datum,
    _arg3: Datum,
) -> Datum {
    unimplemented!() // TODO: src/backend/utils/fmgr/fmgr.c
}
unsafe fn qsort_arg(
    _base: *mut c_void,
    _nel: Size,
    _elsize: Size,
    _cmp: Option<unsafe extern "C" fn(*const c_void, *const c_void, *mut c_void) -> c_int>,
    _arg: *mut c_void,
) {
    unimplemented!() // TODO: src/port/qsort_arg.c
}
unsafe fn build_reloptions(
    _reloptions: Datum,
    _validate: bool,
    _kind: relopt_kind,
    _relopt_struct_size: Size,
    _relopt_elems: *const relopt_parse_elt,
    _num_relopt_elems: c_int,
) -> *mut c_void {
    unimplemented!() // TODO: src/backend/access/common/reloptions.c
}
unsafe fn UnlockReleaseBuffer(_buffer: Buffer) {
    unimplemented!() // TODO: src/backend/storage/buffer/bufmgr.c
}
unsafe fn MarkBufferDirty(_buffer: Buffer) {
    unimplemented!() // TODO: src/backend/storage/buffer/bufmgr.c
}
unsafe fn XLogBeginInsert() {
    unimplemented!() // TODO: src/backend/access/transam/xloginsert.c
}
unsafe fn XLogRegisterData(_data: *mut c_char, _len: c_int) {
    unimplemented!() // TODO: src/backend/access/transam/xloginsert.c
}
unsafe fn XLogRegisterBuffer(_block_id: uint8, _buffer: Buffer, _flags: uint8) {
    unimplemented!() // TODO: src/backend/access/transam/xloginsert.c
}
unsafe fn XLogInsert(_rmid: RmgrId, _info: uint8) -> XLogRecPtr {
    unimplemented!() // TODO: src/backend/access/transam/xloginsert.c
}
unsafe fn PageSetLSN(_page: Page, _lsn: XLogRecPtr) {
    unimplemented!() // TODO: src/include/storage/bufpage.h
}
unsafe fn START_CRIT_SECTION() {
    unimplemented!() // TODO: src/include/miscadmin.h
}
unsafe fn END_CRIT_SECTION() {
    unimplemented!() // TODO: src/include/miscadmin.h
}
unsafe fn OffsetNumberNext(_offsetNumber: OffsetNumber) -> OffsetNumber {
    unimplemented!() // TODO: src/include/storage/off.h
}
// Index AM callback stubs (referenced in ginhandler).
unsafe extern "C" fn ginbuild(_heap: Relation, _index: Relation, _indexInfo: *mut IndexInfo) -> *mut IndexBuildResult {
    unimplemented!() // TODO: src/backend/access/gin/gininsert.c
}
unsafe extern "C" fn ginbuildempty(_index: Relation) {
    unimplemented!() // TODO: src/backend/access/gin/gininsert.c
}
unsafe extern "C" fn gininsert(
    _index: Relation,
    _values: *mut Datum,
    _isnull: *mut bool,
    _ht_ctid: ItemPointer,
    _heapRel: Relation,
    _checkUnique: IndexUniqueCheck,
    _indexUnchanged: bool,
    _indexInfo: *mut IndexInfo,
) -> bool {
    unimplemented!() // TODO: src/backend/access/gin/gininsert.c
}
unsafe extern "C" fn ginbulkdelete(
    _info: *mut IndexVacuumInfo,
    _stats: *mut IndexBulkDeleteResult,
    _callback: IndexBulkDeleteCallback,
    _callback_state: *mut c_void,
) -> *mut IndexBulkDeleteResult {
    unimplemented!() // TODO: src/backend/access/gin/ginvacuum.c
}
unsafe extern "C" fn ginvacuumcleanup(
    _info: *mut IndexVacuumInfo,
    _stats: *mut IndexBulkDeleteResult,
) -> *mut IndexBulkDeleteResult {
    unimplemented!() // TODO: src/backend/access/gin/ginvacuum.c
}
unsafe extern "C" fn gincostestimate(
    _root: *mut PlannerInfo,
    _path: *mut IndexPath,
    _loop_count: f64,
    _indexStartupCost: *mut Cost,
    _indexTotalCost: *mut Cost,
    _indexSelectivity: *mut Selectivity,
    _indexCorrelation: *mut f64,
    _indexPages: *mut f64,
) {
    unimplemented!() // TODO: src/backend/utils/adt/selfuncs.c
}
unsafe extern "C" fn ginvalidate(_opclassoid: Oid) -> bool {
    unimplemented!() // TODO: src/backend/access/gin/ginvalidate.c
}
unsafe extern "C" fn ginadjustmembers(
    _opfamilyoid: Oid,
    _opclassoid: Oid,
    _operators: *mut List,
    _functions: *mut List,
) {
    unimplemented!() // TODO: src/backend/access/gin/ginvalidate.c
}
unsafe extern "C" fn ginbeginscan(_rel: Relation, _nkeys: c_int, _norderbys: c_int) -> IndexScanDesc {
    unimplemented!() // TODO: src/backend/access/gin/ginscan.c
}
unsafe extern "C" fn ginrescan(
    _scan: IndexScanDesc,
    _scankey: ScanKey,
    _nscankeys: c_int,
    _orderbys: ScanKey,
    _norderbys: c_int,
) {
    unimplemented!() // TODO: src/backend/access/gin/ginscan.c
}
unsafe extern "C" fn gingetbitmap(_scan: IndexScanDesc, _tbm: *mut TIDBitmap) -> int64 {
    unimplemented!() // TODO: src/backend/access/gin/ginget.c
}
unsafe extern "C" fn ginendscan(_scan: IndexScanDesc) {
    unimplemented!() // TODO: src/backend/access/gin/ginscan.c
}

// Local type stubs (GIN private types, defined in access/gin_private.h / gin.h).
pub type GinNullCategory = u8;
pub type RegProcedure = Oid;
pub type RmgrId = u8;

#[repr(C)]
pub struct GinState {
    pub index: Relation,
    pub oneCol: bool,
    pub origTupdesc: TupleDesc,
    pub tupdesc: [TupleDesc; INDEX_MAX_KEYS],
    pub compareFn: [FmgrInfo; INDEX_MAX_KEYS],
    pub extractValueFn: [FmgrInfo; INDEX_MAX_KEYS],
    pub extractQueryFn: [FmgrInfo; INDEX_MAX_KEYS],
    pub consistentFn: [FmgrInfo; INDEX_MAX_KEYS],
    pub triConsistentFn: [FmgrInfo; INDEX_MAX_KEYS],
    pub comparePartialFn: [FmgrInfo; INDEX_MAX_KEYS],
    pub canPartialMatch: [bool; INDEX_MAX_KEYS],
    pub supportCollation: [Oid; INDEX_MAX_KEYS],
}

#[repr(C)]
pub struct GinPageOpaqueData {
    pub rightlink: BlockNumber,
    pub maxoff: OffsetNumber,
    pub flags: uint16,
}
pub type GinPageOpaque = *mut GinPageOpaqueData;

#[repr(C)]
pub struct GinMetaPageData {
    pub head: BlockNumber,
    pub tail: BlockNumber,
    pub tailFreeSize: uint32,
    pub nPendingPages: BlockNumber,
    pub nPendingHeapTuples: int64,
    pub nTotalPages: BlockNumber,
    pub nEntryPages: BlockNumber,
    pub nDataPages: BlockNumber,
    pub nEntries: int64,
    pub ginVersion: int32,
}

#[repr(C)]
pub struct GinStatsData {
    pub nPendingPages: BlockNumber,
    pub nTotalPages: BlockNumber,
    pub nEntryPages: BlockNumber,
    pub nDataPages: BlockNumber,
    pub nEntries: int64,
    pub ginVersion: int32,
}

#[repr(C)]
pub struct GinOptions {
    pub vl_len_: int32,
    pub useFastUpdate: bool,
    pub pendingListCleanupSize: c_int,
}

#[repr(C)]
pub struct ginxlogUpdateMeta {
    pub locator: RelFileLocator,
    pub metadata: GinMetaPageData,
    pub prevTail: BlockNumber,
    pub newRightlink: BlockNumber,
    pub ntuples: int32,
}

#[repr(C)]
pub struct TypeCacheEntry {
    pub cmp_proc_finfo: FmgrInfo,
}

#[repr(C)]
pub struct BufferManagerRelation {
    pub rel: Relation,
}

pub const INDEX_MAX_KEYS: usize = 32;

// GIN support procedure numbers (access/gin.h)
pub const GIN_COMPARE_PROC: uint16 = 1;
pub const GIN_EXTRACTVALUE_PROC: uint16 = 2;
pub const GIN_EXTRACTQUERY_PROC: uint16 = 3;
pub const GIN_CONSISTENT_PROC: uint16 = 4;
pub const GIN_COMPARE_PARTIAL_PROC: uint16 = 5;
pub const GIN_TRICONSISTENT_PROC: uint16 = 6;
pub const GIN_OPTIONS_PROC: uint16 = 7;
pub const GINNProcs: uint16 = 7;

// GIN null categories (access/gin.h)
pub const GIN_CAT_NORM_KEY: c_int = 0;
pub const GIN_CAT_NULL_KEY: c_int = 1;
pub const GIN_CAT_EMPTY_ITEM: c_int = 2;
pub const GIN_CAT_NULL_ITEM: c_int = 3;
pub const GIN_CAT_EMPTY_QUERY: c_int = -1;

// GIN page flags / meta (access/gin_private.h)
pub const GIN_META: uint32 = 1 << 3;
pub const GIN_CURRENT_VERSION: int32 = 2;
pub const GIN_METAPAGE_BLKNO: BlockNumber = 0;

// GIN lock modes (access/gin_private.h)
pub const GIN_UNLOCK: c_int = 0;
pub const GIN_SHARE: c_int = 1;
pub const GIN_EXCLUSIVE: c_int = 2;

// Resource manager / xlog (access/ginxlog.h, access/rmgrlist.h)
pub const RM_GIN_ID: RmgrId = 13;
pub const XLOG_GIN_UPDATE_META_PAGE: uint8 = 0x60;

// Buffer/extend flags (storage/bufmgr.h)
pub const EB_LOCK_FIRST: uint32 = 1 << 4;
pub const REGBUF_WILL_INIT: uint8 = 0x04 | 0x08;
pub const REGBUF_STANDARD: uint8 = 0x10;

// reloptions (access/reloptions.h)
pub const RELOPT_TYPE_BOOL: relopt_type = 1;
pub const RELOPT_TYPE_INT: relopt_type = 2;
pub const RELOPT_KIND_GIN: relopt_kind = 1 << 7;
pub type relopt_type = c_int;
pub type relopt_kind = c_int;

#[repr(C)]
pub struct relopt_parse_elt {
    pub optname: *const c_char,
    pub opttype: relopt_type,
    pub offset: c_int,
}
unsafe impl Sync for relopt_parse_elt {}

// vacuum parallel options (commands/vacuum.h)
pub const VACUUM_OPTION_PARALLEL_BULKDEL: uint8 = 1 << 1;
pub const VACUUM_OPTION_PARALLEL_CLEANUP: uint8 = 1 << 3;

// progress phases (commands/progress.h, access/gin.h)
pub const PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE: c_int = 0;
pub const PROGRESS_GIN_PHASE_INDEXBUILD_TABLESCAN: c_int = 2;
pub const PROGRESS_GIN_PHASE_PERFORMSORT_1: c_int = 3;
pub const PROGRESS_GIN_PHASE_MERGE_1: c_int = 4;
pub const PROGRESS_GIN_PHASE_PERFORMSORT_2: c_int = 5;
pub const PROGRESS_GIN_PHASE_MERGE_2: c_int = 6;

// catalog OIDs (catalog/pg_collation.h, catalog/pg_type.h)
pub const DEFAULT_COLLATION_OID: Oid = 100;
pub const INT2OID: Oid = 21;

pub const FirstOffsetNumber: OffsetNumber = 1;

// TYPECACHE flag (utils/typcache.h)
pub const TYPECACHE_CMP_PROC_FINFO: c_int = 0x0080;

// Foundational type aliases / stubs.
pub type OffsetNumber = uint16;
pub type Buffer = c_int;
pub type Page = *mut c_char;
pub type PageHeader = *mut PageHeaderData;
pub type Relation = *mut RelationData;
pub type TupleDesc = *mut TupleDescData;
pub type Form_pg_attribute = *mut FormData_pg_attribute;
pub type IndexTuple = *mut c_void;
pub type ItemPointer = *mut c_void;
// MemoryContext comes from crate::utils::palloc via the prelude.
pub type bytea = c_void;
pub type ForkNumber = c_int;
pub type BufferAccessStrategy = *mut c_void;
pub const MAIN_FORKNUM: ForkNumber = 0;

pub type IndexInfo = c_void;
pub type IndexBuildResult = c_void;
pub type IndexVacuumInfo = c_void;
pub type IndexBulkDeleteResult = c_void;
pub type IndexBulkDeleteCallback =
    Option<unsafe extern "C" fn(ItemPointer, *mut c_void) -> bool>;
pub type IndexUniqueCheck = c_int;
pub type PlannerInfo = c_void;
pub type IndexPath = c_void;
pub type Cost = f64;
pub type Selectivity = f64;
pub type IndexScanDesc = *mut c_void;
pub type ScanKey = *mut c_void;
pub type TIDBitmap = c_void;
pub type List = c_void;

#[repr(C)]
pub struct PageHeaderData {
    pub pd_lsn: [u32; 2],
    pub pd_checksum: u16,
    pub pd_flags: u16,
    pub pd_lower: u16,
    pub pd_upper: u16,
    pub pd_special: u16,
    pub pd_pagesize_version: u16,
}

#[repr(C)]
pub struct RelationData {
    pub rd_locator: RelFileLocator,
    pub rd_indcollation: *mut Oid,
}

#[repr(C)]
pub struct TupleDescData {
    pub natts: c_int,
}

#[repr(C)]
pub struct FormData_pg_attribute {
    pub atttypid: Oid,
    pub atttypmod: int32,
    pub attndims: int16,
    pub attcollation: Oid,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct FmgrInfo {
    pub fn_oid: Oid,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RelFileLocator {
    pub spcOid: Oid,
    pub dbOid: Oid,
    pub relNumber: Oid,
}

// IndexAmRoutine (access/amapi.h) - minimal field set used here.
#[repr(C)]
pub struct IndexAmRoutine {
    pub amstrategies: uint16,
    pub amsupport: uint16,
    pub amoptsprocnum: uint16,
    pub amcanorder: bool,
    pub amcanorderbyop: bool,
    pub amcanhash: bool,
    pub amconsistentequality: bool,
    pub amconsistentordering: bool,
    pub amcanbackward: bool,
    pub amcanunique: bool,
    pub amcanmulticol: bool,
    pub amoptionalkey: bool,
    pub amsearcharray: bool,
    pub amsearchnulls: bool,
    pub amstorage: bool,
    pub amclusterable: bool,
    pub ampredlocks: bool,
    pub amcanparallel: bool,
    pub amcanbuildparallel: bool,
    pub amcaninclude: bool,
    pub amusemaintenanceworkmem: bool,
    pub amsummarizing: bool,
    pub amparallelvacuumoptions: uint8,
    pub amkeytype: Oid,
    pub ambuild: Option<unsafe extern "C" fn(Relation, Relation, *mut IndexInfo) -> *mut IndexBuildResult>,
    pub ambuildempty: Option<unsafe extern "C" fn(Relation)>,
    pub aminsert: Option<
        unsafe extern "C" fn(
            Relation,
            *mut Datum,
            *mut bool,
            ItemPointer,
            Relation,
            IndexUniqueCheck,
            bool,
            *mut IndexInfo,
        ) -> bool,
    >,
    pub aminsertcleanup: Option<unsafe extern "C" fn(*mut IndexInfo)>,
    pub ambulkdelete: Option<
        unsafe extern "C" fn(
            *mut IndexVacuumInfo,
            *mut IndexBulkDeleteResult,
            IndexBulkDeleteCallback,
            *mut c_void,
        ) -> *mut IndexBulkDeleteResult,
    >,
    pub amvacuumcleanup: Option<
        unsafe extern "C" fn(*mut IndexVacuumInfo, *mut IndexBulkDeleteResult) -> *mut IndexBulkDeleteResult,
    >,
    pub amcanreturn: Option<unsafe extern "C" fn(Relation, c_int) -> bool>,
    pub amcostestimate: Option<
        unsafe extern "C" fn(
            *mut PlannerInfo,
            *mut IndexPath,
            f64,
            *mut Cost,
            *mut Cost,
            *mut Selectivity,
            *mut f64,
            *mut f64,
        ),
    >,
    pub amgettreeheight: Option<unsafe extern "C" fn(Relation) -> c_int>,
    pub amoptions: Option<unsafe extern "C" fn(Datum, bool) -> *mut bytea>,
    pub amproperty: Option<unsafe extern "C" fn() -> bool>,
    pub ambuildphasename: Option<unsafe extern "C" fn(int64) -> *mut c_char>,
    pub amvalidate: Option<unsafe extern "C" fn(Oid) -> bool>,
    pub amadjustmembers: Option<unsafe extern "C" fn(Oid, Oid, *mut List, *mut List)>,
    pub ambeginscan: Option<unsafe extern "C" fn(Relation, c_int, c_int) -> IndexScanDesc>,
    pub amrescan: Option<unsafe extern "C" fn(IndexScanDesc, ScanKey, c_int, ScanKey, c_int)>,
    pub amgettuple: Option<unsafe extern "C" fn(IndexScanDesc, c_int) -> bool>,
    pub amgetbitmap: Option<unsafe extern "C" fn(IndexScanDesc, *mut TIDBitmap) -> int64>,
    pub amendscan: Option<unsafe extern "C" fn(IndexScanDesc)>,
    pub ammarkpos: Option<unsafe extern "C" fn(IndexScanDesc)>,
    pub amrestrpos: Option<unsafe extern "C" fn(IndexScanDesc)>,
    pub amestimateparallelscan: Option<unsafe extern "C" fn() -> Size>,
    pub aminitparallelscan: Option<unsafe extern "C" fn(*mut c_void)>,
    pub amparallelrescan: Option<unsafe extern "C" fn(IndexScanDesc)>,
}
