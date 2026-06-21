//! spgutils.rs
//!   various support functions for SP-GiST
//!
//! Translated 1:1 from postgres/src/backend/access/spgist/spgutils.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!         src/backend/access/spgist/spgutils.c
//!
//! #include mapping:
//!   "postgres.h"                 -> crate::prelude::*
//!   "access/amvalidate.h"        -> (unused symbols here)
//!   "access/htup_details.h"      -> GETSTRUCT (crate::access::htup_details), HeapTuple
//!   "access/reloptions.h"        -> build_reloptions / relopt_parse_elt (STUB below)
//!   "access/spgist_private.h"    -> crate::access::spgist::spgist_private
//!   "access/toast_compression.h" -> InvalidCompressionMethod (STUB below)
//!   "access/transam.h"           -> InvalidTransactionId (crate::access::transam)
//!   "access/xact.h"              -> GetTopTransactionIdIfAny (STUB below)
//!   "catalog/pg_amop.h"          -> Form_pg_amop / AMOP_ORDER (STUB below)
//!   "commands/vacuum.h"          -> VACUUM_OPTION_* (crate::commands::vacuumparallel)
//!   "nodes/nodeFuncs.h"          -> exprType (STUB below)
//!   "parser/parse_coerce.h"      -> IsBinaryCoercible (STUB below)
//!   "storage/bufmgr.h"           -> Read/Lock/.. buffer routines (STUB below)
//!   "storage/indexfsm.h"         -> GetFreeIndexPage (STUB below)
//!   "utils/catcache.h"           -> CatCList (STUB below)
//!   "utils/fmgrprotos.h"         -> (unused symbols here)
//!   "utils/index_selfuncs.h"     -> spgcostestimate (callback stub below)
//!   "utils/lsyscache.h"          -> getBaseType / get_atttype / ... (STUB below)
//!   "utils/rel.h"                -> crate::utils::rel (RelationGetDescr, ...)
//!   "utils/syscache.h"           -> SearchSysCache1 / ... (STUB below)
//!
//! REAL imports: spgist_private types/consts/macros, IndexAmRoutine + AM property
//! enum (amapi), TupleDesc helpers (tupdesc), Relation helpers (rel), fmgr,
//! heap/index tuple form/deform helpers, bufpage page accessors, itemptr helpers.
//! STUBBED (deep deps not yet ported): the catalog/syscache lookups, lsyscache
//! helpers, buffer manager, indexfsm, reloptions, parse_coerce, nodeFuncs, the
//! SP-GiST page-flag/header accessor macros, and the AM-callback function table.

#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use crate::prelude::*;

use crate::{ereport, errmsg, makeNode, Assert};

// --- spgist_private.h (REAL) -------------------------------------------------
use crate::access::spgist::spgist_private::{
    spgFirstIncludeColumn, spgKeyColumn, GBUF_INNER_PARITY, GBUF_LEAF, GBUF_NULLS,
    GBUF_PARITY_MASK, GBUF_REQ_LEAF, GBUF_REQ_NULLS, SGITMAXNNODES, SGITMAXPREFIXSIZE, SGITMAXSIZE,
    SGLT_SET_HASNULLMASK, SGLT_SET_NEXTOFFSET, SGLT_GET_HASNULLMASK, SpGistBlockIsFixed,
    SpGistCache, SpGistDeadTuple, SpGistDeadTupleData, SpGistInnerTuple, SpGistInnerTupleData,
    SpGistLUPCache, SpGistLastUsedPage, SpGistLeafTuple, SpGistLeafTupleData, SpGistMetaPageData,
    SpGistNodeTuple, SpGistNodeTupleData, SpGistOptions, SpGistPageOpaque, SpGistPageOpaqueData,
    SpGistState, SpGistTypeDesc, SPGIST_CACHED_PAGES, SPGIST_DEAD, SPGIST_LEAF, SPGIST_MAGIC_NUMBER,
    SPGIST_META, SPGIST_METAPAGE_BLKNO, SPGIST_NULLS, SPGIST_PAGE_ID, SPGIST_PLACEHOLDER,
    SPGIST_REDIRECT,
};

// --- access/spgist.h support function numbers (NOT ported standalone; pulled
//     from the spgist module like the sibling opclass files do) ---------------
use crate::access::spgist::spgist::{
    spgConfigIn, SPGISTNProc, SPGIST_COMPRESS_PROC, SPGIST_CONFIG_PROC, SPGIST_OPTIONS_PROC,
};

// --- access/amapi.h (REAL) ---------------------------------------------------
use crate::access::index::amapi::{
    IndexAMProperty, IndexAmRoutine, IndexBuildResult, IndexBulkDeleteCallback,
    IndexBulkDeleteResult, IndexInfo, IndexScanDesc, IndexUniqueCheck, IndexVacuumInfo,
    PlannerInfo, IndexPath, ScanKey, TIDBitmap, AMPROP_DISTANCE_ORDERABLE,
};
use crate::nodes::nodes::{Cost, Selectivity};
use crate::nodes::pg_list::List;
use crate::nodes::plannodes::ScanDirection;

// --- vacuum parallel options (commands/vacuum.h) (REAL) ----------------------
// VACUUM_OPTION_* (commands/vacuum.h); vacuumparallel deferred -> local.
const VACUUM_OPTION_PARALLEL_BULKDEL: u8 = 1 << 0;
const VACUUM_OPTION_PARALLEL_COND_CLEANUP: u8 = 1 << 2;

// --- TupleDesc helpers (REAL) ------------------------------------------------
use crate::access::common::tupdesc::{
    CreateTupleDescCopy, TupleDesc, TupleDescAttr, TupleDescCompactAttr,
    populate_compact_attribute,
};

// --- Relation helpers (REAL) -------------------------------------------------
use crate::utils::rel::{RelationGetDescr, RelationGetRelationName, Relation};

// --- fmgr (REAL) -------------------------------------------------------------
use crate::utils::fmgr::{FmgrInfo, FunctionCall2Coll, FunctionCallInfo};

// --- heap/index tuple helpers (REAL) -----------------------------------------
use crate::access::common::heaptuple::{heap_compute_data_size, heap_fill_tuple};
use crate::access::common::indextuple::{
    index_deform_tuple_internal, IndexTupleHasNulls, IndexTupleSize, INDEX_NULL_MASK,
    INDEX_SIZE_MASK,
};
use crate::access::htup_details::{HeapTuple, HeapTupleData};

// --- bufpage page accessors (REAL) -------------------------------------------
use crate::storage::bufpage::{
    Page, PageAddItem, PageGetExactFreeSpace, PageGetItem, PageGetItemId,
    PageGetMaxOffsetNumber, PageHeader, PageHeaderData, PageInit, PageIndexTupleDelete,
    PageIsEmpty, PageIsNew,
};
use crate::storage::item::Item;
use crate::storage::itemptr::{ItemPointer, ItemPointerData, ItemPointerSet, ItemPointerSetInvalid};

// --- block / offset / buffer (REAL aliases used in signatures) ---------------
use crate::storage::block::{BlockNumber, InvalidBlockNumber};
use crate::storage::buf::Buffer;
use crate::storage::off::{FirstOffsetNumber, InvalidOffsetNumber, OffsetNumber};

// --- transam (REAL) ----------------------------------------------------------
use crate::access::transam::InvalidTransactionId;

// --- varatt (REAL) -----------------------------------------------------------
use crate::varatt::VARSIZE_ANY;

use std::ffi::{c_char, c_int, c_uint, c_void};
use std::mem::size_of;
use std::ptr;

/*
 * SP-GiST handler function: return IndexAmRoutine with access method parameters
 * and callbacks.
 */
pub unsafe fn spghandler(fcinfo: FunctionCallInfo) -> Datum {
    let amroutine: *mut IndexAmRoutine = makeNode!(IndexAmRoutine, T_IndexAmRoutine);

    (*amroutine).amstrategies = 0;
    (*amroutine).amsupport = SPGISTNProc as uint16;
    (*amroutine).amoptsprocnum = SPGIST_OPTIONS_PROC as uint16;
    (*amroutine).amcanorder = false;
    (*amroutine).amcanorderbyop = true;
    (*amroutine).amcanhash = false;
    (*amroutine).amconsistentequality = false;
    (*amroutine).amconsistentordering = false;
    (*amroutine).amcanbackward = false;
    (*amroutine).amcanunique = false;
    (*amroutine).amcanmulticol = false;
    (*amroutine).amoptionalkey = true;
    (*amroutine).amsearcharray = false;
    (*amroutine).amsearchnulls = true;
    (*amroutine).amstorage = true;
    (*amroutine).amclusterable = false;
    (*amroutine).ampredlocks = false;
    (*amroutine).amcanparallel = false;
    (*amroutine).amcanbuildparallel = false;
    (*amroutine).amcaninclude = true;
    (*amroutine).amusemaintenanceworkmem = false;
    (*amroutine).amsummarizing = false;
    (*amroutine).amparallelvacuumoptions =
        VACUUM_OPTION_PARALLEL_BULKDEL | VACUUM_OPTION_PARALLEL_COND_CLEANUP;
    (*amroutine).amkeytype = InvalidOid;

    (*amroutine).ambuild = Some(spgbuild);
    (*amroutine).ambuildempty = Some(spgbuildempty);
    (*amroutine).aminsert = Some(spginsert);
    (*amroutine).aminsertcleanup = None;
    (*amroutine).ambulkdelete = Some(spgbulkdelete);
    (*amroutine).amvacuumcleanup = Some(spgvacuumcleanup);
    (*amroutine).amcanreturn = Some(spgcanreturn);
    (*amroutine).amcostestimate = Some(spgcostestimate);
    (*amroutine).amgettreeheight = None;
    (*amroutine).amoptions = Some(spgoptions_cb);
    (*amroutine).amproperty = Some(spgproperty_cb);
    (*amroutine).ambuildphasename = None;
    (*amroutine).amvalidate = Some(spgvalidate);
    (*amroutine).amadjustmembers = Some(spgadjustmembers);
    (*amroutine).ambeginscan = Some(spgbeginscan);
    (*amroutine).amrescan = Some(spgrescan);
    (*amroutine).amgettuple = Some(spggettuple);
    (*amroutine).amgetbitmap = Some(spggetbitmap);
    (*amroutine).amendscan = Some(spgendscan);
    (*amroutine).ammarkpos = None;
    (*amroutine).amrestrpos = None;
    (*amroutine).amestimateparallelscan = None;
    (*amroutine).aminitparallelscan = None;
    (*amroutine).amparallelrescan = None;
    (*amroutine).amtranslatestrategy = None;
    (*amroutine).amtranslatecmptype = None;

    PointerGetDatum(amroutine as *mut c_void)
}

/*
 * GetIndexInputType
 *		Determine the nominal input data type for an index column
 *
 * We define the "nominal" input type as the associated opclass's opcintype,
 * or if that is a polymorphic type, the base type of the heap column or
 * expression that is the index's input.  The reason for preferring the
 * opcintype is that non-polymorphic opclasses probably don't want to hear
 * about binary-compatible input types.  For instance, if a text opclass
 * is being used with a varchar heap column, we want to report "text" not
 * "varchar".  Likewise, opclasses don't want to hear about domain types,
 * so if we do consult the actual input type, we make sure to flatten domains.
 *
 * At some point maybe this should go somewhere else, but it's not clear
 * if any other index AMs have a use for it.
 */
unsafe fn GetIndexInputType(index: Relation, indexcol: AttrNumber) -> Oid {
    let opcintype: Oid;
    let heapcol: AttrNumber;
    let indexprs: *mut List;
    let mut indexpr_item: *mut ListCell;

    Assert!(!(*index).rd_index.is_null());
    Assert!(indexcol > 0 && indexcol <= (*(*index).rd_index).indnkeyatts);
    opcintype = *(*index).rd_opcintype.add((indexcol - 1) as usize);
    if !IsPolymorphicType(opcintype) {
        return opcintype;
    }
    // TODO(pg-port): indkey is CATALOG_VARLEN (omitted from FormData_pg_index); single-key spgist identity.
    heapcol = indexcol as AttrNumber;
    if heapcol != 0
    /* Simple index column? */
    {
        return getBaseType(get_atttype((*(*index).rd_index).indrelid, heapcol));
    }

    /*
     * If the index expressions are already cached, skip calling
     * RelationGetIndexExpressions, as it will make a copy which is overkill.
     * We're not going to modify the trees, and we're not going to do anything
     * that would invalidate the relcache entry before we're done.
     */
    if !(*index).rd_indexprs.is_null() {
        indexprs = (*index).rd_indexprs;
    } else {
        indexprs = RelationGetIndexExpressions(index);
    }
    indexpr_item = list_head(indexprs);
    let mut i: c_int = 1;
    while i <= (*(*index).rd_index).indnkeyatts as i32 {
        if false /* TODO(pg-port): indkey omitted; no expression columns assumed */ {
            /* expression column */
            if indexpr_item.is_null() {
                elog!(ERROR, "wrong number of index expressions");
            }
            if i == indexcol as c_int {
                return getBaseType(exprType(lfirst(indexpr_item) as *mut Node));
            }
            indexpr_item = lnext(indexprs, indexpr_item);
        }
        i += 1;
    }
    elog!(ERROR, "wrong number of index expressions");
    InvalidOid /* keep compiler quiet */
}

/* Fill in a SpGistTypeDesc struct with info about the specified data type */
unsafe fn fillTypeDesc(desc: *mut SpGistTypeDesc, type_: Oid) {
    let tp: HeapTuple;
    let typtup: Form_pg_type;

    (*desc).type_ = type_;
    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_));
    if !HeapTupleIsValid(tp) {
        elog!(ERROR, "cache lookup failed for type {}", type_);
    }
    typtup = GETSTRUCT(tp) as Form_pg_type;
    (*desc).attlen = (*typtup).typlen;
    (*desc).attbyval = (*typtup).typbyval;
    (*desc).attalign = (*typtup).typalign;
    (*desc).attstorage = (*typtup).typstorage;
    ReleaseSysCache(tp);
}

/*
 * Fetch local cache of AM-specific info about the index, initializing it
 * if necessary
 */
pub unsafe fn spgGetCache(index: Relation) -> *mut SpGistCache {
    let cache: *mut SpGistCache;

    if (*index).rd_amcache.is_null() {
        let atttype: Oid;
        let mut in_: spgConfigIn = std::mem::zeroed();
        let procinfo: *mut FmgrInfo;

        let cache = MemoryContextAllocZero((*index).rd_indexcxt as crate::utils::palloc::MemoryContext, size_of::<SpGistCache>())
            as *mut SpGistCache;

        /* SPGiST must have one key column and can also have INCLUDE columns */
        Assert!(IndexRelationGetNumberOfKeyAttributes(index) == 1);
        Assert!(IndexRelationGetNumberOfAttributes(index) <= INDEX_MAX_KEYS as c_int);

        /*
         * Get the actual (well, nominal) data type of the key column.  We
         * pass this to the opclass config function so that polymorphic
         * opclasses are possible.
         */
        atttype = GetIndexInputType(index, (spgKeyColumn + 1) as AttrNumber);

        /* Call the config function to get config info for the opclass */
        in_.attType = atttype;

        procinfo = index_getprocinfo(index, 1, SPGIST_CONFIG_PROC as uint16);
        FunctionCall2Coll(
            procinfo,
            *(*index).rd_indcollation.add(spgKeyColumn as usize),
            PointerGetDatum(&mut in_ as *mut spgConfigIn as *mut c_void),
            PointerGetDatum(&mut (*cache).config as *mut _ as *mut c_void),
        );

        /*
         * If leafType isn't specified, use the declared index column type,
         * which index.c will have derived from the opclass's opcintype.
         * (Although we now make spgvalidate.c warn if these aren't the same,
         * old user-defined opclasses may not set the STORAGE parameter
         * correctly, so believe leafType if it's given.)
         */
        if !OidIsValid((*cache).config.leafType) {
            (*cache).config.leafType =
                (*TupleDescAttr(RelationGetDescr(index), spgKeyColumn)).atttypid;

            /*
             * If index column type is binary-coercible to atttype (for
             * example, it's a domain over atttype), treat it as plain atttype
             * to avoid thinking we need to compress.
             */
            if (*cache).config.leafType != atttype
                && IsBinaryCoercible((*cache).config.leafType, atttype)
            {
                (*cache).config.leafType = atttype;
            }
        }

        /* Get the information we need about each relevant datatype */
        fillTypeDesc(&mut (*cache).attType, atttype);

        if (*cache).config.leafType != atttype {
            if !OidIsValid(index_getprocid(index, 1, SPGIST_COMPRESS_PROC as uint16)) {
                ereport!(
                    ERROR,
                    errmsg!("compress method must be defined when leaf type is different from input type")
                );
            }

            fillTypeDesc(&mut (*cache).attLeafType, (*cache).config.leafType);
        } else {
            /* Save lookups in this common case */
            (*cache).attLeafType = (*cache).attType;
        }

        fillTypeDesc(&mut (*cache).attPrefixType, (*cache).config.prefixType);
        fillTypeDesc(&mut (*cache).attLabelType, (*cache).config.labelType);

        /*
         * Finally, if it's a real index (not a partitioned one), get the
         * lastUsedPages data from the metapage
         */
        if (*(*index).rd_rel).relkind != RELKIND_PARTITIONED_INDEX {
            let metabuffer: Buffer;
            let metadata: *mut SpGistMetaPageData;

            metabuffer = ReadBuffer(index, SPGIST_METAPAGE_BLKNO);
            LockBuffer(metabuffer, BUFFER_LOCK_SHARE);

            metadata = SpGistPageGetMeta(BufferGetPage(metabuffer));

            if (*metadata).magicNumber != SPGIST_MAGIC_NUMBER {
                elog!(
                    ERROR,
                    "index \"{}\" is not an SP-GiST index",
                    CStr_display(RelationGetRelationName(index))
                );
            }

            (*cache).lastUsedPages = (*metadata).lastUsedPages;

            UnlockReleaseBuffer(metabuffer);
        }

        (*index).rd_amcache = cache as *mut c_void;
        return cache;
    } else {
        /* assume it's up to date */
        cache = (*index).rd_amcache as *mut SpGistCache;
    }

    cache
}

/*
 * Compute a tuple descriptor for leaf tuples or index-only-scan result tuples.
 *
 * We can use the relcache's tupdesc as-is in many cases, and it's always
 * OK so far as any INCLUDE columns are concerned.  However, the entry for
 * the key column has to match leafType in the first case or attType in the
 * second case.  While the relcache's tupdesc *should* show leafType, this
 * might not hold for legacy user-defined opclasses, since before v14 they
 * were not allowed to declare their true storage type in CREATE OPCLASS.
 * Also, attType can be different from what is in the relcache.
 *
 * This function gives back either a pointer to the relcache's tupdesc
 * if that is suitable, or a palloc'd copy that's been adjusted to match
 * the specified key column type.  We can avoid doing any catalog lookups
 * here by insisting that the caller pass an SpGistTypeDesc not just an OID.
 */
pub unsafe fn getSpGistTupleDesc(index: Relation, keyType: *mut SpGistTypeDesc) -> TupleDesc {
    let outTupDesc: TupleDesc;
    let att: Form_pg_attribute;

    if (*keyType).type_ == (*TupleDescAttr(RelationGetDescr(index), spgKeyColumn)).atttypid {
        outTupDesc = RelationGetDescr(index);
    } else {
        outTupDesc = CreateTupleDescCopy(RelationGetDescr(index));
        att = TupleDescAttr(outTupDesc, spgKeyColumn);
        /* It's sufficient to update the type-dependent fields of the column */
        (*att).atttypid = (*keyType).type_;
        (*att).atttypmod = -1;
        (*att).attlen = (*keyType).attlen;
        (*att).attbyval = (*keyType).attbyval;
        (*att).attalign = (*keyType).attalign;
        (*att).attstorage = (*keyType).attstorage;
        /* We shouldn't need to bother with making these valid: */
        (*att).attcompression = InvalidCompressionMethod;
        (*att).attcollation = InvalidOid;
        /* In case we changed typlen, we'd better reset following offsets */
        let mut i: c_int = spgFirstIncludeColumn;
        while i < (*outTupDesc).natts {
            (*TupleDescCompactAttr(outTupDesc, i)).attcacheoff = -1;
            i += 1;
        }

        populate_compact_attribute(outTupDesc, spgKeyColumn);
    }
    outTupDesc
}

/* Initialize SpGistState for working with the given index */
pub unsafe fn initSpGistState(state: *mut SpGistState, index: Relation) {
    let cache: *mut SpGistCache;

    (*state).index = index;

    /* Get cached static information about index */
    cache = spgGetCache(index);

    (*state).config = (*cache).config;
    (*state).attType = (*cache).attType;
    (*state).attLeafType = (*cache).attLeafType;
    (*state).attPrefixType = (*cache).attPrefixType;
    (*state).attLabelType = (*cache).attLabelType;

    /* Ensure we have a valid descriptor for leaf tuples */
    (*state).leafTupDesc = getSpGistTupleDesc((*state).index, &mut (*state).attLeafType);

    /* Make workspace for constructing dead tuples */
    (*state).deadTupleStorage = palloc0(SGDTSIZE) as *mut c_char;

    /*
     * Set horizon XID to use in redirection tuples.  Use our own XID if we
     * have one, else use InvalidTransactionId.  The latter case can happen in
     * VACUUM or REINDEX CONCURRENTLY, and in neither case would it be okay to
     * force an XID to be assigned.  VACUUM won't create any redirection
     * tuples anyway, but REINDEX CONCURRENTLY can.  Fortunately, REINDEX
     * CONCURRENTLY doesn't mark the index valid until the end, so there could
     * never be any concurrent scans "in flight" to a redirection tuple it has
     * inserted.  And it locks out VACUUM until the end, too.  So it's okay
     * for VACUUM to immediately expire a redirection tuple that contains an
     * invalid xid.
     */
    (*state).redirectXid = GetTopTransactionIdIfAny();

    /* Assume we're not in an index build (spgbuild will override) */
    (*state).isBuild = false;
}

/*
 * Allocate a new page (either by recycling, or by extending the index file).
 *
 * The returned buffer is already pinned and exclusive-locked.
 * Caller is responsible for initializing the page by calling SpGistInitBuffer.
 */
pub unsafe fn SpGistNewBuffer(index: Relation) -> Buffer {
    let buffer: Buffer;

    /* First, try to get a page from FSM */
    loop {
        let blkno: BlockNumber = GetFreeIndexPage(index);

        if blkno == InvalidBlockNumber {
            break; /* nothing known to FSM */
        }

        /*
         * The fixed pages shouldn't ever be listed in FSM, but just in case
         * one is, ignore it.
         */
        if SpGistBlockIsFixed(blkno) {
            continue;
        }

        let buffer = ReadBuffer(index, blkno);

        /*
         * We have to guard against the possibility that someone else already
         * recycled this page; the buffer may be locked if so.
         */
        if ConditionalLockBuffer(buffer) {
            let page: Page = BufferGetPage(buffer);

            if PageIsNew(page) {
                return buffer; /* OK to use, if never initialized */
            }

            if SpGistPageIsDeleted(page) || PageIsEmpty(page) {
                return buffer; /* OK to use */
            }

            LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        }

        /* Can't use it, so release buffer and try again */
        ReleaseBuffer(buffer);
    }

    buffer = ExtendBufferedRel(BMR_REL(index), MAIN_FORKNUM, ptr::null_mut(), EB_LOCK_FIRST);

    buffer
}

/*
 * Update index metapage's lastUsedPages info from local cache, if possible
 *
 * Updating meta page isn't critical for index working, so
 * 1 use ConditionalLockBuffer to improve concurrency
 * 2 don't WAL-log metabuffer changes to decrease WAL traffic
 */
pub unsafe fn SpGistUpdateMetaPage(index: Relation) {
    let cache: *mut SpGistCache = (*index).rd_amcache as *mut SpGistCache;

    if !cache.is_null() {
        let metabuffer: Buffer;

        metabuffer = ReadBuffer(index, SPGIST_METAPAGE_BLKNO);

        if ConditionalLockBuffer(metabuffer) {
            let metapage: Page = BufferGetPage(metabuffer);
            let metadata: *mut SpGistMetaPageData = SpGistPageGetMeta(metapage);

            (*metadata).lastUsedPages = (*cache).lastUsedPages;

            /*
             * Set pd_lower just past the end of the metadata.  This is
             * essential, because without doing so, metadata will be lost if
             * xlog.c compresses the page.  (We must do this here because
             * pre-v11 versions of PG did not set the metapage's pd_lower
             * correctly, so a pg_upgraded index might contain the wrong
             * value.)
             */
            (*(metapage as PageHeader)).pd_lower = ((metadata as *mut c_char)
                .add(size_of::<SpGistMetaPageData>()))
            .offset_from(metapage as *mut c_char) as uint16;

            MarkBufferDirty(metabuffer);
            UnlockReleaseBuffer(metabuffer);
        } else {
            ReleaseBuffer(metabuffer);
        }
    }
}

/* Macro to select proper element of lastUsedPages cache depending on flags */
/* Masking flags with SPGIST_CACHED_PAGES is just for paranoia's sake */
#[inline]
unsafe fn GET_LUP(c: *mut SpGistCache, f: c_int) -> *mut SpGistLastUsedPage {
    &mut (*c).lastUsedPages.cachedPage[((f as c_uint) as usize) % SPGIST_CACHED_PAGES]
        as *mut SpGistLastUsedPage
}

/*
 * Allocate and initialize a new buffer of the type and parity specified by
 * flags.  The returned buffer is already pinned and exclusive-locked.
 *
 * When requesting an inner page, if we get one with the wrong parity,
 * we just release the buffer and try again.  We will get a different page
 * because GetFreeIndexPage will have marked the page used in FSM.  The page
 * is entered in our local lastUsedPages cache, so there's some hope of
 * making use of it later in this session, but otherwise we rely on VACUUM
 * to eventually re-enter the page in FSM, making it available for recycling.
 * Note that such a page does not get marked dirty here, so unless it's used
 * fairly soon, the buffer will just get discarded and the page will remain
 * as it was on disk.
 *
 * When we return a buffer to the caller, the page is *not* entered into
 * the lastUsedPages cache; we expect the caller will do so after it's taken
 * whatever space it will use.  This is because after the caller has used up
 * some space, the page might have less space than whatever was cached already
 * so we'd rather not trash the old cache entry.
 */
unsafe fn allocNewBuffer(index: Relation, flags: c_int) -> Buffer {
    let cache: *mut SpGistCache = spgGetCache(index);
    let mut pageflags: uint16 = 0;

    if GBUF_REQ_LEAF(flags) {
        pageflags |= SPGIST_LEAF as uint16;
    }
    if GBUF_REQ_NULLS(flags) != 0 {
        pageflags |= SPGIST_NULLS as uint16;
    }

    loop {
        let buffer: Buffer;

        buffer = SpGistNewBuffer(index);
        SpGistInitBuffer(buffer, pageflags);

        if pageflags & (SPGIST_LEAF as uint16) != 0 {
            /* Leaf pages have no parity concerns, so just use it */
            return buffer;
        } else {
            let blkno: BlockNumber = BufferGetBlockNumber(buffer);
            let mut blkFlags: c_int = GBUF_INNER_PARITY(blkno);

            if (flags & GBUF_PARITY_MASK) == blkFlags {
                /* Page has right parity, use it */
                return buffer;
            } else {
                /* Page has wrong parity, record it in cache and try again */
                if pageflags & (SPGIST_NULLS as uint16) != 0 {
                    blkFlags |= GBUF_NULLS;
                }
                (*cache).lastUsedPages.cachedPage[blkFlags as usize].blkno = blkno;
                (*cache).lastUsedPages.cachedPage[blkFlags as usize].freeSpace =
                    PageGetExactFreeSpace(BufferGetPage(buffer)) as c_int;
                UnlockReleaseBuffer(buffer);
            }
        }
    }
}

/*
 * Get a buffer of the type and parity specified by flags, having at least
 * as much free space as indicated by needSpace.  We use the lastUsedPages
 * cache to assign the same buffer previously requested when possible.
 * The returned buffer is already pinned and exclusive-locked.
 *
 * *isNew is set true if the page was initialized here, false if it was
 * already valid.
 */
pub unsafe fn SpGistGetBuffer(
    index: Relation,
    flags: c_int,
    mut needSpace: c_int,
    isNew: *mut bool,
) -> Buffer {
    let cache: *mut SpGistCache = spgGetCache(index);
    let lup: *mut SpGistLastUsedPage;

    /* Bail out if even an empty page wouldn't meet the demand */
    if needSpace > SPGIST_PAGE_CAPACITY {
        elog!(ERROR, "desired SPGiST tuple size is too big");
    }

    /*
     * If possible, increase the space request to include relation's
     * fillfactor.  This ensures that when we add unrelated tuples to a page,
     * we try to keep 100-fillfactor% available for adding tuples that are
     * related to the ones already on it.  But fillfactor mustn't cause an
     * error for requests that would otherwise be legal.
     */
    needSpace += SpGistGetTargetPageFreeSpace(index);
    needSpace = Min(needSpace, SPGIST_PAGE_CAPACITY);

    /* Get the cache entry for this flags setting */
    lup = GET_LUP(cache, flags);

    /* If we have nothing cached, just turn it over to allocNewBuffer */
    if (*lup).blkno == InvalidBlockNumber {
        *isNew = true;
        return allocNewBuffer(index, flags);
    }

    /* fixed pages should never be in cache */
    Assert!(!SpGistBlockIsFixed((*lup).blkno));

    /* If cached freeSpace isn't enough, don't bother looking at the page */
    if (*lup).freeSpace >= needSpace {
        let buffer: Buffer;
        let page: Page;

        buffer = ReadBuffer(index, (*lup).blkno);

        if !ConditionalLockBuffer(buffer) {
            /*
             * buffer is locked by another process, so return a new buffer
             */
            ReleaseBuffer(buffer);
            *isNew = true;
            return allocNewBuffer(index, flags);
        }

        page = BufferGetPage(buffer);

        if PageIsNew(page) || SpGistPageIsDeleted(page) || PageIsEmpty(page) {
            /* OK to initialize the page */
            let mut pageflags: uint16 = 0;

            if GBUF_REQ_LEAF(flags) {
                pageflags |= SPGIST_LEAF as uint16;
            }
            if GBUF_REQ_NULLS(flags) != 0 {
                pageflags |= SPGIST_NULLS as uint16;
            }
            SpGistInitBuffer(buffer, pageflags);
            (*lup).freeSpace = PageGetExactFreeSpace(page) as c_int - needSpace;
            *isNew = true;
            return buffer;
        }

        /*
         * Check that page is of right type and has enough space.  We must
         * recheck this since our cache isn't necessarily up to date.
         */
        if (if GBUF_REQ_LEAF(flags) {
            SpGistPageIsLeaf(page)
        } else {
            !SpGistPageIsLeaf(page)
        }) && (if GBUF_REQ_NULLS(flags) != 0 {
            SpGistPageStoresNulls(page)
        } else {
            !SpGistPageStoresNulls(page)
        }) {
            let freeSpace: c_int = PageGetExactFreeSpace(page) as c_int;

            if freeSpace >= needSpace {
                /* Success, update freespace info and return the buffer */
                (*lup).freeSpace = freeSpace - needSpace;
                *isNew = false;
                return buffer;
            }
        }

        /*
         * fallback to allocation of new buffer
         */
        UnlockReleaseBuffer(buffer);
    }

    /* No success with cache, so return a new buffer */
    *isNew = true;
    allocNewBuffer(index, flags)
}

/*
 * Update lastUsedPages cache when done modifying a page.
 *
 * We update the appropriate cache entry if it already contained this page
 * (its freeSpace is likely obsolete), or if this page has more space than
 * whatever we had cached.
 */
pub unsafe fn SpGistSetLastUsedPage(index: Relation, buffer: Buffer) {
    let cache: *mut SpGistCache = spgGetCache(index);
    let lup: *mut SpGistLastUsedPage;
    let freeSpace: c_int;
    let page: Page = BufferGetPage(buffer);
    let blkno: BlockNumber = BufferGetBlockNumber(buffer);
    let mut flags: c_int;

    /* Never enter fixed pages (root pages) in cache, though */
    if SpGistBlockIsFixed(blkno) {
        return;
    }

    if SpGistPageIsLeaf(page) {
        flags = GBUF_LEAF;
    } else {
        flags = GBUF_INNER_PARITY(blkno);
    }
    if SpGistPageStoresNulls(page) {
        flags |= GBUF_NULLS;
    }

    lup = GET_LUP(cache, flags);

    freeSpace = PageGetExactFreeSpace(page) as c_int;
    if (*lup).blkno == InvalidBlockNumber || (*lup).blkno == blkno || (*lup).freeSpace < freeSpace {
        (*lup).blkno = blkno;
        (*lup).freeSpace = freeSpace;
    }
}

/*
 * Initialize an SPGiST page to empty, with specified flags
 */
pub unsafe fn SpGistInitPage(page: Page, f: uint16) {
    let opaque: SpGistPageOpaque;

    PageInit(page, BLCKSZ, size_of::<SpGistPageOpaqueData>());
    opaque = SpGistPageGetOpaque(page);
    (*opaque).flags = f;
    (*opaque).spgist_page_id = SPGIST_PAGE_ID as uint16;
}

/*
 * Initialize a buffer's page to empty, with specified flags
 */
pub unsafe fn SpGistInitBuffer(b: Buffer, f: uint16) {
    Assert!(BufferGetPageSize(b) == BLCKSZ);
    SpGistInitPage(BufferGetPage(b), f);
}

/*
 * Initialize metadata page
 */
pub unsafe fn SpGistInitMetapage(page: Page) {
    let metadata: *mut SpGistMetaPageData;
    let mut i: c_int;

    SpGistInitPage(page, SPGIST_META as uint16);
    metadata = SpGistPageGetMeta(page);
    ptr::write_bytes(metadata as *mut u8, 0, size_of::<SpGistMetaPageData>());
    (*metadata).magicNumber = SPGIST_MAGIC_NUMBER;

    /* initialize last-used-page cache to empty */
    i = 0;
    while (i as usize) < SPGIST_CACHED_PAGES {
        (*metadata).lastUsedPages.cachedPage[i as usize].blkno = InvalidBlockNumber;
        i += 1;
    }

    /*
     * Set pd_lower just past the end of the metadata.  This is essential,
     * because without doing so, metadata will be lost if xlog.c compresses
     * the page.
     */
    (*(page as PageHeader)).pd_lower = ((metadata as *mut c_char)
        .add(size_of::<SpGistMetaPageData>()))
    .offset_from(page as *mut c_char) as uint16;
}

/*
 * reloptions processing for SPGiST
 */
pub unsafe fn spgoptions(reloptions: Datum, validate: bool) -> *mut bytea {
    static tab: [relopt_parse_elt; 1] = [relopt_parse_elt {
        optname: c"fillfactor".as_ptr(),
        opttype: RELOPT_TYPE_INT,
        offset: offset_of_SpGistOptions_fillfactor() as c_int,
    }];

    build_reloptions(
        reloptions,
        validate,
        RELOPT_KIND_SPGIST,
        size_of::<SpGistOptions>(),
        tab.as_ptr(),
        lengthof!(tab) as c_int,
    ) as *mut bytea
}

/*
 * Get the space needed to store a non-null datum of the indicated type
 * in an inner tuple (that is, as a prefix or node label).
 * Note the result is already rounded up to a MAXALIGN boundary.
 * Here we follow the convention that pass-by-val types are just stored
 * in their Datum representation (compare memcpyInnerDatum).
 */
pub unsafe fn SpGistGetInnerTypeSize(att: *mut SpGistTypeDesc, datum: Datum) -> c_uint {
    let size: c_uint;

    if (*att).attbyval {
        size = size_of::<Datum>() as c_uint;
    } else if (*att).attlen > 0 {
        size = (*att).attlen as c_uint;
    } else {
        size = VARSIZE_ANY(DatumGetPointer(datum) as *const c_char);
    }

    MAXALIGN(size as usize) as c_uint
}

/*
 * Copy the given non-null datum to *target, in the inner-tuple case
 */
unsafe fn memcpyInnerDatum(target: *mut c_void, att: *mut SpGistTypeDesc, datum: Datum) {
    let size: c_uint;

    if (*att).attbyval {
        ptr::copy_nonoverlapping(
            &datum as *const Datum as *const u8,
            target as *mut u8,
            size_of::<Datum>(),
        );
    } else {
        size = if (*att).attlen > 0 {
            (*att).attlen as c_uint
        } else {
            VARSIZE_ANY(DatumGetPointer(datum) as *const c_char)
        };
        ptr::copy_nonoverlapping(
            DatumGetPointer(datum) as *const u8,
            target as *mut u8,
            size as usize,
        );
    }
}

/*
 * Compute space required for a leaf tuple holding the given data.
 *
 * This must match the size-calculation portion of spgFormLeafTuple.
 */
pub unsafe fn SpGistGetLeafTupleSize(
    tupleDescriptor: TupleDesc,
    datums: *const Datum,
    isnulls: *const bool,
) -> Size {
    let mut size: Size;
    let data_size: Size;
    let mut needs_null_mask: bool = false;
    let natts: c_int = (*tupleDescriptor).natts;

    /*
     * Decide whether we need a nulls bitmask.
     *
     * If there is only a key attribute (natts == 1), never use a bitmask, for
     * compatibility with the pre-v14 layout of leaf tuples.  Otherwise, we
     * need one if any attribute is null.
     */
    if natts > 1 {
        let mut i: c_int = 0;
        while i < natts {
            if *isnulls.add(i as usize) {
                needs_null_mask = true;
                break;
            }
            i += 1;
        }
    }

    /*
     * Calculate size of the data part; same as for heap tuples.
     */
    data_size = heap_compute_data_size(tupleDescriptor, datums as *mut Datum, isnulls as *mut bool);

    /*
     * Compute total size.
     */
    size = SGLTHDRSZ(needs_null_mask);
    size += data_size;
    size = MAXALIGN(size);

    /*
     * Ensure that we can replace the tuple with a dead tuple later. This test
     * is unnecessary when there are any non-null attributes, but be safe.
     */
    if size < SGDTSIZE {
        size = SGDTSIZE;
    }

    size
}

/*
 * Construct a leaf tuple containing the given heap TID and datum values
 */
pub unsafe fn spgFormLeafTuple(
    state: *mut SpGistState,
    heapPtr: ItemPointer,
    datums: *const Datum,
    isnulls: *const bool,
) -> SpGistLeafTuple {
    let tup: SpGistLeafTuple;
    let tupleDescriptor: TupleDesc = (*state).leafTupDesc;
    let mut size: Size;
    let hoff: Size;
    let data_size: Size;
    let mut needs_null_mask: bool = false;
    let natts: c_int = (*tupleDescriptor).natts;
    let tp: *mut c_char; /* ptr to tuple data */
    let mut tupmask: uint16 = 0; /* unused heap_fill_tuple output */

    /*
     * Decide whether we need a nulls bitmask.
     *
     * If there is only a key attribute (natts == 1), never use a bitmask, for
     * compatibility with the pre-v14 layout of leaf tuples.  Otherwise, we
     * need one if any attribute is null.
     */
    if natts > 1 {
        let mut i: c_int = 0;
        while i < natts {
            if *isnulls.add(i as usize) {
                needs_null_mask = true;
                break;
            }
            i += 1;
        }
    }

    /*
     * Calculate size of the data part; same as for heap tuples.
     */
    data_size = heap_compute_data_size(tupleDescriptor, datums as *mut Datum, isnulls as *mut bool);

    /*
     * Compute total size.
     */
    hoff = SGLTHDRSZ(needs_null_mask);
    size = hoff + data_size;
    size = MAXALIGN(size);

    /*
     * Ensure that we can replace the tuple with a dead tuple later. This test
     * is unnecessary when there are any non-null attributes, but be safe.
     */
    if size < SGDTSIZE {
        size = SGDTSIZE;
    }

    /* OK, form the tuple */
    tup = palloc0(size) as SpGistLeafTuple;

    SGLT_SET_SIZE(tup, size as c_uint);
    SGLT_SET_NEXTOFFSET(tup, InvalidOffsetNumber);
    (*tup).heapPtr = *heapPtr;

    tp = (tup as *mut c_char).add(hoff);

    if needs_null_mask {
        let bp: *mut bits8; /* ptr to null bitmap in tuple */

        /* Set nullmask presence bit in SpGistLeafTuple header */
        SGLT_SET_HASNULLMASK(tup, true);
        /* Fill the data area and null mask */
        bp = (tup as *mut c_char).add(size_of::<SpGistLeafTupleData>()) as *mut bits8;
        heap_fill_tuple(
            tupleDescriptor,
            datums as *mut Datum,
            isnulls as *mut bool,
            tp,
            data_size,
            &mut tupmask,
            bp,
        );
    } else if natts > 1 || !*isnulls.add(spgKeyColumn as usize) {
        /* Fill data area only */
        heap_fill_tuple(
            tupleDescriptor,
            datums as *mut Datum,
            isnulls as *mut bool,
            tp,
            data_size,
            &mut tupmask,
            ptr::null_mut::<bits8>(),
        );
    }
    /* otherwise we have no data, nor a bitmap, to fill */

    tup
}

/*
 * Construct a node (to go into an inner tuple) containing the given label
 *
 * Note that the node's downlink is just set invalid here.  Caller will fill
 * it in later.
 */
pub unsafe fn spgFormNodeTuple(state: *mut SpGistState, label: Datum, isnull: bool) -> SpGistNodeTuple {
    let tup: SpGistNodeTuple;
    let mut size: c_uint;
    let mut infomask: c_ushort = 0;

    /* compute space needed (note result is already maxaligned) */
    size = SGNTHDRSZ() as c_uint;
    if !isnull {
        size += SpGistGetInnerTypeSize(&mut (*state).attLabelType, label);
    }

    /*
     * Here we make sure that the size will fit in the field reserved for it
     * in t_info.
     */
    if (size & (INDEX_SIZE_MASK as c_uint)) != size {
        ereport!(
            ERROR,
            errmsg!(
                "index row requires {} bytes, maximum size is {}",
                size as Size,
                INDEX_SIZE_MASK as Size
            )
        );
    }

    tup = palloc0(size as Size) as SpGistNodeTuple;

    if isnull {
        infomask |= INDEX_NULL_MASK;
    }
    /* we don't bother setting the INDEX_VAR_MASK bit */
    infomask |= size as c_ushort;
    (*tup).t_info = infomask;

    /* The TID field will be filled in later */
    ItemPointerSetInvalid(&mut (*tup).t_tid);

    if !isnull {
        memcpyInnerDatum(SGNTDATAPTR(tup), &mut (*state).attLabelType, label);
    }

    tup
}

/*
 * Construct an inner tuple containing the given prefix and node array
 */
pub unsafe fn spgFormInnerTuple(
    state: *mut SpGistState,
    hasPrefix: bool,
    prefix: Datum,
    nNodes: c_int,
    nodes: *mut SpGistNodeTuple,
) -> SpGistInnerTuple {
    let tup: SpGistInnerTuple;
    let mut size: c_uint;
    let prefixSize: c_uint;
    let mut i: c_int;
    let mut ptr_: *mut c_char;

    /* Compute size needed */
    if hasPrefix {
        prefixSize = SpGistGetInnerTypeSize(&mut (*state).attPrefixType, prefix);
    } else {
        prefixSize = 0;
    }

    size = SGITHDRSZ() as c_uint + prefixSize;

    /* Note: we rely on node tuple sizes to be maxaligned already */
    i = 0;
    while i < nNodes {
        size += IndexTupleSize(*nodes.add(i as usize)) as c_uint;
        i += 1;
    }

    /*
     * Ensure that we can replace the tuple with a dead tuple later.  This
     * test is unnecessary given current tuple layouts, but let's be safe.
     */
    if (size as Size) < SGDTSIZE {
        size = SGDTSIZE as c_uint;
    }

    /*
     * Inner tuple should be small enough to fit on a page
     */
    if (size as Size) > SPGIST_PAGE_CAPACITY as Size - size_of::<ItemIdData>() {
        ereport!(
            ERROR,
            errmsg!(
                "SP-GiST inner tuple size {} exceeds maximum {}",
                size as Size,
                SPGIST_PAGE_CAPACITY as Size - size_of::<ItemIdData>()
            )
        );
    }

    /*
     * Check for overflow of header fields --- probably can't fail if the
     * above succeeded, but let's be paranoid
     */
    if size as c_int > SGITMAXSIZE
        || prefixSize as c_int > SGITMAXPREFIXSIZE
        || nNodes > SGITMAXNNODES
    {
        elog!(ERROR, "SPGiST inner tuple header field is too small");
    }

    /* OK, form the tuple */
    tup = palloc0(size as Size) as SpGistInnerTuple;

    SGIT_SET_NNODES(tup, nNodes as c_uint);
    SGIT_SET_PREFIXSIZE(tup, prefixSize);
    (*tup).size = size as uint16;

    if hasPrefix {
        memcpyInnerDatum(SGITDATAPTR(tup), &mut (*state).attPrefixType, prefix);
    }

    ptr_ = SGITNODEPTR(tup) as *mut c_char;

    i = 0;
    while i < nNodes {
        let node: SpGistNodeTuple = *nodes.add(i as usize);

        ptr::copy_nonoverlapping(node as *const u8, ptr_ as *mut u8, IndexTupleSize(node));
        ptr_ = ptr_.add(IndexTupleSize(node));
        i += 1;
    }

    tup
}

/*
 * Construct a "dead" tuple to replace a tuple being deleted.
 *
 * The state can be SPGIST_REDIRECT, SPGIST_DEAD, or SPGIST_PLACEHOLDER.
 * For a REDIRECT tuple, a pointer (blkno+offset) must be supplied, and
 * the xid field is filled in automatically.
 *
 * This is called in critical sections, so we don't use palloc; the tuple
 * is built in preallocated storage.  It should be copied before another
 * call with different parameters can occur.
 */
pub unsafe fn spgFormDeadTuple(
    state: *mut SpGistState,
    tupstate: c_int,
    blkno: BlockNumber,
    offnum: OffsetNumber,
) -> SpGistDeadTuple {
    let tuple: SpGistDeadTuple = (*state).deadTupleStorage as SpGistDeadTuple;

    SGDT_SET_TUPSTATE(tuple, tupstate as c_uint);
    SGDT_SET_SIZE(tuple, SGDTSIZE as c_uint);
    SGLT_SET_NEXTOFFSET_DEAD(tuple, InvalidOffsetNumber);

    if tupstate == SPGIST_REDIRECT {
        ItemPointerSet(&mut (*tuple).pointer, blkno, offnum);
        (*tuple).xid = (*state).redirectXid;
    } else {
        ItemPointerSetInvalid(&mut (*tuple).pointer);
        (*tuple).xid = InvalidTransactionId;
    }

    tuple
}

/*
 * Convert an SPGiST leaf tuple into Datum/isnull arrays.
 *
 * The caller must allocate sufficient storage for the output arrays.
 * (INDEX_MAX_KEYS entries should be enough.)
 */
pub unsafe fn spgDeformLeafTuple(
    tup: SpGistLeafTuple,
    tupleDescriptor: TupleDesc,
    datums: *mut Datum,
    isnulls: *mut bool,
    keyColumnIsNull: bool,
) {
    let hasNullsMask: bool = SGLT_GET_HASNULLMASK(tup);
    let tp: *mut c_char; /* ptr to tuple data */
    let bp: *mut bits8; /* ptr to null bitmap in tuple */

    if keyColumnIsNull && (*tupleDescriptor).natts == 1 {
        /*
         * Trivial case: there is only the key attribute and we're in a nulls
         * tree.  The hasNullsMask bit in the tuple header should not be set
         * (and thus we can't use index_deform_tuple_internal), but
         * nonetheless the result is NULL.
         *
         * Note: currently this is dead code, because noplace calls this when
         * there is only the key attribute.  But we should cover the case.
         */
        Assert!(!hasNullsMask);

        *datums.add(spgKeyColumn as usize) = 0 as Datum;
        *isnulls.add(spgKeyColumn as usize) = true;
        return;
    }

    tp = (tup as *mut c_char).add(SGLTHDRSZ(hasNullsMask));
    bp = (tup as *mut c_char).add(size_of::<SpGistLeafTupleData>()) as *mut bits8;

    index_deform_tuple_internal(tupleDescriptor, datums, isnulls, tp, bp, hasNullsMask as c_int);

    /*
     * Key column isnull value from the tuple should be consistent with
     * keyColumnIsNull flag from the caller.
     */
    Assert!(keyColumnIsNull == *isnulls.add(spgKeyColumn as usize));
}

/*
 * Extract the label datums of the nodes within innerTuple
 *
 * Returns NULL if label datums are NULLs
 */
pub unsafe fn spgExtractNodeLabels(
    state: *mut SpGistState,
    innerTuple: SpGistInnerTuple,
) -> *mut Datum {
    let nodeLabels: *mut Datum;
    let mut i: c_int = 0;
    let mut node: SpGistNodeTuple;

    /* Either all the labels must be NULL, or none. */
    node = SGITNODEPTR(innerTuple);
    if IndexTupleHasNulls(node) {
        // SGITITERATE(innerTuple, i, node)
        i = 0;
        node = SGITNODEPTR(innerTuple);
        while i < SGITGetNNodes(innerTuple) {
            if !IndexTupleHasNulls(node) {
                elog!(
                    ERROR,
                    "some but not all node labels are null in SPGiST inner tuple"
                );
            }
            i += 1;
            node = (node as *mut c_char).add(IndexTupleSize(node)) as SpGistNodeTuple;
        }
        /* They're all null, so just return NULL */
        ptr::null_mut()
    } else {
        nodeLabels =
            palloc((size_of::<Datum>() * SGITGetNNodes(innerTuple) as usize) as Size) as *mut Datum;
        // SGITITERATE(innerTuple, i, node)
        i = 0;
        node = SGITNODEPTR(innerTuple);
        while i < SGITGetNNodes(innerTuple) {
            if IndexTupleHasNulls(node) {
                elog!(
                    ERROR,
                    "some but not all node labels are null in SPGiST inner tuple"
                );
            }
            *nodeLabels.add(i as usize) = SGNTDATUM(node, state);
            i += 1;
            node = (node as *mut c_char).add(IndexTupleSize(node)) as SpGistNodeTuple;
        }
        nodeLabels
    }
}

/*
 * Add a new item to the page, replacing a PLACEHOLDER item if possible.
 * Return the location it's inserted at, or InvalidOffsetNumber on failure.
 *
 * If startOffset isn't NULL, we start searching for placeholders at
 * *startOffset, and update that to the next place to search.  This is just
 * an optimization for repeated insertions.
 *
 * If errorOK is false, we throw error when there's not enough room,
 * rather than returning InvalidOffsetNumber.
 */
pub unsafe fn SpGistPageAddNewItem(
    state: *mut SpGistState,
    page: Page,
    item: Item,
    size: Size,
    startOffset: *mut OffsetNumber,
    errorOK: bool,
) -> OffsetNumber {
    let opaque: SpGistPageOpaque = SpGistPageGetOpaque(page);
    let mut i: OffsetNumber;
    let maxoff: OffsetNumber;
    let mut offnum: OffsetNumber;

    if (*opaque).nPlaceholder > 0
        && PageGetExactFreeSpace(page) + SGDTSIZE >= MAXALIGN(size)
    {
        /* Try to replace a placeholder */
        maxoff = PageGetMaxOffsetNumber(page);
        offnum = InvalidOffsetNumber;

        loop {
            if !startOffset.is_null() && *startOffset != InvalidOffsetNumber {
                i = *startOffset;
            } else {
                i = FirstOffsetNumber;
            }
            while i <= maxoff {
                let it: SpGistDeadTuple =
                    PageGetItem(page, PageGetItemId(page, i)) as SpGistDeadTuple;

                if SGDT_GET_TUPSTATE(it) == SPGIST_PLACEHOLDER as c_uint {
                    offnum = i;
                    break;
                }
                i += 1;
            }

            /* Done if we found a placeholder */
            if offnum != InvalidOffsetNumber {
                break;
            }

            if !startOffset.is_null() && *startOffset != InvalidOffsetNumber {
                /* Hint was no good, re-search from beginning */
                *startOffset = InvalidOffsetNumber;
                continue;
            }

            /* Hmm, no placeholder found? */
            (*opaque).nPlaceholder = 0;
            break;
        }

        if offnum != InvalidOffsetNumber {
            /* Replace the placeholder tuple */
            PageIndexTupleDelete(page, offnum);

            offnum = PageAddItem(page, item, size, offnum, false, false);

            /*
             * We should not have failed given the size check at the top of
             * the function, but test anyway.  If we did fail, we must PANIC
             * because we've already deleted the placeholder tuple, and
             * there's no other way to keep the damage from getting to disk.
             */
            if offnum != InvalidOffsetNumber {
                Assert!((*opaque).nPlaceholder > 0);
                (*opaque).nPlaceholder -= 1;
                if !startOffset.is_null() {
                    *startOffset = offnum + 1;
                }
            } else {
                elog!(
                    PANIC,
                    "failed to add item of size {} to SPGiST index page",
                    size
                );
            }

            return offnum;
        }
    }

    /* No luck in replacing a placeholder, so just add it to the page */
    offnum = PageAddItem(page, item, size, InvalidOffsetNumber, false, false);

    if offnum == InvalidOffsetNumber && !errorOK {
        elog!(
            ERROR,
            "failed to add item of size {} to SPGiST index page",
            size
        );
    }

    offnum
}

/*
 *	spgproperty() -- Check boolean properties of indexes.
 *
 * This is optional for most AMs, but is required for SP-GiST because the core
 * property code doesn't support AMPROP_DISTANCE_ORDERABLE.
 */
pub unsafe fn spgproperty(
    index_oid: Oid,
    attno: c_int,
    prop: IndexAMProperty,
    propname: *const c_char,
    res: *mut bool,
    isnull: *mut bool,
) -> bool {
    let opclass: Oid;
    let mut opfamily: Oid = InvalidOid;
    let mut opcintype: Oid = InvalidOid;
    let catlist: *mut CatCList;
    let mut i: c_int;

    /* Only answer column-level inquiries */
    if attno == 0 {
        return false;
    }

    match prop {
        AMPROP_DISTANCE_ORDERABLE => {}
        _ => return false,
    }

    /*
     * Currently, SP-GiST distance-ordered scans require that there be a
     * distance operator in the opclass with the default types. So we assume
     * that if such an operator exists, then there's a reason for it.
     */

    /* First we need to know the column's opclass. */
    opclass = get_index_column_opclass(index_oid, attno);
    if !OidIsValid(opclass) {
        *isnull = true;
        return true;
    }

    /* Now look up the opclass family and input datatype. */
    if !get_opclass_opfamily_and_input_type(opclass, &mut opfamily, &mut opcintype) {
        *isnull = true;
        return true;
    }

    /* And now we can check whether the operator is provided. */
    catlist = SearchSysCacheList1(AMOPSTRATEGY, ObjectIdGetDatum(opfamily));

    *res = false;

    i = 0;
    while i < (*catlist).n_members {
        let amoptup: HeapTuple = &mut (*(*(*catlist).members.add(i as usize))).tuple;
        let amopform: Form_pg_amop = GETSTRUCT(amoptup) as Form_pg_amop;

        if (*amopform).amoppurpose == AMOP_ORDER
            && ((*amopform).amoplefttype == opcintype || (*amopform).amoprighttype == opcintype)
            && opfamily_can_sort_type(
                (*amopform).amopsortfamily,
                get_op_rettype((*amopform).amopopr),
            )
        {
            *res = true;
            break;
        }
        i += 1;
    }

    ReleaseSysCacheList(catlist);

    *isnull = false;

    true
}

// ============================================================================
// Local stubs for unported dependencies.
//
// The sibling SP-GiST units (spgvacuum.rs, ginutil.rs, ...) follow the same
// convention: keep each translated .c self-contained by locally stubbing the
// buffer manager, page-flag accessor macros, catalog/syscache lookups, and the
// AM-callback function table, until those headers/units are ported.
// ============================================================================

// ---- foundational scalar/pointer aliases used in this file ----------------
pub type AttrNumber = int16;
/// TODO(pg-port): real `RegProcedure` lives in postgres_ext.h.
pub type RegProcedure = Oid;

/// TODO(pg-port): real `Node` lives in nodes/nodes.h.
pub enum Node {}

/// TODO(pg-port): real `ListCell` lives in nodes/pg_list.h.
pub enum ListCell {}

/// TODO(pg-port): real `Form_pg_type` lives in catalog/pg_type.h.
pub type Form_pg_type = *mut FormData_pg_type;
#[repr(C)]
pub struct FormData_pg_type {
    pub typlen: int16,
    pub typbyval: bool,
    pub typalign: c_char,
    pub typstorage: c_char,
}

/// TODO(pg-port): real `Form_pg_attribute` lives in catalog/pg_attribute.h
/// (REAL def IS ported there; aliased through tupdesc in other units). Local
/// stub mirrors the fields this file touches.
pub type Form_pg_attribute = crate::catalog::pg_attribute::Form_pg_attribute;

/// TODO(pg-port): real `Form_pg_amop` lives in catalog/pg_amop.h.
pub type Form_pg_amop = *mut FormData_pg_amop;
#[repr(C)]
pub struct FormData_pg_amop {
    pub amoppurpose: c_char,
    pub amoplefttype: Oid,
    pub amoprighttype: Oid,
    pub amopopr: Oid,
    pub amopsortfamily: Oid,
}

/// TODO(pg-port): real `ItemIdData` lives in storage/itemid.h.
#[repr(C)]
pub struct ItemIdData {
    pub bits: c_uint,
}

/// TODO(pg-port): real `CatCList`/`CatCTup` live in utils/catcache.h.
#[repr(C)]
pub struct CatCList {
    pub n_members: c_int,
    pub members: *mut *mut CatCTup,
}
#[repr(C)]
pub struct CatCTup {
    pub tuple: HeapTupleData,
}

// ---- c.rs / system OID constants (not yet exported by their real homes) ----
/// TODO(pg-port): real value lives in pg_config.h (block size).
pub const BLCKSZ: Size = 8192;
/// TODO(pg-port): real value lives in pg_config_manual.h.
pub const INDEX_MAX_KEYS: usize = 32;
/// TODO(pg-port): real value lives in catalog/pg_class.h.
pub const RELKIND_PARTITIONED_INDEX: c_char = b'I' as c_char;
/// TODO(pg-port): real value lives in access/toast_compression.h.
pub const InvalidCompressionMethod: c_char = 0;
/// TODO(pg-port): real value lives in utils/syscache.h (SysCacheIdentifier).
pub const TYPEOID: c_int = 82;
pub const AMOPSTRATEGY: c_int = 4;
/// TODO(pg-port): real value lives in catalog/pg_amop.h.
pub const AMOP_ORDER: c_char = b'o' as c_char;
/// TODO(pg-port): real value lives in access/reloptions.h.
pub const RELOPT_TYPE_INT: relopt_type = 2;
pub const RELOPT_KIND_SPGIST: relopt_kind = 1 << 6;
pub type relopt_type = c_int;
pub type relopt_kind = c_int;

#[repr(C)]
pub struct relopt_parse_elt {
    pub optname: *const c_char,
    pub opttype: relopt_type,
    pub offset: c_int,
}
unsafe impl Sync for relopt_parse_elt {}

// ---- SP-GiST private header derived sizes (access/spgist_private.h macros) --
// SGITHDRSZ / SGNTHDRSZ / SGLTHDRSZ / SGDTSIZE and SPGIST_PAGE_CAPACITY.
// TODO(pg-port): mirror the exact macros once spgist_private.rs exports them.
#[inline]
fn SGITHDRSZ() -> Size {
    MAXALIGN(size_of::<SpGistInnerTupleData>())
}
#[inline]
fn SGNTHDRSZ() -> Size {
    MAXALIGN(size_of::<SpGistNodeTupleData>())
}
#[inline]
unsafe fn SGLTHDRSZ(hasnulls: bool) -> Size {
    if hasnulls {
        MAXALIGN(size_of::<SpGistLeafTupleData>() + size_of::<IndexAttributeBitMapData>())
    } else {
        MAXALIGN(size_of::<SpGistLeafTupleData>())
    }
}
// SGDTSIZE = MAXALIGN(sizeof(SpGistDeadTupleData)).
#[allow(non_upper_case_globals)]
const SGDTSIZE: Size = MAXALIGN(size_of::<SpGistDeadTupleData>());

/// TODO(pg-port): real `IndexAttributeBitMapData` lives in access/itup.h.
#[repr(C)]
pub struct IndexAttributeBitMapData {
    pub bits: [bits8; (INDEX_MAX_KEYS + 8 - 1) / 8],
}

// SPGIST_PAGE_CAPACITY = BLCKSZ - SizeOfPageHeaderData - MAXALIGN(special).
// TODO(pg-port): mirror exactly once spgist_private.rs exports it.
pub const SPGIST_PAGE_CAPACITY: c_int = 8192;

// offsetof(SpGistOptions, fillfactor)
#[inline]
const fn offset_of_SpGistOptions_fillfactor() -> usize {
    core::mem::offset_of!(SpGistOptions, fillfactor)
}

// ---- Sp-GiST leaf/inner/dead tuple bit-field accessors ---------------------
// The C macros write the packed bit fields (tupstate/size/nNodes/prefixSize).
// These mirror the bit layout documented in spgist_private.rs.
#[inline]
unsafe fn SGLT_SET_SIZE(tup: SpGistLeafTuple, size: c_uint) {
    /* tupstate:2, size:30 -> set size, preserve tupstate (=SPGIST_LIVE=0) */
    (*tup).bits_ = ((*tup).bits_ & 0x3) | ((size << 2) & !0x3);
}
#[inline]
unsafe fn SGIT_SET_NNODES(tup: SpGistInnerTuple, nNodes: c_uint) {
    /* tupstate:2, allTheSame:1, nNodes:13, prefixSize:16 */
    (*tup).bits_ = ((*tup).bits_ & !(0x1FFF << 3)) | ((nNodes & 0x1FFF) << 3);
}
#[inline]
unsafe fn SGIT_SET_PREFIXSIZE(tup: SpGistInnerTuple, prefixSize: c_uint) {
    (*tup).bits_ = ((*tup).bits_ & 0xFFFF) | ((prefixSize & 0xFFFF) << 16);
}
#[inline]
unsafe fn SGITGetNNodes(tup: SpGistInnerTuple) -> c_int {
    (((*tup).bits_ >> 3) & 0x1FFF) as c_int
}
#[inline]
unsafe fn SGDT_SET_TUPSTATE(tup: SpGistDeadTuple, tupstate: c_uint) {
    (*tup).bits_ = ((*tup).bits_ & !0x3) | (tupstate & 0x3);
}
#[inline]
unsafe fn SGDT_GET_TUPSTATE(tup: SpGistDeadTuple) -> c_uint {
    (*tup).bits_ & 0x3
}
#[inline]
unsafe fn SGDT_SET_SIZE(tup: SpGistDeadTuple, size: c_uint) {
    (*tup).bits_ = ((*tup).bits_ & 0x3) | ((size << 2) & !0x3);
}
#[inline]
unsafe fn SGLT_SET_NEXTOFFSET_DEAD(tup: SpGistDeadTuple, offsetNumber: OffsetNumber) {
    (*tup).t_info = ((*tup).t_info & 0xC000) | ((offsetNumber as uint16) & 0x3FFF);
}

// ---- SP-GiST page/data accessor macros (access/spgist_private.h) -----------
unsafe fn SpGistPageGetOpaque(page: Page) -> SpGistPageOpaque {
    unimplemented!() // TODO(pg-port): access/spgist_private.h (PageGetSpecialPointer)
}
unsafe fn SpGistPageGetMeta(page: Page) -> *mut SpGistMetaPageData {
    unimplemented!() // TODO(pg-port): access/spgist_private.h
}
unsafe fn SpGistPageIsLeaf(page: Page) -> bool {
    unimplemented!() // TODO(pg-port): access/spgist_private.h
}
unsafe fn SpGistPageIsDeleted(page: Page) -> bool {
    unimplemented!() // TODO(pg-port): access/spgist_private.h
}
unsafe fn SpGistPageStoresNulls(page: Page) -> bool {
    unimplemented!() // TODO(pg-port): access/spgist_private.h
}
unsafe fn SpGistGetTargetPageFreeSpace(index: Relation) -> c_int {
    unimplemented!() // TODO(pg-port): access/spgist_private.h (RelationGetFillFactor)
}
unsafe fn SGITDATAPTR(tup: SpGistInnerTuple) -> *mut c_void {
    unimplemented!() // TODO(pg-port): access/spgist_private.h
}
unsafe fn SGITNODEPTR(tup: SpGistInnerTuple) -> SpGistNodeTuple {
    unimplemented!() // TODO(pg-port): access/spgist_private.h
}
unsafe fn SGNTDATAPTR(tup: SpGistNodeTuple) -> *mut c_void {
    unimplemented!() // TODO(pg-port): access/spgist_private.h
}
unsafe fn SGNTDATUM(tup: SpGistNodeTuple, state: *mut SpGistState) -> Datum {
    unimplemented!() // TODO(pg-port): access/spgist_private.h
}

// ---- relcache field helpers (utils/rel.h) ----------------------------------
unsafe fn IndexRelationGetNumberOfKeyAttributes(index: Relation) -> c_int { crate::access::nbtree::nbtdedup::IndexRelationGetNumberOfKeyAttributes(index) }
unsafe fn IndexRelationGetNumberOfAttributes(index: Relation) -> c_int { crate::access::nbtree::nbtsearch::IndexRelationGetNumberOfAttributes(index) }
unsafe fn CStr_display(p: *mut c_char) -> &'static str {
    "" // TODO(pg-port): utils/rel.h (RelationGetRelationName) - format helper
}

// ---- index AM accessor (access/index/indexam.c) ----------------------------
unsafe fn index_getprocinfo(irel: Relation, attnum: AttrNumber, procnum: uint16) -> *mut FmgrInfo { crate::access::index::indexam::index_getprocinfo(irel, attnum, procnum) }
unsafe fn index_getprocid(irel: Relation, attnum: AttrNumber, procnum: uint16) -> RegProcedure { crate::access::index::indexam::index_getprocid(irel, attnum, procnum) }

// ---- buffer manager (storage/bufmgr.c) -------------------------------------
pub const BUFFER_LOCK_UNLOCK: c_int = 0;
pub const BUFFER_LOCK_SHARE: c_int = 1;
pub const EB_LOCK_FIRST: uint32 = 1 << 4;
pub const MAIN_FORKNUM: ForkNumber = 0;
pub type ForkNumber = c_int;
pub type BufferAccessStrategy = *mut c_void;
#[repr(C)]
pub struct BufferManagerRelation {
    pub rel: Relation,
}

unsafe fn ReadBuffer(reln: Relation, blockNum: BlockNumber) -> Buffer {
    unimplemented!() // TODO(pg-port): src/backend/storage/buffer/bufmgr.c
}
unsafe fn LockBuffer(buffer: Buffer, mode: c_int) {
    unimplemented!() // TODO(pg-port): src/backend/storage/buffer/bufmgr.c
}
unsafe fn ConditionalLockBuffer(buffer: Buffer) -> bool {
    unimplemented!() // TODO(pg-port): src/backend/storage/buffer/bufmgr.c
}
unsafe fn UnlockReleaseBuffer(buffer: Buffer) {
    unimplemented!() // TODO(pg-port): src/backend/storage/buffer/bufmgr.c
}
unsafe fn ReleaseBuffer(buffer: Buffer) {
    unimplemented!() // TODO(pg-port): src/backend/storage/buffer/bufmgr.c
}
unsafe fn MarkBufferDirty(buffer: Buffer) {
    unimplemented!() // TODO(pg-port): src/backend/storage/buffer/bufmgr.c
}
unsafe fn BufferGetPage(buffer: Buffer) -> Page {
    unimplemented!() // TODO(pg-port): src/include/storage/bufmgr.h
}
unsafe fn BufferGetPageSize(buffer: Buffer) -> Size { crate::access::nbtree::nbtpage::BufferGetPageSize(buffer) }
unsafe fn BufferGetBlockNumber(buffer: Buffer) -> BlockNumber {
    unimplemented!() // TODO(pg-port): src/backend/storage/buffer/bufmgr.c
}
unsafe fn ExtendBufferedRel(
    bmr: BufferManagerRelation,
    forkNum: ForkNumber,
    strategy: BufferAccessStrategy,
    flags: uint32,
) -> Buffer { unimplemented!() }
unsafe fn BMR_REL(rel: Relation) -> BufferManagerRelation {
    unimplemented!() // TODO(pg-port): src/include/storage/bufmgr.h
}

// ---- index FSM (storage/indexfsm.c) ----------------------------------------
unsafe fn GetFreeIndexPage(rel: Relation) -> BlockNumber { crate::storage::freespace::indexfsm::GetFreeIndexPage(rel) }

// ---- reloptions (access/reloptions.c) --------------------------------------
unsafe fn build_reloptions(
    reloptions: Datum,
    validate: bool,
    kind: relopt_kind,
    relopt_struct_size: Size,
    relopt_elems: *const relopt_parse_elt,
    num_relopt_elems: c_int,
) -> *mut c_void { unimplemented!() }

// ---- transaction / xact (access/xact.c) ------------------------------------
unsafe fn GetTopTransactionIdIfAny() -> TransactionId {
    unimplemented!() // TODO(pg-port): src/backend/access/transam/xact.c
}

// ---- catalog/syscache lookups (utils/cache/*) ------------------------------
unsafe fn SearchSysCache1(cacheId: c_int, key1: Datum) -> HeapTuple {
    unimplemented!() // TODO(pg-port): src/backend/utils/cache/syscache.c
}
unsafe fn ReleaseSysCache(tuple: HeapTuple) {
    unimplemented!() // TODO(pg-port): src/backend/utils/cache/catcache.c
}
unsafe fn SearchSysCacheList1(cacheId: c_int, key1: Datum) -> *mut CatCList {
    unimplemented!() // TODO(pg-port): src/backend/utils/cache/syscache.c
}
unsafe fn ReleaseSysCacheList(list: *mut CatCList) {
    unimplemented!() // TODO(pg-port): src/backend/utils/cache/catcache.c
}
unsafe fn GETSTRUCT(tuple: HeapTuple) -> *mut c_char {
    unimplemented!() // TODO(pg-port): src/include/access/htup_details.h
}
unsafe fn HeapTupleIsValid(tuple: HeapTuple) -> bool {
    !tuple.is_null() // TODO(pg-port): src/include/access/htup.h
}

// ---- lsyscache / type helpers (utils/cache/lsyscache.c) --------------------
unsafe fn getBaseType(typid: Oid) -> Oid { crate::utils::cache::lsyscache::getBaseType(typid) }
unsafe fn get_atttype(relid: Oid, attnum: AttrNumber) -> Oid { crate::utils::cache::lsyscache::get_atttype(relid, attnum) }
unsafe fn get_index_column_opclass(index_oid: Oid, attno: c_int) -> Oid { crate::utils::cache::lsyscache::get_index_column_opclass(index_oid, attno) }
unsafe fn get_opclass_opfamily_and_input_type(
    opclass: Oid,
    opfamily: *mut Oid,
    opcintype: *mut Oid,
) -> bool { crate::utils::cache::lsyscache::get_opclass_opfamily_and_input_type(opclass, opfamily, opcintype) }
unsafe fn get_op_rettype(opno: Oid) -> Oid { crate::utils::cache::lsyscache::get_op_rettype(opno) }
unsafe fn opfamily_can_sort_type(opfamilyoid: Oid, datatypeoid: Oid) -> bool { crate::access::index::amvalidate::opfamily_can_sort_type(opfamilyoid, datatypeoid) }
unsafe fn IsPolymorphicType(typid: Oid) -> bool {
    unimplemented!() // TODO(pg-port): src/include/catalog/pg_type.h
}

// ---- parse_coerce (parser/parse_coerce.c) ----------------------------------
unsafe fn IsBinaryCoercible(srctype: Oid, targettype: Oid) -> bool { crate::parser::parse_coerce::IsBinaryCoercible(srctype, targettype) }

// ---- nodeFuncs / pg_list (nodes/*) -----------------------------------------
unsafe fn exprType(expr: *mut Node) -> Oid {
    unimplemented!() // TODO(pg-port): src/backend/nodes/nodeFuncs.c
}
unsafe fn RelationGetIndexExpressions(relation: Relation) -> *mut List {
    unimplemented!() // TODO(pg-port): src/backend/utils/cache/relcache.c
}
unsafe fn list_head(l: *mut List) -> *mut ListCell {
    unimplemented!() // TODO(pg-port): src/include/nodes/pg_list.h
}
unsafe fn lnext(l: *mut List, cell: *mut ListCell) -> *mut ListCell {
    unimplemented!() // TODO(pg-port): src/backend/nodes/list.c
}
unsafe fn lfirst(cell: *mut ListCell) -> *mut c_void {
    unimplemented!() // TODO(pg-port): src/include/nodes/pg_list.h
}

// ---- AM callback function table (referenced in spghandler) -----------------
// These live in the other SP-GiST units (spginsert.c, spgvacuum.c, spgscan.c,
// spgvalidate.c) and selfuncs.c.  Stubbed here to build the IndexAmRoutine.
unsafe extern "C" fn spgbuild(
    heap: Relation,
    index: Relation,
    indexInfo: *mut IndexInfo,
) -> *mut IndexBuildResult { crate::access::spgist::spginsert::spgbuild(heap, index, indexInfo) }
unsafe extern "C" fn spgbuildempty(index: Relation) { crate::access::spgist::spginsert::spgbuildempty(index) }
unsafe extern "C" fn spginsert(
    index: Relation,
    values: *mut Datum,
    isnull: *mut bool,
    ht_ctid: ItemPointer,
    heapRel: Relation,
    checkUnique: IndexUniqueCheck,
    indexUnchanged: bool,
    indexInfo: *mut IndexInfo,
) -> bool { crate::access::spgist::spginsert::spginsert(index, values, isnull, ht_ctid, heapRel, checkUnique, indexUnchanged, indexInfo) }
unsafe extern "C" fn spgbulkdelete(
    info: *mut IndexVacuumInfo,
    stats: *mut IndexBulkDeleteResult,
    callback: IndexBulkDeleteCallback,
    callback_state: *mut c_void,
) -> *mut IndexBulkDeleteResult { unimplemented!() }
unsafe extern "C" fn spgvacuumcleanup(
    info: *mut IndexVacuumInfo,
    stats: *mut IndexBulkDeleteResult,
) -> *mut IndexBulkDeleteResult { unimplemented!() }
unsafe extern "C" fn spgcanreturn(index: Relation, attno: c_int) -> bool { crate::access::spgist::spgscan::spgcanreturn(index, attno) }
unsafe extern "C" fn spgcostestimate(
    root: *mut PlannerInfo,
    path: *mut IndexPath,
    loop_count: f64,
    indexStartupCost: *mut Cost,
    indexTotalCost: *mut Cost,
    indexSelectivity: *mut Selectivity,
    indexCorrelation: *mut f64,
    indexPages: *mut f64,
) { unimplemented!() }
unsafe extern "C" fn spgoptions_cb(reloptions: Datum, validate: bool) -> *mut bytea {
    spgoptions(reloptions, validate)
}
unsafe extern "C" fn spgproperty_cb(
    index_oid: Oid,
    attno: c_int,
    prop: IndexAMProperty,
    propname: *const c_char,
    res: *mut bool,
    isnull: *mut bool,
) -> bool {
    spgproperty(index_oid, attno, prop, propname, res, isnull)
}
unsafe extern "C" fn spgvalidate(opclassoid: Oid) -> bool { crate::access::spgist::spgvalidate::spgvalidate(opclassoid) }
unsafe extern "C" fn spgadjustmembers(
    opfamilyoid: Oid,
    opclassoid: Oid,
    operators: *mut List,
    functions: *mut List,
) { crate::access::spgist::spgvalidate::spgadjustmembers(opfamilyoid, opclassoid, operators, functions) }
unsafe extern "C" fn spgbeginscan(rel: Relation, nkeys: c_int, norderbys: c_int) -> IndexScanDesc { unimplemented!() }
unsafe extern "C" fn spgrescan(
    scan: IndexScanDesc,
    scankey: ScanKey,
    nscankeys: c_int,
    orderbys: ScanKey,
    norderbys: c_int,
) { unimplemented!() }
unsafe extern "C" fn spggettuple(scan: IndexScanDesc, direction: ScanDirection) -> bool { unimplemented!() }
unsafe extern "C" fn spggetbitmap(scan: IndexScanDesc, tbm: *mut TIDBitmap) -> int64 { unimplemented!() }
unsafe extern "C" fn spgendscan(scan: IndexScanDesc) { unimplemented!() }

// `c_ushort` mirrors C `unsigned short` for spgFormNodeTuple's infomask.
type c_ushort = u16;
