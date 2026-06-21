//! src/backend/access/hash/hash.c
//!
//! hash.c
//!   Implementation of Margo Seltzer's Hashing package for postgres.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/access/hash/hash.c
//!
//! NOTES
//!   This file contains only the public interface routines.
//!
//! This module also merges the public declarations from
//! src/include/access/hash.h.

use crate::prelude::*;

use std::ffi::{c_int, c_void};

use crate::c::{int16, int32, int64, uint16, uint32, uint8, Size};

use crate::access::cmptype::{CompareType, COMPARE_EQ, COMPARE_INVALID};
use crate::access::common::indextuple::{
    index_form_tuple, IndexTuple, IndexTupleData, INDEX_AM_RESERVED_BIT,
};
use crate::access::common::scankey::{ScanKey, ScanKeyData};
use crate::access::hash::hashsort::HSpool;
use crate::access::index::amapi::{IndexAmRoutine, IndexUniqueCheck};
use crate::access::index::genam::{
    BufferAccessStrategy, IndexBuildResult, IndexBulkDeleteCallback, IndexBulkDeleteResult,
    IndexVacuumInfo, RelationGetIndexScan,
};
use crate::access::rmgrdesc::hashdesc::{
    xl_hash_delete, xl_hash_update_meta_page, SizeOfHashDelete, SizeOfHashUpdateMetaPage,
    XLOG_HASH_DELETE, XLOG_HASH_SPLIT_CLEANUP, XLOG_HASH_UPDATE_META_PAGE,
};
use crate::access::rmgrlist::RM_HASH_ID;
use crate::access::relscan::IndexScanDesc;
use crate::access::sdir::{ForwardScanDirection, ScanDirection};
use crate::access::stratnum::{
    HTEqualStrategyNumber, HTMaxStrategyNumber, InvalidStrategy, StrategyNumber,
};
use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::miscadmin::{END_CRIT_SECTION, START_CRIT_SECTION};
use crate::catalog::pg_class::RELPERSISTENCE_TEMP;
use crate::catalog::pg_type_d::INT4OID;
use crate::commands::progress::PROGRESS_CREATEIDX_TUPLES_TOTAL;
use crate::common::relpath::{ForkNumber, INIT_FORKNUM, MAIN_FORKNUM};
use crate::nodes::execnodes::IndexInfo;
use crate::nodes::nodes::{Cost, Selectivity};
use crate::nodes::pg_list::List;
use crate::utils::fmgr::FunctionCallInfo;
use crate::nodes::tidbitmap::{tbm_add_tuples, TIDBitmap};
use crate::utils::activity::backend_progress::pgstat_progress_update_param;
use crate::pg_config::BLCKSZ;
use crate::storage::block::{BlockNumber, BlockNumberIsValid, InvalidBlockNumber};
use crate::storage::buf::{Buffer, InvalidBuffer};
use crate::storage::bufpage::{
    Page, PageGetItem, PageGetItemId, PageGetMaxOffsetNumber, PageGetSpecialPointer,
    PageIndexMultiDelete, PageSetLSN,
};
use crate::storage::itemptr::{ItemPointer, ItemPointerData};
use crate::storage::off::{
    FirstOffsetNumber, MaxOffsetNumber, OffsetNumber, OffsetNumberNext,
};
use crate::utils::init::globals::{maintenance_work_mem, NBuffers};
use crate::utils::rel::{
    Relation, RelationGetDescr, RelationGetRelationName,
};
use crate::{makeNode, Assert, PG_RETURN_POINTER};

/* ----------------------------------------------------------------
 * hash.h type and constant definitions
 * ----------------------------------------------------------------
 */

/*
 * Mapping from hash bucket number to physical block number of bucket's
 * starting page.  Beware of multiple evaluations of argument!
 */
pub type Bucket = uint32;

pub const InvalidBucket: Bucket = 0xFFFFFFFF as Bucket;

/*
 * Special space for hash index pages.
 */
pub const LH_UNUSED_PAGE: uint16 = 0;
pub const LH_OVERFLOW_PAGE: uint16 = 1 << 0;
pub const LH_BUCKET_PAGE: uint16 = 1 << 1;
pub const LH_BITMAP_PAGE: uint16 = 1 << 2;
pub const LH_META_PAGE: uint16 = 1 << 3;
pub const LH_BUCKET_BEING_POPULATED: uint16 = 1 << 4;
pub const LH_BUCKET_BEING_SPLIT: uint16 = 1 << 5;
pub const LH_BUCKET_NEEDS_SPLIT_CLEANUP: uint16 = 1 << 6;
pub const LH_PAGE_HAS_DEAD_TUPLES: uint16 = 1 << 7;

pub const LH_PAGE_TYPE: uint16 =
    LH_OVERFLOW_PAGE | LH_BUCKET_PAGE | LH_BITMAP_PAGE | LH_META_PAGE;

/*
 * HashPageOpaqueData
 */
#[repr(C)]
pub struct HashPageOpaqueData {
    pub hasho_prevblkno: BlockNumber, /* see above */
    pub hasho_nextblkno: BlockNumber, /* see above */
    pub hasho_bucket: Bucket,         /* bucket number this pg belongs to */
    pub hasho_flag: uint16,           /* page type code + flag bits, see above */
    pub hasho_page_id: uint16,        /* for identification of hash indexes */
}

pub type HashPageOpaque = *mut HashPageOpaqueData;

#[inline]
pub unsafe fn HashPageGetOpaque(page: Page) -> HashPageOpaque {
    PageGetSpecialPointer(page) as HashPageOpaque
}

#[inline]
pub unsafe fn H_NEEDS_SPLIT_CLEANUP(opaque: HashPageOpaque) -> bool {
    ((*opaque).hasho_flag & LH_BUCKET_NEEDS_SPLIT_CLEANUP) != 0
}
#[inline]
pub unsafe fn H_BUCKET_BEING_SPLIT(opaque: HashPageOpaque) -> bool {
    ((*opaque).hasho_flag & LH_BUCKET_BEING_SPLIT) != 0
}
#[inline]
pub unsafe fn H_BUCKET_BEING_POPULATED(opaque: HashPageOpaque) -> bool {
    ((*opaque).hasho_flag & LH_BUCKET_BEING_POPULATED) != 0
}
#[inline]
pub unsafe fn H_HAS_DEAD_TUPLES(opaque: HashPageOpaque) -> bool {
    ((*opaque).hasho_flag & LH_PAGE_HAS_DEAD_TUPLES) != 0
}

pub const HASHO_PAGE_ID: uint16 = 0xFF80;

#[repr(C)]
pub struct HashScanPosItem {
    /* what we remember about each match */
    pub heapTid: ItemPointerData, /* TID of referenced heap item */
    pub indexOffset: OffsetNumber, /* index item's location within page */
}

#[repr(C)]
pub struct HashScanPosData {
    pub buf: Buffer,         /* if valid, the buffer is pinned */
    pub currPage: BlockNumber, /* current hash index page */
    pub nextPage: BlockNumber, /* next overflow page */
    pub prevPage: BlockNumber, /* prev overflow or bucket page */

    pub firstItem: c_int, /* first valid index in items[] */
    pub lastItem: c_int,  /* last valid index in items[] */
    pub itemIndex: c_int, /* current index in items[] */

    pub items: [HashScanPosItem; MaxIndexTuplesPerPage as usize], /* MUST BE LAST */
}

#[inline]
pub unsafe fn HashScanPosIsValid(scanpos: &HashScanPosData) -> bool {
    BlockNumberIsValid(scanpos.currPage)
}

#[inline]
pub unsafe fn HashScanPosInvalidate(scanpos: &mut HashScanPosData) {
    scanpos.buf = InvalidBuffer as Buffer;
    scanpos.currPage = InvalidBlockNumber;
    scanpos.nextPage = InvalidBlockNumber;
    scanpos.prevPage = InvalidBlockNumber;
    scanpos.firstItem = 0;
    scanpos.lastItem = 0;
    scanpos.itemIndex = 0;
}

/*
 *	HashScanOpaqueData is private state for a hash index scan.
 */
#[repr(C)]
pub struct HashScanOpaqueData {
    /* Hash value of the scan key, ie, the hash key we seek */
    pub hashso_sk_hash: uint32,

    /* remember the buffer associated with primary bucket */
    pub hashso_bucket_buf: Buffer,

    /*
     * remember the buffer associated with primary bucket page of bucket being
     * split.
     */
    pub hashso_split_bucket_buf: Buffer,

    /* Whether scan starts on bucket being populated due to split */
    pub hashso_buc_populated: bool,

    /*
     * Whether scanning bucket being split?
     */
    pub hashso_buc_split: bool,
    /* info about killed items if any (killedItems is NULL if never used) */
    pub killedItems: *mut c_int, /* currPos.items indexes of killed items */
    pub numKilled: c_int,        /* number of currently stored items */

    /*
     * Identify all the matching items on a page and save them in
     * HashScanPosData
     */
    pub currPos: HashScanPosData, /* current position data */
}

pub type HashScanOpaque = *mut HashScanOpaqueData;

/*
 * Definitions for metapage.
 */
pub const HASH_METAPAGE: BlockNumber = 0; /* metapage is always block 0 */

pub const HASH_MAGIC: uint32 = 0x6440640;
pub const HASH_VERSION: uint32 = 4;

pub const HASH_SPLITPOINT_PHASE_BITS: uint32 = 2;
pub const HASH_SPLITPOINT_PHASES_PER_GRP: uint32 = 1 << HASH_SPLITPOINT_PHASE_BITS;
pub const HASH_SPLITPOINT_PHASE_MASK: uint32 = HASH_SPLITPOINT_PHASES_PER_GRP - 1;
pub const HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE: uint32 = 10;

/* defines max number of splitpoint phases a hash index can have */
pub const HASH_MAX_SPLITPOINT_GROUP: uint32 = 32;
pub const HASH_MAX_SPLITPOINTS: uint32 = ((HASH_MAX_SPLITPOINT_GROUP
    - HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE)
    * HASH_SPLITPOINT_PHASES_PER_GRP)
    + HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE;

#[repr(C)]
pub struct HashMetaPageData {
    pub hashm_magic: uint32,    /* magic no. for hash tables */
    pub hashm_version: uint32,  /* version ID */
    pub hashm_ntuples: f64,     /* number of tuples stored in the table */
    pub hashm_ffactor: uint16,  /* target fill factor (tuples/bucket) */
    pub hashm_bsize: uint16,    /* index page size (bytes) */
    pub hashm_bmsize: uint16,   /* bitmap array size (bytes) - power of 2 */
    pub hashm_bmshift: uint16,  /* log2(bitmap array size in BITS) */
    pub hashm_maxbucket: uint32, /* ID of maximum bucket in use */
    pub hashm_highmask: uint32, /* mask to modulo into entire table */
    pub hashm_lowmask: uint32,  /* mask to modulo into lower half of table */
    pub hashm_ovflpoint: uint32, /* splitpoint from which ovflpage allocated */
    pub hashm_firstfree: uint32, /* lowest-number free ovflpage (bit#) */
    pub hashm_nmaps: uint32,    /* number of bitmap pages */
    pub hashm_procid: RegProcedure, /* hash function id from pg_proc */
    pub hashm_spares: [uint32; HASH_MAX_SPLITPOINTS as usize], /* spare pages */
    pub hashm_mapp: [BlockNumber; HASH_MAX_BITMAPS as usize], /* blknos of ovfl bitmaps */
}

/* HASH_MAX_BITMAPS = Min(BLCKSZ / 8, 1024) */
pub const HASH_MAX_BITMAPS: usize = {
    let a = (BLCKSZ as usize) / 8;
    if a < 1024 { a } else { 1024 }
};

pub type HashMetaPage = *mut HashMetaPageData;

#[repr(C)]
pub struct HashOptions {
    pub varlena_header_: int32, /* varlena header (do not touch directly!) */
    pub fillfactor: c_int,      /* page fill factor in percent (0..100) */
}

pub const INDEX_MOVED_BY_SPLIT_MASK: uint16 = INDEX_AM_RESERVED_BIT;

pub const HASH_MIN_FILLFACTOR: c_int = 10;
pub const HASH_DEFAULT_FILLFACTOR: c_int = 75;

/*
 * Constants
 */
pub const BYTE_TO_BIT: uint32 = 3; /* 2^3 bits/byte */
pub const ALL_SET: uint32 = !0u32;

/*
 * The number of bits in an ovflpage bitmap word.
 */
pub const BITS_PER_MAP: uint32 = 32; /* Number of bits in uint32 */

/*
 * page-level and high-level locking modes (see README)
 */
pub const HASH_READ: c_int = BUFFER_LOCK_SHARE;
pub const HASH_WRITE: c_int = BUFFER_LOCK_EXCLUSIVE;
pub const HASH_NOLOCK: c_int = -1;

pub const HASHSTANDARD_PROC: c_int = 1;
pub const HASHEXTENDED_PROC: c_int = 2;
pub const HASHOPTIONS_PROC: c_int = 3;
pub const HASHNProcs: c_int = 3;

/*
 * BUCKET_TO_BLKNO(metap,B)
 */
#[inline]
pub unsafe fn BUCKET_TO_BLKNO(metap: HashMetaPage, B: Bucket) -> BlockNumber {
    ((B
        + (if B != 0 {
            (*metap).hashm_spares[(_hash_spareindex(B + 1) - 1) as usize]
        } else {
            0
        })) as BlockNumber)
        + 1
}

/* ----------------------------------------------------------------
 * hash.c implementation
 * ----------------------------------------------------------------
 */

/* Working state for hashbuild and its callback */
#[repr(C)]
struct HashBuildState {
    spool: *mut HSpool,   /* NULL if not using spooling */
    indtuples: f64,       /* # tuples accepted into index */
    heapRel: Relation,    /* heap relation descriptor */
}

/*
 * Hash handler function: return IndexAmRoutine with access method parameters
 * and callbacks.
 */
pub unsafe fn hashhandler(fcinfo: FunctionCallInfo) -> Datum {
    let amroutine: *mut IndexAmRoutine = makeNode!(IndexAmRoutine, T_IndexAmRoutine);

    (*amroutine).amstrategies = HTMaxStrategyNumber as u16;
    (*amroutine).amsupport = HASHNProcs as u16;
    (*amroutine).amoptsprocnum = HASHOPTIONS_PROC as u16;
    (*amroutine).amcanorder = false;
    (*amroutine).amcanorderbyop = false;
    (*amroutine).amcanhash = true;
    (*amroutine).amconsistentequality = true;
    (*amroutine).amconsistentordering = false;
    (*amroutine).amcanbackward = true;
    (*amroutine).amcanunique = false;
    (*amroutine).amcanmulticol = false;
    (*amroutine).amoptionalkey = false;
    (*amroutine).amsearcharray = false;
    (*amroutine).amsearchnulls = false;
    (*amroutine).amstorage = false;
    (*amroutine).amclusterable = false;
    (*amroutine).ampredlocks = true;
    (*amroutine).amcanparallel = false;
    (*amroutine).amcanbuildparallel = false;
    (*amroutine).amcaninclude = false;
    (*amroutine).amusemaintenanceworkmem = false;
    (*amroutine).amsummarizing = false;
    (*amroutine).amparallelvacuumoptions = VACUUM_OPTION_PARALLEL_BULKDEL;
    (*amroutine).amkeytype = INT4OID;

    (*amroutine).ambuild = Some(core::mem::transmute(hashbuild as *const ()));
    (*amroutine).ambuildempty = Some(core::mem::transmute(hashbuildempty as *const ()));
    (*amroutine).aminsert = Some(core::mem::transmute(hashinsert as *const ()));
    (*amroutine).aminsertcleanup = None;
    (*amroutine).ambulkdelete = Some(core::mem::transmute(hashbulkdelete as *const ()));
    (*amroutine).amvacuumcleanup = Some(core::mem::transmute(hashvacuumcleanup as *const ()));
    (*amroutine).amcanreturn = None;
    (*amroutine).amcostestimate = Some(core::mem::transmute(hashcostestimate as *const ()));
    (*amroutine).amgettreeheight = None;
    (*amroutine).amoptions = Some(core::mem::transmute(hashoptions as *const ()));
    (*amroutine).amproperty = None;
    (*amroutine).ambuildphasename = None;
    (*amroutine).amvalidate = Some(core::mem::transmute(hashvalidate as *const ()));
    (*amroutine).amadjustmembers = Some(core::mem::transmute(hashadjustmembers as *const ()));
    (*amroutine).ambeginscan = Some(core::mem::transmute(hashbeginscan as *const ()));
    (*amroutine).amrescan = Some(core::mem::transmute(hashrescan as *const ()));
    (*amroutine).amgettuple = Some(core::mem::transmute(hashgettuple as *const ()));
    (*amroutine).amgetbitmap = Some(core::mem::transmute(hashgetbitmap as *const ()));
    (*amroutine).amendscan = Some(core::mem::transmute(hashendscan as *const ()));
    (*amroutine).ammarkpos = None;
    (*amroutine).amrestrpos = None;
    (*amroutine).amestimateparallelscan = None;
    (*amroutine).aminitparallelscan = None;
    (*amroutine).amparallelrescan = None;
    (*amroutine).amtranslatestrategy = Some(core::mem::transmute(hashtranslatestrategy as *const ()));
    (*amroutine).amtranslatecmptype = Some(core::mem::transmute(hashtranslatecmptype as *const ()));

    PG_RETURN_POINTER!(amroutine as *mut c_void)
}

/*
 *	hashbuild() -- build a new hash index.
 */
#[no_mangle]
pub unsafe extern "C" fn hashbuild(
    heap: Relation,
    index: Relation,
    indexInfo: *mut IndexInfo,
) -> *mut IndexBuildResult {
    let result: *mut IndexBuildResult;
    let mut relpages: BlockNumber = 0;
    let mut reltuples: f64 = 0.0;
    let mut allvisfrac: f64 = 0.0;
    let num_buckets: uint32;
    let mut sort_threshold: Size;
    let mut buildstate: HashBuildState = std::mem::zeroed();

    /*
     * We expect to be called exactly once for any index relation. If that's
     * not the case, big trouble's what we have.
     */
    if RelationGetNumberOfBlocks(index) != 0 {
        elog!(
            ERROR,
            "index \"{}\" already contains data",
            core::ffi::CStr::from_ptr(RelationGetRelationName(index)).to_string_lossy()
        );
    }

    /* Estimate the number of rows currently present in the table */
    estimate_rel_size(
        heap,
        std::ptr::null_mut(),
        &mut relpages,
        &mut reltuples,
        &mut allvisfrac,
    );

    /* Initialize the hash index metadata page and initial buckets */
    num_buckets = _hash_init(index, reltuples, MAIN_FORKNUM);

    /*
     * If we just insert the tuples into the index in scan order, then ... we
     * can sort the tuples by (expected) bucket number.  We choose to sort if
     * the initial index size exceeds maintenance_work_mem, or the number of
     * buffers usable for the index, whichever is less.
     */
    sort_threshold = (maintenance_work_mem as Size * 1024 as Size) / BLCKSZ as Size;
    if (*(*index).rd_rel).relpersistence != RELPERSISTENCE_TEMP {
        sort_threshold = Min(sort_threshold, NBuffers as Size);
    } else {
        sort_threshold = Min(sort_threshold, NLocBuffer as Size);
    }

    if num_buckets as Size >= sort_threshold {
        buildstate.spool = _h_spoolinit(heap, index, num_buckets);
    } else {
        buildstate.spool = std::ptr::null_mut();
    }

    /* prepare to build the index */
    buildstate.indtuples = 0.0;
    buildstate.heapRel = heap;

    /* do the heap scan */
    reltuples = table_index_build_scan(
        heap,
        index,
        indexInfo,
        true,
        true,
        Some(hashbuildCallback),
        &mut buildstate as *mut _ as *mut c_void,
        std::ptr::null_mut(),
    );
    pgstat_progress_update_param(
        PROGRESS_CREATEIDX_TUPLES_TOTAL,
        buildstate.indtuples as int64,
    );

    if !buildstate.spool.is_null() {
        /* sort the tuples and insert them into the index */
        _h_indexbuild(buildstate.spool, buildstate.heapRel);
        _h_spooldestroy(buildstate.spool);
    }

    /*
     * Return statistics
     */
    result = palloc(std::mem::size_of::<IndexBuildResult>()) as *mut IndexBuildResult;

    (*result).heap_tuples = reltuples;
    (*result).index_tuples = buildstate.indtuples;

    result
}

/*
 *	hashbuildempty() -- build an empty hash index in the initialization fork
 */
#[no_mangle]
pub unsafe extern "C" fn hashbuildempty(index: Relation) {
    _hash_init(index, 0.0, INIT_FORKNUM);
}

/*
 * Per-tuple callback for table_index_build_scan
 */
unsafe extern "C" fn hashbuildCallback(
    index: Relation,
    tid: ItemPointer,
    values: *mut Datum,
    isnull: *mut bool,
    _tupleIsAlive: bool,
    state: *mut c_void,
) {
    let buildstate = state as *mut HashBuildState;
    let mut index_values: [Datum; 1] = [0; 1];
    let mut index_isnull: [bool; 1] = [false; 1];
    let itup: IndexTuple;

    /* convert data to a hash key; on failure, do not insert anything */
    if !_hash_convert_tuple(
        index,
        values,
        isnull,
        index_values.as_mut_ptr(),
        index_isnull.as_mut_ptr(),
    ) {
        return;
    }

    /* Either spool the tuple for sorting, or just put it into the index */
    if !(*buildstate).spool.is_null() {
        _h_spool(
            (*buildstate).spool,
            tid,
            index_values.as_ptr(),
            index_isnull.as_ptr(),
        );
    } else {
        /* form an index tuple and point it at the heap tuple */
        itup = index_form_tuple(
            RelationGetDescr(index),
            index_values.as_mut_ptr(),
            index_isnull.as_mut_ptr(),
        );
        (*itup).t_tid = *tid;
        _hash_doinsert(index, itup, (*buildstate).heapRel, false);
        pfree(itup as *mut c_void);
    }

    (*buildstate).indtuples += 1.0;
}

/*
 *	hashinsert() -- insert an index tuple into a hash table.
 */
#[no_mangle]
pub unsafe extern "C" fn hashinsert(
    rel: Relation,
    values: *mut Datum,
    isnull: *mut bool,
    ht_ctid: ItemPointer,
    heapRel: Relation,
    _checkUnique: IndexUniqueCheck,
    _indexUnchanged: bool,
    _indexInfo: *mut IndexInfo,
) -> bool {
    let mut index_values: [Datum; 1] = [0; 1];
    let mut index_isnull: [bool; 1] = [false; 1];
    let itup: IndexTuple;

    /* convert data to a hash key; on failure, do not insert anything */
    if !_hash_convert_tuple(
        rel,
        values,
        isnull,
        index_values.as_mut_ptr(),
        index_isnull.as_mut_ptr(),
    ) {
        return false;
    }

    /* form an index tuple and point it at the heap tuple */
    itup = index_form_tuple(
        RelationGetDescr(rel),
        index_values.as_mut_ptr(),
        index_isnull.as_mut_ptr(),
    );
    (*itup).t_tid = *ht_ctid;

    _hash_doinsert(rel, itup, heapRel, false);

    pfree(itup as *mut c_void);

    false
}

/*
 *	hashgettuple() -- Get the next tuple in the scan.
 */
#[no_mangle]
pub unsafe extern "C" fn hashgettuple(scan: IndexScanDesc, dir: ScanDirection) -> bool {
    let so = (*scan).opaque as HashScanOpaque;
    let res: bool;

    /* Hash indexes are always lossy since we store only the hash code */
    (*scan).xs_recheck = true;

    /*
     * If we've already initialized this scan, we can just advance it in the
     * appropriate direction.
     */
    if !HashScanPosIsValid(&(*so).currPos) {
        res = _hash_first(scan, dir);
    } else {
        /*
         * Check to see if we should kill the previously-fetched tuple.
         */
        if (*scan).kill_prior_tuple {
            /*
             * Yes, so remember it for later.
             */
            if (*so).killedItems.is_null() {
                (*so).killedItems =
                    palloc(MaxIndexTuplesPerPage as usize * std::mem::size_of::<c_int>())
                        as *mut c_int;
            }

            if (*so).numKilled < MaxIndexTuplesPerPage {
                *(*so).killedItems.offset((*so).numKilled as isize) =
                    (*so).currPos.itemIndex;
                (*so).numKilled += 1;
            }
        }

        /*
         * Now continue the scan.
         */
        res = _hash_next(scan, dir);
    }

    res
}

/*
 *	hashgetbitmap() -- get all tuples at once
 */
#[no_mangle]
pub unsafe extern "C" fn hashgetbitmap(scan: IndexScanDesc, tbm: *mut TIDBitmap) -> int64 {
    let so = (*scan).opaque as HashScanOpaque;
    let mut res: bool;
    let mut ntids: int64 = 0;
    let currItem: *mut HashScanPosItem;

    res = _hash_first(scan, ForwardScanDirection);

    while res {
        let currItem =
            &mut (*so).currPos.items[(*so).currPos.itemIndex as usize] as *mut HashScanPosItem;

        /*
         * _hash_first and _hash_next handle eliminate dead index entries
         * whenever scan->ignore_killed_tuples is true.
         */
        tbm_add_tuples(tbm, &mut (*currItem).heapTid, 1, true);
        ntids += 1;

        res = _hash_next(scan, ForwardScanDirection);
    }

    let _ = currItem;
    ntids
}

/*
 *	hashbeginscan() -- start a scan on a hash index
 */
#[no_mangle]
pub unsafe extern "C" fn hashbeginscan(
    rel: Relation,
    nkeys: c_int,
    norderbys: c_int,
) -> IndexScanDesc {
    let scan: IndexScanDesc;
    let so: HashScanOpaque;

    /* no order by operators allowed */
    Assert!(norderbys == 0);

    scan = RelationGetIndexScan(rel, nkeys, norderbys);

    so = palloc(std::mem::size_of::<HashScanOpaqueData>()) as HashScanOpaque;
    HashScanPosInvalidate(&mut (*so).currPos);
    (*so).hashso_bucket_buf = InvalidBuffer as Buffer;
    (*so).hashso_split_bucket_buf = InvalidBuffer as Buffer;

    (*so).hashso_buc_populated = false;
    (*so).hashso_buc_split = false;

    (*so).killedItems = std::ptr::null_mut();
    (*so).numKilled = 0;

    (*scan).opaque = so as *mut c_void;

    scan
}

/*
 *	hashrescan() -- rescan an index relation
 */
#[no_mangle]
pub unsafe extern "C" fn hashrescan(
    scan: IndexScanDesc,
    scankey: ScanKey,
    _nscankeys: c_int,
    _orderbys: ScanKey,
    _norderbys: c_int,
) {
    let so = (*scan).opaque as HashScanOpaque;
    let rel = (*scan).indexRelation;

    if HashScanPosIsValid(&(*so).currPos) {
        /* Before leaving current page, deal with any killed items */
        if (*so).numKilled > 0 {
            _hash_kill_items(scan);
        }
    }

    _hash_dropscanbuf(rel, so);

    /* set position invalid (this will cause _hash_first call) */
    HashScanPosInvalidate(&mut (*so).currPos);

    /* Update scan key, if a new one is given */
    if !scankey.is_null() && (*scan).numberOfKeys > 0 {
        memcpy(
            (*scan).keyData as *mut c_void,
            scankey as *const c_void,
            (*scan).numberOfKeys as usize * std::mem::size_of::<ScanKeyData>(),
        );
    }

    (*so).hashso_buc_populated = false;
    (*so).hashso_buc_split = false;
}

/*
 *	hashendscan() -- close down a scan
 */
#[no_mangle]
pub unsafe extern "C" fn hashendscan(scan: IndexScanDesc) {
    let so = (*scan).opaque as HashScanOpaque;
    let rel = (*scan).indexRelation;

    if HashScanPosIsValid(&(*so).currPos) {
        /* Before leaving current page, deal with any killed items */
        if (*so).numKilled > 0 {
            _hash_kill_items(scan);
        }
    }

    _hash_dropscanbuf(rel, so);

    if !(*so).killedItems.is_null() {
        pfree((*so).killedItems as *mut c_void);
    }
    pfree(so as *mut c_void);
    (*scan).opaque = std::ptr::null_mut();
}

/*
 * Bulk deletion of all index entries pointing to a set of heap tuples.
 *
 * Result: a palloc'd struct containing statistical info for VACUUM displays.
 */
#[no_mangle]
pub unsafe extern "C" fn hashbulkdelete(
    info: *mut IndexVacuumInfo,
    mut stats: *mut IndexBulkDeleteResult,
    callback: IndexBulkDeleteCallback,
    callback_state: *mut c_void,
) -> *mut IndexBulkDeleteResult {
    let rel = (*info).index;
    let mut tuples_removed: f64;
    let mut num_index_tuples: f64;
    let orig_ntuples: f64;
    let orig_maxbucket: Bucket;
    let mut cur_maxbucket: Bucket;
    let mut cur_bucket: Bucket;
    let mut metabuf: Buffer = InvalidBuffer as Buffer;
    let mut metap: HashMetaPage;
    let mut cachedmetap: HashMetaPage;

    tuples_removed = 0.0;
    num_index_tuples = 0.0;

    /*
     * We need a copy of the metapage so that we can use its hashm_spares[]
     * values to compute bucket page addresses, but a cached copy should be
     * good enough.
     */
    cachedmetap = _hash_getcachedmetap(rel, &mut metabuf, false);
    Assert!(!cachedmetap.is_null());

    orig_maxbucket = (*cachedmetap).hashm_maxbucket;
    orig_ntuples = (*cachedmetap).hashm_ntuples;

    /* Scan the buckets that we know exist */
    cur_bucket = 0;
    cur_maxbucket = orig_maxbucket;

    'loop_top: loop {
        while cur_bucket <= cur_maxbucket {
            let bucket_blkno: BlockNumber;
            let blkno: BlockNumber;
            let bucket_buf: Buffer;
            let buf: Buffer;
            let bucket_opaque: HashPageOpaque;
            let page: Page;
            let mut split_cleanup: bool = false;

            /* Get address of bucket's start page */
            bucket_blkno = BUCKET_TO_BLKNO(cachedmetap, cur_bucket);

            blkno = bucket_blkno;

            /*
             * We need to acquire a cleanup lock on the primary bucket page to
             * out wait concurrent scans before deleting the dead tuples.
             */
            buf = ReadBufferExtended(
                rel,
                MAIN_FORKNUM,
                blkno,
                RBM_NORMAL,
                (*info).strategy,
            );
            LockBufferForCleanup(buf);
            _hash_checkpage(rel, buf, LH_BUCKET_PAGE as c_int);

            page = BufferGetPage(buf);
            bucket_opaque = HashPageGetOpaque(page);

            /*
             * If the bucket contains tuples that are moved by split, then we
             * need to delete such tuples.
             */
            if !H_BUCKET_BEING_SPLIT(bucket_opaque)
                && H_NEEDS_SPLIT_CLEANUP(bucket_opaque)
            {
                split_cleanup = true;

                /*
                 * This bucket might have been split since we last held a lock
                 * on the metapage.
                 */
                Assert!((*bucket_opaque).hasho_prevblkno != InvalidBlockNumber);
                if (*bucket_opaque).hasho_prevblkno > (*cachedmetap).hashm_maxbucket {
                    cachedmetap = _hash_getcachedmetap(rel, &mut metabuf, true);
                    Assert!(!cachedmetap.is_null());
                }
            }

            bucket_buf = buf;

            hashbucketcleanup(
                rel,
                cur_bucket,
                bucket_buf,
                blkno,
                (*info).strategy,
                (*cachedmetap).hashm_maxbucket,
                (*cachedmetap).hashm_highmask,
                (*cachedmetap).hashm_lowmask,
                &mut tuples_removed,
                &mut num_index_tuples,
                split_cleanup,
                callback,
                callback_state,
            );

            _hash_dropbuf(rel, bucket_buf);

            /* Advance to next bucket */
            cur_bucket += 1;
        }

        if BufferIsInvalid(metabuf) {
            metabuf = _hash_getbuf(rel, HASH_METAPAGE, HASH_NOLOCK, LH_META_PAGE as c_int);
        }

        /* Write-lock metapage and check for split since we started */
        LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
        metap = HashPageGetMeta(BufferGetPage(metabuf));

        if cur_maxbucket != (*metap).hashm_maxbucket {
            /* There's been a split, so process the additional bucket(s) */
            LockBuffer(metabuf, BUFFER_LOCK_UNLOCK);
            cachedmetap = _hash_getcachedmetap(rel, &mut metabuf, true);
            Assert!(!cachedmetap.is_null());
            cur_maxbucket = (*cachedmetap).hashm_maxbucket;
            continue 'loop_top;
        }

        /* Okay, we're really done.  Update tuple count in metapage. */
        START_CRIT_SECTION();

        if orig_maxbucket == (*metap).hashm_maxbucket
            && orig_ntuples == (*metap).hashm_ntuples
        {
            /*
             * No one has split or inserted anything since start of scan, so
             * believe our count as gospel.
             */
            (*metap).hashm_ntuples = num_index_tuples;
        } else {
            /*
             * Otherwise, our count is untrustworthy since we may have
             * double-scanned tuples in split buckets.  Proceed by
             * dead-reckoning.
             */
            if (*metap).hashm_ntuples > tuples_removed {
                (*metap).hashm_ntuples -= tuples_removed;
            } else {
                (*metap).hashm_ntuples = 0.0;
            }
            num_index_tuples = (*metap).hashm_ntuples;
        }

        MarkBufferDirty(metabuf);

        /* XLOG stuff */
        if RelationNeedsWAL(rel) {
            let mut xlrec: xl_hash_update_meta_page = std::mem::zeroed();
            let recptr: XLogRecPtr;

            xlrec.ntuples = (*metap).hashm_ntuples;

            XLogBeginInsert();
            XLogRegisterData(
                &mut xlrec as *mut _ as *mut c_void,
                SizeOfHashUpdateMetaPage as c_int,
            );

            XLogRegisterBuffer(0, metabuf, REGBUF_STANDARD as uint8);

            recptr = XLogInsert(RM_HASH_ID, XLOG_HASH_UPDATE_META_PAGE);
            PageSetLSN(BufferGetPage(metabuf), recptr);
        }

        END_CRIT_SECTION();

        _hash_relbuf(rel, metabuf);

        break;
    }

    /* return statistics */
    if stats.is_null() {
        stats = palloc0(std::mem::size_of::<IndexBulkDeleteResult>())
            as *mut IndexBulkDeleteResult;
    }
    (*stats).estimated_count = false;
    (*stats).num_index_tuples = num_index_tuples;
    (*stats).tuples_removed += tuples_removed;
    /* hashvacuumcleanup will fill in num_pages */

    stats
}

/*
 * Post-VACUUM cleanup.
 *
 * Result: a palloc'd struct containing statistical info for VACUUM displays.
 */
#[no_mangle]
pub unsafe extern "C" fn hashvacuumcleanup(
    info: *mut IndexVacuumInfo,
    stats: *mut IndexBulkDeleteResult,
) -> *mut IndexBulkDeleteResult {
    let rel = (*info).index;
    let num_pages: BlockNumber;

    /* If hashbulkdelete wasn't called, return NULL signifying no change */
    /* Note: this covers the analyze_only case too */
    if stats.is_null() {
        return std::ptr::null_mut();
    }

    /* update statistics */
    num_pages = RelationGetNumberOfBlocks(rel);
    (*stats).num_pages = num_pages;

    stats
}

/*
 * Helper function to perform deletion of index entries from a bucket.
 *
 * This function expects that the caller has acquired a cleanup lock on the
 * primary bucket page, and will return with a write lock again held on the
 * primary bucket page.
 */
#[no_mangle]
pub unsafe extern "C" fn hashbucketcleanup(
    rel: Relation,
    cur_bucket: Bucket,
    bucket_buf: Buffer,
    bucket_blkno: BlockNumber,
    bstrategy: BufferAccessStrategy,
    maxbucket: uint32,
    highmask: uint32,
    lowmask: uint32,
    tuples_removed: *mut f64,
    num_index_tuples: *mut f64,
    split_cleanup: bool,
    callback: IndexBulkDeleteCallback,
    callback_state: *mut c_void,
) {
    let mut blkno: BlockNumber;
    let mut buf: Buffer;
    let mut new_bucket: Bucket = InvalidBucket;
    let mut bucket_dirty: bool = false;

    blkno = bucket_blkno;
    buf = bucket_buf;

    if split_cleanup {
        new_bucket =
            _hash_get_newbucket_from_oldbucket(rel, cur_bucket, lowmask, maxbucket);
    }

    /* Scan each page in bucket */
    loop {
        let opaque: HashPageOpaque;
        let mut offno: OffsetNumber;
        let maxoffno: OffsetNumber;
        let next_buf: Buffer;
        let page: Page;
        let mut deletable: [OffsetNumber; MaxOffsetNumber as usize] =
            [0; MaxOffsetNumber as usize];
        let mut ndeletable: c_int = 0;
        let retain_pin: bool;
        let mut clear_dead_marking: bool = false;

        vacuum_delay_point(false);

        page = BufferGetPage(buf);
        opaque = HashPageGetOpaque(page);

        /* Scan each tuple in page */
        maxoffno = PageGetMaxOffsetNumber(page);
        offno = FirstOffsetNumber;
        while offno <= maxoffno {
            let htup: ItemPointer;
            let itup: IndexTuple;
            let bucket: Bucket;
            let mut kill_tuple: bool = false;

            itup = PageGetItem(page, PageGetItemId(page, offno)) as IndexTuple;
            htup = &mut (*itup).t_tid as *mut ItemPointerData;

            /*
             * To remove the dead tuples, we strictly want to rely on results
             * of callback function.  refer btvacuumpage for detailed reason.
             */
            if callback.is_some() && callback.unwrap()(htup as _, callback_state) {
                kill_tuple = true;
                if !tuples_removed.is_null() {
                    *tuples_removed += 1.0;
                }
            } else if split_cleanup {
                /* delete the tuples that are moved by split. */
                bucket = _hash_hashkey2bucket(
                    _hash_get_indextuple_hashkey(itup),
                    maxbucket,
                    highmask,
                    lowmask,
                );
                /* mark the item for deletion */
                if bucket != cur_bucket {
                    /*
                     * We expect tuples to either belong to current bucket or
                     * new_bucket.
                     */
                    Assert!(bucket == new_bucket);
                    kill_tuple = true;
                }
            }

            if kill_tuple {
                /* mark the item for deletion */
                deletable[ndeletable as usize] = offno;
                ndeletable += 1;
            } else {
                /* we're keeping it, so count it */
                if !num_index_tuples.is_null() {
                    *num_index_tuples += 1.0;
                }
            }

            offno = OffsetNumberNext(offno);
        }

        /* retain the pin on primary bucket page till end of bucket scan */
        if blkno == bucket_blkno {
            retain_pin = true;
        } else {
            retain_pin = false;
        }

        blkno = (*opaque).hasho_nextblkno;

        /*
         * Apply deletions, advance to next page and write page if needed.
         */
        if ndeletable > 0 {
            /* No ereport(ERROR) until changes are logged */
            START_CRIT_SECTION();

            PageIndexMultiDelete(page, deletable.as_mut_ptr(), ndeletable);
            bucket_dirty = true;

            /*
             * Let us mark the page as clean if vacuum removes the DEAD tuples
             * from an index page.
             */
            if !tuples_removed.is_null()
                && *tuples_removed > 0.0
                && H_HAS_DEAD_TUPLES(opaque)
            {
                (*opaque).hasho_flag &= !LH_PAGE_HAS_DEAD_TUPLES;
                clear_dead_marking = true;
            }

            MarkBufferDirty(buf);

            /* XLOG stuff */
            if RelationNeedsWAL(rel) {
                let mut xlrec: xl_hash_delete = std::mem::zeroed();
                let recptr: XLogRecPtr;

                xlrec.clear_dead_marking = clear_dead_marking;
                xlrec.is_primary_bucket_page = buf == bucket_buf;

                XLogBeginInsert();
                XLogRegisterData(
                    &mut xlrec as *mut _ as *mut c_void,
                    SizeOfHashDelete as c_int,
                );

                /*
                 * bucket buffer was not changed, but still needs to be
                 * registered to ensure that we can acquire a cleanup lock on
                 * it during replay.
                 */
                if !xlrec.is_primary_bucket_page {
                    let flags: uint8 =
                        (REGBUF_STANDARD | REGBUF_NO_IMAGE | REGBUF_NO_CHANGE) as uint8;

                    XLogRegisterBuffer(0, bucket_buf, flags);
                }

                XLogRegisterBuffer(1, buf, REGBUF_STANDARD as uint8);
                XLogRegisterBufData(
                    1,
                    deletable.as_mut_ptr() as *mut c_void,
                    ndeletable as usize * std::mem::size_of::<OffsetNumber>(),
                );

                recptr = XLogInsert(RM_HASH_ID, XLOG_HASH_DELETE);
                PageSetLSN(BufferGetPage(buf), recptr);
            }

            END_CRIT_SECTION();
        }

        /* bail out if there are no more pages to scan. */
        if !BlockNumberIsValid(blkno) {
            break;
        }

        next_buf = _hash_getbuf_with_strategy(
            rel,
            blkno,
            HASH_WRITE,
            LH_OVERFLOW_PAGE as c_int,
            bstrategy,
        );

        /*
         * release the lock on previous page after acquiring the lock on next
         * page
         */
        if retain_pin {
            LockBuffer(buf, BUFFER_LOCK_UNLOCK);
        } else {
            _hash_relbuf(rel, buf);
        }

        buf = next_buf;
    }

    /*
     * lock the bucket page to clear the garbage flag and squeeze the bucket.
     */
    if buf != bucket_buf {
        _hash_relbuf(rel, buf);
        LockBuffer(bucket_buf, BUFFER_LOCK_EXCLUSIVE);
    }

    /*
     * Clear the garbage flag from bucket after deleting the tuples that are
     * moved by split.
     */
    if split_cleanup {
        let bucket_opaque: HashPageOpaque;
        let page: Page;

        page = BufferGetPage(bucket_buf);
        bucket_opaque = HashPageGetOpaque(page);

        /* No ereport(ERROR) until changes are logged */
        START_CRIT_SECTION();

        (*bucket_opaque).hasho_flag &= !LH_BUCKET_NEEDS_SPLIT_CLEANUP;
        MarkBufferDirty(bucket_buf);

        /* XLOG stuff */
        if RelationNeedsWAL(rel) {
            let recptr: XLogRecPtr;

            XLogBeginInsert();
            XLogRegisterBuffer(0, bucket_buf, REGBUF_STANDARD as uint8);

            recptr = XLogInsert(RM_HASH_ID, XLOG_HASH_SPLIT_CLEANUP);
            PageSetLSN(page, recptr);
        }

        END_CRIT_SECTION();
    }

    /*
     * If we have deleted anything, try to compact free space.
     */
    if bucket_dirty && IsBufferCleanupOK(bucket_buf) {
        _hash_squeezebucket(rel, cur_bucket, bucket_blkno, bucket_buf, bstrategy);
    } else {
        LockBuffer(bucket_buf, BUFFER_LOCK_UNLOCK);
    }

    let _ = new_bucket;
}

#[no_mangle]
pub unsafe extern "C" fn hashtranslatestrategy(
    strategy: StrategyNumber,
    _opfamily: Oid,
) -> CompareType {
    if strategy == HTEqualStrategyNumber {
        return COMPARE_EQ;
    }
    COMPARE_INVALID
}

#[no_mangle]
pub unsafe extern "C" fn hashtranslatecmptype(
    cmptype: CompareType,
    _opfamily: Oid,
) -> StrategyNumber {
    if cmptype == COMPARE_EQ {
        return HTEqualStrategyNumber;
    }
    InvalidStrategy
}

/* ----------------------------------------------------------------
 * Local stubs / constants for as-yet unported helpers
 * ----------------------------------------------------------------
 */

/* itup.h - max index tuples per page (deferred). */
const MaxIndexTuplesPerPage: c_int = 407;

/* miscadmin.h - number of local buffers (not yet ported). */
static mut NLocBuffer: c_int = 0;

/* commands/vacuum.h - parallel vacuum options (not yet ported / orphan file). */
const VACUUM_OPTION_PARALLEL_BULKDEL: uint8 = 1 << 1;

/* bufmgr.h buffer-lock modes (not yet ported). */
const BUFFER_LOCK_UNLOCK: c_int = 0;
const BUFFER_LOCK_SHARE: c_int = 1;
const BUFFER_LOCK_EXCLUSIVE: c_int = 2;

/* bufmgr.h ReadBufferMode (not yet ported). */
const RBM_NORMAL: c_int = 0;

/* xloginsert.h REGBUF flags (not yet ported). */
const REGBUF_STANDARD: c_int = 0x04;
const REGBUF_NO_IMAGE: c_int = 0x01;
const REGBUF_NO_CHANGE: c_int = 0x40;

unsafe fn HashPageGetMeta(_page: Page) -> HashMetaPage { crate::access::hash::hashutil::HashPageGetMeta(_page) as _ }
unsafe fn BufferGetPage(_buffer: Buffer) -> Page {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn BufferIsInvalid(_buffer: Buffer) -> bool { crate::storage::buf::BufferIsInvalid(_buffer) }
unsafe fn ReadBufferExtended(
    _reln: Relation,
    _forkNum: ForkNumber,
    _blockNum: BlockNumber,
    _mode: c_int,
    _strategy: BufferAccessStrategy,
) -> Buffer {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn LockBuffer(_buffer: Buffer, _mode: c_int) {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn LockBufferForCleanup(_buffer: Buffer) {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn IsBufferCleanupOK(_buffer: Buffer) -> bool { crate::storage::buffer::bufmgr::IsBufferCleanupOK(_buffer) }
unsafe fn MarkBufferDirty(_buffer: Buffer) {
    unimplemented!() // TODO: storage/bufmgr.h
}
unsafe fn RelationNeedsWAL(_relation: Relation) -> bool { crate::access::nbtree::nbtdedup::RelationNeedsWAL(_relation) }
unsafe fn RelationGetNumberOfBlocks(_relation: Relation) -> BlockNumber { crate::access::nbtree::nbtpage::RelationGetNumberOfBlocks(_relation) }
unsafe fn estimate_rel_size(
    rel: Relation,
    attr_widths: *mut int32,
    pages: *mut BlockNumber,
    tuples: *mut f64,
    allvisfrac: *mut f64,
) {
    crate::optimizer::util::plancat::estimate_rel_size_local(rel as _, attr_widths, pages, tuples, allvisfrac)
}
unsafe fn table_index_build_scan(
    heap: Relation,
    index: Relation,
    indexInfo: *mut IndexInfo,
    allow_sync: bool,
    progress: bool,
    callback: Option<
        unsafe extern "C" fn(Relation, ItemPointer, *mut Datum, *mut bool, bool, *mut c_void),
    >,
    callback_state: *mut c_void,
    scan: *mut c_void,
) -> f64 {
    let am = (*heap).rd_tableam as *const crate::access::table::tableam::TableAmRoutine;
    ((*am).index_build_range_scan.unwrap())(
        heap as _, index as _, indexInfo as _, allow_sync, false, progress,
        0, !0u32, core::mem::transmute(callback), callback_state, scan as _,
    )
}
unsafe fn vacuum_delay_point(_is_analyze: bool) { crate::commands::vacuum::vacuum_delay_point(_is_analyze) }
unsafe fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void {
    core::ptr::copy_nonoverlapping(src as *const u8, dest as *mut u8, n); dest
}
unsafe fn XLogBeginInsert() {
    unimplemented!() // TODO: access/xloginsert.h
}
unsafe fn XLogRegisterData(_data: *mut c_void, _len: c_int) {
    unimplemented!() // TODO: access/xloginsert.h
}
unsafe fn XLogRegisterBuffer(_block_id: uint8, _buffer: Buffer, _flags: uint8) {
    unimplemented!() // TODO: access/xloginsert.h
}
unsafe fn XLogRegisterBufData(_block_id: uint8, _data: *mut c_void, _len: usize) {
    unimplemented!() // TODO: access/xloginsert.h
}
unsafe fn XLogInsert(_rmid: u8, _info: uint8) -> XLogRecPtr {
    unimplemented!() // TODO: access/xloginsert.h
}

unsafe fn _hash_spareindex(_num_bucket: uint32) -> uint32 { crate::access::hash::hashutil::_hash_spareindex(_num_bucket) }
unsafe fn _hash_init(_rel: Relation, _num_tuples: f64, _forkNum: ForkNumber) -> uint32 { crate::access::hash::hashpage::_hash_init(_rel, _num_tuples, _forkNum) }
unsafe fn _h_spoolinit(_heap: Relation, _index: Relation, _num_buckets: uint32) -> *mut HSpool { crate::access::hash::hashsort::_h_spoolinit(_heap, _index, _num_buckets) }
unsafe fn _h_spooldestroy(_hspool: *mut HSpool) { crate::access::hash::hashsort::_h_spooldestroy(_hspool) }
unsafe fn _h_spool(
    _hspool: *mut HSpool,
    _self_: ItemPointer,
    _values: *const Datum,
    _isnull: *const bool,
) { crate::access::hash::hashsort::_h_spool(_hspool, _self_, _values, _isnull) }
unsafe fn _h_indexbuild(_hspool: *mut HSpool, _heapRel: Relation) { crate::access::hash::hashsort::_h_indexbuild(_hspool, _heapRel) }
unsafe fn _hash_convert_tuple(
    _index: Relation,
    _user_values: *mut Datum,
    _user_isnull: *mut bool,
    _index_values: *mut Datum,
    _index_isnull: *mut bool,
) -> bool { crate::access::hash::hashutil::_hash_convert_tuple(_index, _user_values, _user_isnull, _index_values, _index_isnull) }
unsafe fn _hash_doinsert(_rel: Relation, _itup: IndexTuple, _heapRel: Relation, _sorted: bool) { crate::access::hash::hashinsert::_hash_doinsert(_rel, _itup, _heapRel, _sorted) }
unsafe fn _hash_first(_scan: IndexScanDesc, _dir: ScanDirection) -> bool { crate::access::hash::hashsearch::_hash_first(_scan, _dir) }
unsafe fn _hash_next(_scan: IndexScanDesc, _dir: ScanDirection) -> bool { crate::access::hash::hashsearch::_hash_next(_scan, _dir) }
unsafe fn _hash_kill_items(_scan: IndexScanDesc) { crate::access::hash::hashutil::_hash_kill_items(_scan) }
unsafe fn _hash_dropscanbuf(_rel: Relation, _so: HashScanOpaque) { crate::access::hash::hashpage::_hash_dropscanbuf(_rel, _so) }
unsafe fn _hash_getcachedmetap(
    _rel: Relation,
    _metabuf: *mut Buffer,
    _force_refresh: bool,
) -> HashMetaPage { crate::access::hash::hashpage::_hash_getcachedmetap(_rel, _metabuf, _force_refresh) as _ }
unsafe fn _hash_getbuf(_rel: Relation, _blkno: BlockNumber, _access: c_int, _flags: c_int) -> Buffer { crate::access::hash::hashpage::_hash_getbuf(_rel, _blkno, _access, _flags) }
unsafe fn _hash_getbuf_with_strategy(
    _rel: Relation,
    _blkno: BlockNumber,
    _access: c_int,
    _flags: c_int,
    _bstrategy: BufferAccessStrategy,
) -> Buffer { crate::access::hash::hashpage::_hash_getbuf_with_strategy(_rel, _blkno, _access, _flags, _bstrategy as _) }
unsafe fn _hash_relbuf(_rel: Relation, _buf: Buffer) { crate::access::hash::hashpage::_hash_relbuf(_rel, _buf) }
unsafe fn _hash_dropbuf(_rel: Relation, _buf: Buffer) { crate::access::hash::hashpage::_hash_dropbuf(_rel, _buf) }
unsafe fn _hash_checkpage(_rel: Relation, _buf: Buffer, _flags: c_int) { crate::access::hash::hashutil::_hash_checkpage(_rel, _buf, _flags) }
unsafe fn _hash_squeezebucket(
    _rel: Relation,
    _bucket: Bucket,
    _bucket_blkno: BlockNumber,
    _bucket_buf: Buffer,
    _bstrategy: BufferAccessStrategy,
) { crate::access::hash::hashovfl::_hash_squeezebucket(_rel, _bucket, _bucket_blkno, _bucket_buf, _bstrategy as _) }
unsafe fn _hash_hashkey2bucket(
    _hashkey: uint32,
    _maxbucket: uint32,
    _highmask: uint32,
    _lowmask: uint32,
) -> Bucket { crate::access::hash::hashutil::_hash_hashkey2bucket(_hashkey, _maxbucket, _highmask, _lowmask) }
unsafe fn _hash_get_indextuple_hashkey(_itup: IndexTuple) -> uint32 { crate::access::hash::hashutil::_hash_get_indextuple_hashkey(_itup) }
unsafe fn _hash_get_newbucket_from_oldbucket(
    _rel: Relation,
    _old_bucket: Bucket,
    _lowmask: uint32,
    _maxbucket: uint32,
) -> Bucket { crate::access::hash::hashutil::_hash_get_newbucket_from_oldbucket(_rel, _old_bucket, _lowmask, _maxbucket) }
unsafe fn hashoptions(_reloptions: Datum, _validate: bool) -> *mut bytea { crate::access::hash::hashutil::hashoptions(_reloptions, _validate) }
unsafe fn hashvalidate(_opclassoid: Oid) -> bool { crate::access::hash::hashvalidate::hashvalidate(_opclassoid) }
unsafe fn hashadjustmembers(
    _opfamilyoid: Oid,
    _opclassoid: Oid,
    _operators: *mut List,
    _functions: *mut List,
) { crate::access::hash::hashvalidate::hashadjustmembers(_opfamilyoid, _opclassoid, _operators, _functions) }
unsafe fn hashcostestimate(
    _root: *mut c_void,
    _path: *mut c_void,
    _loop_count: f64,
    _indexStartupCost: *mut Cost,
    _indexTotalCost: *mut Cost,
    _indexSelectivity: *mut Selectivity,
    _indexCorrelation: *mut f64,
    _indexPages: *mut f64,
) { crate::utils::adt::selfuncs::hashcostestimate(_root as _, _path as _, _loop_count, _indexStartupCost, _indexTotalCost, _indexSelectivity, _indexCorrelation, _indexPages) }
