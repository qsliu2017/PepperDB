//! nbtree.rs
//!   Implementation of Lehman and Yao's btree management algorithm for
//!   Postgres.
//!
//! NOTES
//!   This file contains only the public interface routines.
//!
//! Translated 1:1 from postgres/src/backend/access/nbtree/nbtree.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/access/nbtree/nbtree.c
//!
//! #include mapping:
//!   "postgres.h"                  -> crate::prelude::*
//!   "access/nbtree.h"             -> BTScanOpaque/BTPageOpaque/BTVacState etc. (stubs below)
//!   "access/relscan.h"            -> IndexScanDesc (crate::access::relscan)
//!   "access/stratnum.h"           -> StrategyNumber (crate::access::stratnum)
//!   "commands/progress.h"         -> PROGRESS_SCAN_BLOCKS_* (stubs)
//!   "commands/vacuum.h"           -> IndexBulkDeleteResult/IndexVacuumInfo (stubs)
//!   "nodes/execnodes.h"           -> IndexInfo (crate::nodes::execnodes)
//!   "pgstat.h"                    -> pgstat_progress_update_param (stub)
//!   "storage/bulk_write.h"        -> BulkWriteState (stubs)
//!   "storage/condition_variable.h"-> ConditionVariable (stubs)
//!   "storage/indexfsm.h"          -> RecordFreeIndexPage/IndexFreeSpaceMapVacuum (stubs)
//!   "storage/ipc.h"               -> PG_ENSURE_ERROR_CLEANUP/PG_END_ENSURE_ERROR_CLEANUP (stubs)
//!   "storage/lmgr.h"              -> LockRelationForExtension/UnlockRelationForExtension (stubs)
//!   "storage/read_stream.h"       -> ReadStream/read_stream_* (stubs)
//!   "utils/datum.h"               -> datumEstimateSpace/datumSerialize/datumRestore (stubs)
//!   "utils/fmgrprotos.h"          -> btcostestimate (stub)
//!   "utils/index_selfuncs.h"      -> btcostestimate (stub)
//!   "utils/memutils.h"            -> AllocSetContextCreate/MemoryContextDelete/MemoryContextReset

#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(dead_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(unused_assignments)]
#![allow(unused_labels)]
#![allow(unexpected_cfgs)]
#![allow(improper_ctypes)]

use crate::prelude::*;
use crate::AllocSetContextCreate;

use std::mem::{size_of, offset_of};
use std::ffi::{c_char, c_int, c_void};

// errmsg_internal() is a thin alias for errmsg() in this port (see sibling
// nbtree files); errcode/errdetail/errhint get folded into /* C also: */ comments.
macro_rules! errmsg_internal { ($fmt:literal $(, $arg:expr)*) => { errmsg!($fmt $(, $arg)*) }; }

use crate::c::{int16, int32, uint8, uint16, uint32, Size};

// ---------------------------------------------------------------------------
// Real, already-ported homes.
// ---------------------------------------------------------------------------
use crate::access::common::indextuple::{
    index_form_tuple, IndexTuple, IndexTupleData,
};
use crate::access::common::scankey::{ScanKey, ScanKeyData, SK_ISNULL, SK_SEARCHNULL};
use crate::access::stratnum::{
    InvalidStrategy, StrategyNumber,
    BTEqualStrategyNumber, BTLessStrategyNumber, BTLessEqualStrategyNumber,
    BTGreaterEqualStrategyNumber, BTGreaterStrategyNumber,
};
use crate::access::relscan::{IndexScanDesc, ParallelIndexScanDescData};
use crate::access::cmptype::{CompareType, COMPARE_LT, COMPARE_LE, COMPARE_EQ, COMPARE_GE, COMPARE_GT, COMPARE_INVALID};
use crate::access::index::amapi::{IndexAmRoutine, IndexUniqueCheck};
use crate::access::index::genam::{
    IndexBulkDeleteCallback, IndexBulkDeleteResult, IndexVacuumInfo,
    BufferAccessStrategy, RelationGetIndexScan,
};
use crate::nodes::execnodes::IndexInfo;
use crate::nodes::tidbitmap::{tbm_add_tuples, TIDBitmap};
use crate::storage::block::{BlockNumber, BlockNumberIsValid, InvalidBlockNumber};
use crate::storage::buf::{Buffer, InvalidBuffer};
use crate::storage::bufpage::{Page, PageGetItem, PageGetItemId, PageGetMaxOffsetNumber};
use crate::storage::itemptr::{ItemPointer, ItemPointerData};
use crate::storage::off::{OffsetNumber, OffsetNumberNext, FirstOffsetNumber, InvalidOffsetNumber};
use crate::utils::rel::{Relation, RelationGetRelationName};
use crate::utils::palloc::MemoryContext;
use crate::{makeNode, Assert, PG_RETURN_POINTER};
use crate::postgres::{DatumGetPointer, PointerGetDatum};
use crate::pg_config::BLCKSZ;

// Re-use nbtdedup types that are canonical for nbtree.
use crate::access::nbtree::nbtdedup::{
    BTPageOpaque, BTPageOpaqueData,
    BTVacuumPosting, BTVacuumPostingData,
    BTP_HAS_GARBAGE, BTMaxItemSize, MaxIndexTuplesPerPage, MaxTIDsPerBTreePage,
};
// Re-use nbtxlog types.
use crate::access::nbtree::nbtxlog::{BTREE_METAPAGE, BTP_SPLIT_END, P_NONE};
// Re-use nbtutils types.
use crate::access::nbtree::nbtutils::{
    BTArrayKeyInfo, BTCycleId,
    BTScanPosData, BTScanPosItem,
    SK_BT_SKIP, SK_BT_MINVAL, SK_BT_MAXVAL,
    ScanDirection, ForwardScanDirection,
    _bt_allequalimage, _bt_start_vacuum, _bt_end_vacuum, _bt_end_vacuum_callback,
    _bt_killitems, _bt_start_prim_scan, _bt_start_array_keys,
    btoptions, btproperty, btbuildphasename,
};
use crate::utils::fmgr::{FmgrInfo, FunctionCallInfo};
use crate::access::nbtree::nbtvalidate::{
    BTOPTIONS_PROC, BTMaxStrategyNumber,
    btvalidate, btadjustmembers,
};

/* BTNProcs from access/nbtree.h: #define BTNProcs 6 */
pub const BTNProcs: c_int = 6;

/*
 * P_RIGHTMOST() from access/nbtree.h.  Each nbtree translation unit defines its
 * own local copy (see sibling files), since the C macro is header-inlined.
 */
unsafe fn P_RIGHTMOST(opaque: BTPageOpaque) -> bool {
    (*opaque).btpo_next == P_NONE
}

// ---------------------------------------------------------------------------
// Extended BTScanOpaqueData -- the nbtutils stub is missing markItemIndex,
// currTuples, markTuples, arrayContext, orderProcs, skipScan.
// This local definition is the complete version; it shadows the nbtutils stub.
// TODO(pg-port): real definition lives in access/nbtree.h
// ---------------------------------------------------------------------------
#[repr(C)]
pub struct BTScanOpaqueData {
    pub currPos:          BTScanPosData,
    pub markPos:          BTScanPosData,
    pub markItemIndex:    c_int,
    pub dropPin:          bool,
    pub skipScan:         bool,
    pub scanBehind:       bool,
    pub oppositeDirCheck: bool,
    pub needPrimScan:     bool,
    pub qual_ok:          bool,
    pub numArrayKeys:     c_int,
    pub numKilled:        c_int,
    pub arrayKeys:        *mut BTArrayKeyInfo,
    pub orderProcs:       *mut FmgrInfo,
    pub keyData:          ScanKey,
    pub numberOfKeys:     c_int,
    pub arrayContext:     MemoryContext,
    pub currTuples:       *mut c_char,
    pub markTuples:       *mut c_char,
    pub killedItems:      *mut c_int,
}
pub type BTScanOpaque = *mut BTScanOpaqueData;

// ---------------------------------------------------------------------------
// Types local to this translation unit.
// ---------------------------------------------------------------------------

/*
 * BTPARALLEL_NOT_INITIALIZED indicates that the scan has not started.
 *
 * BTPARALLEL_NEED_PRIMSCAN indicates that some process must now seize the
 * scan to advance it via another call to _bt_first.
 *
 * BTPARALLEL_ADVANCING indicates that some process is advancing the scan to
 * a new page; others must wait.
 *
 * BTPARALLEL_IDLE indicates that no backend is currently advancing the scan
 * to a new page; some process can start doing that.
 *
 * BTPARALLEL_DONE indicates that the scan is complete (including error exit).
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum BTPS_State {
    BTPARALLEL_NOT_INITIALIZED,
    BTPARALLEL_NEED_PRIMSCAN,
    BTPARALLEL_ADVANCING,
    BTPARALLEL_IDLE,
    BTPARALLEL_DONE,
}
use BTPS_State::*;

/*
 * BTParallelScanDescData contains btree specific shared information required
 * for parallel scan.
 */
#[repr(C)]
pub struct BTParallelScanDescData {
    pub btps_nextScanPage:  BlockNumber,    /* next page to be scanned */
    pub btps_lastCurrPage:  BlockNumber,    /* page whose sibling link was copied into
                                             * btps_nextScanPage */
    pub btps_pageStatus:    BTPS_State,     /* indicates whether next page is
                                             * available for scan. see above for
                                             * possible states of parallel scan. */
    pub btps_lock:          LWLock,         /* protects shared parallel state */
    pub btps_cv:            ConditionVariable, /* used to synchronize parallel scan */

    /*
     * btps_arrElems is used when scans need to schedule another primitive
     * index scan with one or more SAOP arrays.  Holds BTArrayKeyInfo.cur_elem
     * offsets for each = scan key associated with a ScalarArrayOp array.
     */
    pub btps_arrElems:      [c_int; 0],     /* FLEXIBLE_ARRAY_MEMBER */

    /*
     * Additional space (at the end of the struct) is used when scans need to
     * schedule another primitive index scan with one or more skip arrays.
     * Holds a flattened datum representation for each = scan key associated
     * with a skip array.
     */
}
pub type BTParallelScanDesc = *mut BTParallelScanDescData;

// ---------------------------------------------------------------------------
// Stubs: symbols not yet ported from other translation units.
// ---------------------------------------------------------------------------

/// TODO(pg-port): LWLock from storage/lwlock.h.
pub type LWLock = u64;
/// TODO(pg-port): LW_EXCLUSIVE from storage/lwlock.h.
pub const LW_EXCLUSIVE: c_int = 2;
/// TODO(pg-port): LWTRANCHE_PARALLEL_BTREE_SCAN from storage/lwlock.h.
pub const LWTRANCHE_PARALLEL_BTREE_SCAN: c_int = 25;

/// TODO(pg-port): ConditionVariable from storage/condition_variable.h.
pub type ConditionVariable = u64;

/// TODO(pg-port): BulkWriteState from storage/bulk_write.h.
pub type BulkWriteState = c_void;
/// TODO(pg-port): BulkWriteBuffer from storage/bulk_write.h.
pub type BulkWriteBuffer = *mut c_void;

/// TODO(pg-port): ReadStream from storage/read_stream.h.
pub type ReadStream = c_void;

/// TODO(pg-port): BlockRangeReadStreamPrivate from storage/read_stream.h.
#[repr(C)]
pub struct BlockRangeReadStreamPrivate {
    pub current_blocknum: BlockNumber,
    pub last_exclusive:   BlockNumber,
}

/// TODO(pg-port): BTVacState from nbtree internal (nbtutils.c).
#[repr(C)]
pub struct BTVacState {
    pub info:            *mut IndexVacuumInfo,
    pub stats:           *mut IndexBulkDeleteResult,
    pub callback:        IndexBulkDeleteCallback,
    pub callback_state:  *mut c_void,
    pub cycleid:         BTCycleId,
    pub pagedelcontext:  MemoryContext,
    pub bufsize:         c_int,
    pub maxbufsize:      c_int,
    pub pendingpages:    *mut c_void,
    pub npendingpages:   c_int,
}

/// TODO(pg-port): PROGRESS_SCAN_BLOCKS_TOTAL from commands/progress.h.
pub const PROGRESS_SCAN_BLOCKS_TOTAL: c_int = 14;
/// TODO(pg-port): PROGRESS_SCAN_BLOCKS_DONE from commands/progress.h.
pub const PROGRESS_SCAN_BLOCKS_DONE: c_int = 15;

/// TODO(pg-port): READ_STREAM_MAINTENANCE from storage/read_stream.h.
pub const READ_STREAM_MAINTENANCE: c_int = 0x0001;
/// TODO(pg-port): READ_STREAM_FULL from storage/read_stream.h.
pub const READ_STREAM_FULL: c_int = 0x0002;
/// TODO(pg-port): READ_STREAM_USE_BATCHING from storage/read_stream.h.
pub const READ_STREAM_USE_BATCHING: c_int = 0x0004;
/// TODO(pg-port): MAIN_FORKNUM from common/relpath.h.
pub const MAIN_FORKNUM: c_int = 0;
/// TODO(pg-port): INIT_FORKNUM from common/relpath.h.
pub const INIT_FORKNUM: c_int = 1;
/// TODO(pg-port): RBM_NORMAL from storage/bufmgr.h.
pub const RBM_NORMAL: c_int = 0;
/// TODO(pg-port): ExclusiveLock from storage/lockdefs.h.
pub const ExclusiveLock: c_int = 8;
/// TODO(pg-port): BT_READ from nbtree.h.
pub const BT_READ: c_int = 1;

/// TODO(pg-port): WAIT_EVENT_BTREE_PAGE from pgstat.h / utils/wait_event.h.
pub const WAIT_EVENT_BTREE_PAGE: u32 = 0;

/// TODO(pg-port): VACUUM_OPTION_PARALLEL_BULKDEL from commands/vacuum.h.
pub const VACUUM_OPTION_PARALLEL_BULKDEL: c_int = 0x01;
/// TODO(pg-port): VACUUM_OPTION_PARALLEL_COND_CLEANUP from commands/vacuum.h.
pub const VACUUM_OPTION_PARALLEL_COND_CLEANUP: c_int = 0x02;

// TODO(pg-port): IndexBulkDeleteResult from access/genam.h (opaque c_void alias).
// Already imported as c_void alias via genam.

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;

    // nbtutils.c / nbtinsert.c / nbtsearch.c -- not yet ported to Rust
    fn btbuild(
        heap: Relation,
        index: Relation,
        indexInfo: *mut IndexInfo,
    ) -> *mut c_void /* IndexBuildResult */;
    fn btcostestimate(
        root: *mut c_void,
        path: *mut c_void,
        loop_count: f64,
        indexStartupCost: *mut f64,
        indexTotalCost: *mut f64,
        indexSelectivity: *mut f64,
        indexCorrelation: *mut f64,
        indexPages: *mut f64,
    );
    fn _bt_doinsert(
        rel: Relation,
        itup: IndexTuple,
        checkUnique: IndexUniqueCheck,
        indexUnchanged: bool,
        heapRel: Relation,
    ) -> bool;
    fn _bt_first(scan: IndexScanDesc, dir: ScanDirection) -> bool;
    fn _bt_next(scan: IndexScanDesc, dir: ScanDirection) -> bool;
    fn _bt_readfirstpage(scan: IndexScanDesc, offnum: OffsetNumber, dir: ScanDirection) -> bool;
    fn _bt_readnextpage(scan: IndexScanDesc, blkno: BlockNumber, dir: ScanDirection) -> bool;
    fn _bt_steppage(scan: IndexScanDesc, dir: ScanDirection) -> bool;
    fn _bt_initmetapage(page: Page, rootblkno: BlockNumber, rootlevel: u32, allequalimage: bool);
    fn _bt_checkpage(rel: Relation, buf: Buffer);
    fn _bt_lockbuf(rel: Relation, buf: Buffer, access: c_int);
    fn _bt_relbuf(rel: Relation, buf: Buffer);
    fn _bt_getrootheight(rel: Relation) -> c_int;
    fn _bt_pendingfsm_init(rel: Relation, vstate: *mut BTVacState, cleanuponly: bool);
    fn _bt_pendingfsm_finalize(rel: Relation, vstate: *mut BTVacState);
    fn _bt_pagedel(rel: Relation, buf: Buffer, vstate: *mut BTVacState);
    fn _bt_delitems_vacuum(
        rel: Relation,
        buf: Buffer,
        deletable: *mut OffsetNumber,
        ndeletable: c_int,
        updatable: *mut BTVacuumPosting,
        nupdatable: c_int,
    );
    fn _bt_upgradelockbufcleanup(rel: Relation, buf: Buffer);
    fn _bt_vacuum_needs_cleanup(rel: Relation) -> bool;
    fn _bt_set_cleanup_info(rel: Relation, num_delpages: BlockNumber);
    fn BTPageGetOpaque(page: Page) -> BTPageOpaque;
    fn BTPageIsRecyclable(page: Page, heaprel: Relation) -> bool;
    fn P_ISLEAF(opaque: BTPageOpaque) -> bool;
    fn P_ISDELETED(opaque: BTPageOpaque) -> bool;
    fn P_ISHALFDEAD(opaque: BTPageOpaque) -> bool;
    fn P_FIRSTDATAKEY(opaque: BTPageOpaque) -> OffsetNumber;
    fn BTScanPosIsValid(pos: BTScanPosData) -> bool;
    fn BTScanPosIsPinned(pos: BTScanPosData) -> bool;
    fn BTScanPosUnpinIfPinned(pos: *mut BTScanPosData);
    fn BTScanPosInvalidate(pos: *mut BTScanPosData);
    fn BTreeTupleIsPivot(itup: IndexTuple) -> bool;
    fn BTreeTupleIsPosting(itup: IndexTuple) -> bool;
    fn BTreeTupleGetNPosting(itup: IndexTuple) -> c_int;
    fn BTreeTupleGetPosting(itup: IndexTuple) -> *mut ItemPointerData;
    fn BufferGetPage(buf: Buffer) -> Page;
    fn BufferGetBlockNumber(buf: Buffer) -> BlockNumber;
    fn BufferIsValid(buf: Buffer) -> bool;
    fn IncrBufferRefCount(buf: Buffer);
    fn MarkBufferDirtyHint(buf: Buffer, buffer_std: bool);
    fn PageIsNew(page: Page) -> bool;
    fn LWLockInitialize(lock: *mut LWLock, tranche_id: c_int);
    fn LWLockAcquire(lock: *mut LWLock, mode: c_int) -> bool;
    fn LWLockRelease(lock: *mut LWLock);
    fn ConditionVariableInit(cv: *mut ConditionVariable);
    fn ConditionVariableSleep(cv: *mut ConditionVariable, wait_event: u32);
    fn ConditionVariableCancelSleep();
    fn ConditionVariableSignal(cv: *mut ConditionVariable);
    fn ConditionVariableBroadcast(cv: *mut ConditionVariable);
    fn datumEstimateSpace(
        value: Datum,
        isnull: bool,
        typbyval: bool,
        typlen: c_int,
    ) -> Size;
    fn datumSerialize(
        value: Datum,
        isnull: bool,
        typbyval: bool,
        typlen: c_int,
        start_address: *mut *mut c_char,
    );
    fn datumRestore(start_address: *mut *mut c_char, isnull: *mut bool) -> Datum;
    fn smgr_bulk_start_rel(rel: Relation, forknum: c_int) -> *mut BulkWriteState;
    fn smgr_bulk_get_buf(bulkstate: *mut BulkWriteState) -> BulkWriteBuffer;
    fn smgr_bulk_write(
        bulkstate: *mut BulkWriteState,
        blocknum: BlockNumber,
        buf: BulkWriteBuffer,
        page_std: bool,
    );
    fn smgr_bulk_finish(bulkstate: *mut BulkWriteState);
    fn RelationGetDescr(rel: Relation) -> *mut c_void; /* TupleDesc */
    fn RelationGetNumberOfBlocks(rel: Relation) -> BlockNumber;
    fn RelationNeedsWAL(rel: Relation) -> bool;
    fn RELATION_IS_LOCAL(rel: Relation) -> bool;
    fn IndexRelationGetNumberOfKeyAttributes(rel: Relation) -> int16;
    fn TupleDescCompactAttr(tupdesc: *mut c_void, attnum: c_int) -> *mut CompactAttribute;
    fn LockRelationForExtension(rel: Relation, lockmode: c_int);
    fn UnlockRelationForExtension(rel: Relation, lockmode: c_int);
    fn RecordFreeIndexPage(rel: Relation, blkno: BlockNumber);
    fn IndexFreeSpaceMapVacuum(rel: Relation);
    fn ReadBufferExtended(
        rel: Relation,
        forknum: c_int,
        blkno: BlockNumber,
        mode: c_int,
        strategy: *mut BufferAccessStrategy,
    ) -> Buffer;
    fn read_stream_begin_relation(
        flags: c_int,
        strategy: *mut BufferAccessStrategy,
        rel: Relation,
        forknum: c_int,
        callback: unsafe extern "C" fn(*mut ReadStream, *mut c_void, *mut c_void) -> BlockNumber,
        callback_state: *mut c_void,
        per_buffer_data_size: usize,
    ) -> *mut ReadStream;
    fn read_stream_next_buffer(stream: *mut ReadStream, per_buffer_data: *mut c_void) -> Buffer;
    fn read_stream_reset(stream: *mut ReadStream);
    fn read_stream_end(stream: *mut ReadStream);
    fn block_range_read_stream_cb(
        stream: *mut ReadStream,
        callback_private_data: *mut c_void,
        per_buffer_data: *mut c_void,
    ) -> BlockNumber;
    fn vacuum_delay_point(is_analyze: bool);
    fn pgstat_progress_update_param(index: c_int, val: i64);
    fn IsMVCCSnapshot(snapshot: *mut c_void) -> bool;
    fn pfree(ptr: *mut c_void);
    fn OffsetToPointer(base: *mut c_void, offset: usize) -> *mut c_void;
    fn add_size(s1: Size, s2: Size) -> Size;
    fn MemoryContextDelete(context: MemoryContext);
    fn MemoryContextReset(context: MemoryContext);
    fn MemoryContextSwitchTo(context: MemoryContext) -> MemoryContext;
    fn PG_ENSURE_ERROR_CLEANUP(
        cleanup_func: unsafe extern "C" fn(c_int, Datum),
        arg: Datum,
    );
    fn PG_END_ENSURE_ERROR_CLEANUP(
        cleanup_func: unsafe extern "C" fn(c_int, Datum),
        arg: Datum,
    );
}

/// TODO(pg-port): CompactAttribute from access/tupdesc.h.
#[repr(C)]
pub struct CompactAttribute {
    pub attbyval: bool,
    pub attlen:   c_int,
}

// ---------------------------------------------------------------------------
// bthandler -- Btree handler function: return IndexAmRoutine
// ---------------------------------------------------------------------------

/*
 * Btree handler function: return IndexAmRoutine with access method parameters
 * and callbacks.
 */
#[no_mangle]
pub unsafe extern "C" fn bthandler(fcinfo: FunctionCallInfo) -> Datum {
    let amroutine: *mut IndexAmRoutine = makeNode!(IndexAmRoutine, T_IndexAmRoutine);

    (*amroutine).amstrategies = BTMaxStrategyNumber as u16;
    (*amroutine).amsupport = BTNProcs as u16;
    (*amroutine).amoptsprocnum = BTOPTIONS_PROC as u16;
    (*amroutine).amcanorder = true;
    (*amroutine).amcanorderbyop = false;
    (*amroutine).amcanhash = false;
    (*amroutine).amconsistentequality = true;
    (*amroutine).amconsistentordering = true;
    (*amroutine).amcanbackward = true;
    (*amroutine).amcanunique = true;
    (*amroutine).amcanmulticol = true;
    (*amroutine).amoptionalkey = true;
    (*amroutine).amsearcharray = true;
    (*amroutine).amsearchnulls = true;
    (*amroutine).amstorage = false;
    (*amroutine).amclusterable = true;
    (*amroutine).ampredlocks = true;
    (*amroutine).amcanparallel = true;
    (*amroutine).amcanbuildparallel = true;
    (*amroutine).amcaninclude = true;
    (*amroutine).amusemaintenanceworkmem = false;
    (*amroutine).amsummarizing = false;
    (*amroutine).amparallelvacuumoptions =
        (VACUUM_OPTION_PARALLEL_BULKDEL | VACUUM_OPTION_PARALLEL_COND_CLEANUP) as u8;
    (*amroutine).amkeytype = 0; /* InvalidOid */

    /*
     * The IndexAmRoutine method-pointer fields are typed with amapi.rs's opaque
     * placeholder aliases (IndexScanDesc/IndexInfo/TIDBitmap/... are still
     * `*mut c_void`), while our callbacks use the real ported types.  Since a
     * pointer is a pointer, we transmute each fn item to the field's fn-pointer
     * type.  This mirrors the C assignment `amroutine->ambuild = btbuild;`.
     */
    (*amroutine).ambuild = Some(core::mem::transmute(btbuild as *const ()));
    (*amroutine).ambuildempty = Some(core::mem::transmute(btbuildempty as *const ()));
    (*amroutine).aminsert = Some(core::mem::transmute(btinsert as *const ()));
    (*amroutine).aminsertcleanup = None;
    (*amroutine).ambulkdelete = Some(core::mem::transmute(btbulkdelete as *const ()));
    (*amroutine).amvacuumcleanup = Some(core::mem::transmute(btvacuumcleanup as *const ()));
    (*amroutine).amcanreturn = Some(core::mem::transmute(btcanreturn as *const ()));
    (*amroutine).amcostestimate = Some(core::mem::transmute(btcostestimate as *const ()));
    (*amroutine).amgettreeheight = Some(core::mem::transmute(btgettreeheight as *const ()));
    (*amroutine).amoptions = Some(core::mem::transmute(btoptions as *const ()));
    (*amroutine).amproperty = Some(core::mem::transmute(btproperty as *const ()));
    (*amroutine).ambuildphasename = Some(core::mem::transmute(btbuildphasename as *const ()));
    (*amroutine).amvalidate = Some(core::mem::transmute(btvalidate as *const ()));
    (*amroutine).amadjustmembers = Some(core::mem::transmute(btadjustmembers as *const ()));
    (*amroutine).ambeginscan = Some(core::mem::transmute(btbeginscan as *const ()));
    (*amroutine).amrescan = Some(core::mem::transmute(btrescan as *const ()));
    (*amroutine).amgettuple = Some(core::mem::transmute(btgettuple as *const ()));
    (*amroutine).amgetbitmap = Some(core::mem::transmute(btgetbitmap as *const ()));
    (*amroutine).amendscan = Some(core::mem::transmute(btendscan as *const ()));
    (*amroutine).ammarkpos = Some(core::mem::transmute(btmarkpos as *const ()));
    (*amroutine).amrestrpos = Some(core::mem::transmute(btrestrpos as *const ()));
    (*amroutine).amestimateparallelscan = Some(core::mem::transmute(btestimateparallelscan as *const ()));
    (*amroutine).aminitparallelscan = Some(core::mem::transmute(btinitparallelscan as *const ()));
    (*amroutine).amparallelrescan = Some(core::mem::transmute(btparallelrescan as *const ()));
    (*amroutine).amtranslatestrategy = Some(core::mem::transmute(bttranslatestrategy as *const ()));
    (*amroutine).amtranslatecmptype = Some(core::mem::transmute(bttranslatecmptype as *const ()));

    PG_RETURN_POINTER!(amroutine)
}

/*
 *	btbuildempty() -- build an empty btree index in the initialization fork
 */
pub unsafe extern "C" fn btbuildempty(index: Relation) {
    let allequalimage: bool = _bt_allequalimage(index, false);
    let mut bulkstate: *mut BulkWriteState;
    let mut metabuf: BulkWriteBuffer;

    bulkstate = smgr_bulk_start_rel(index, INIT_FORKNUM);

    /* Construct metapage. */
    metabuf = smgr_bulk_get_buf(bulkstate);
    _bt_initmetapage(metabuf as Page, P_NONE, 0, allequalimage);
    smgr_bulk_write(bulkstate, BTREE_METAPAGE, metabuf, true);

    smgr_bulk_finish(bulkstate);
}

/*
 *	btinsert() -- insert an index tuple into a btree.
 *
 *		Descend the tree recursively, find the appropriate location for our
 *		new tuple, and put it there.
 */
pub unsafe extern "C" fn btinsert(
    rel: Relation,
    values: *mut Datum,
    isnull: *mut bool,
    ht_ctid: ItemPointer,
    heapRel: Relation,
    checkUnique: IndexUniqueCheck,
    indexUnchanged: bool,
    indexInfo: *mut IndexInfo,
) -> bool {
    let mut result: bool;
    let mut itup: IndexTuple;

    /* generate an index tuple */
    itup = index_form_tuple(RelationGetDescr(rel) as _, values, isnull);
    (*itup).t_tid = *ht_ctid;

    result = _bt_doinsert(rel, itup, checkUnique, indexUnchanged, heapRel);

    pfree(itup as *mut c_void);

    result
}

/*
 *	btgettuple() -- Get the next tuple in the scan.
 */
pub unsafe extern "C" fn btgettuple(scan: IndexScanDesc, dir: ScanDirection) -> bool {
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;
    let mut res: bool;

    Assert!((*scan).heapRelation != std::ptr::null_mut());

    /* btree indexes are never lossy */
    (*scan).xs_recheck = false;

    /* Each loop iteration performs another primitive index scan */
    loop {
        /*
         * If we've already initialized this scan, we can just advance it in
         * the appropriate direction.  If we haven't done so yet, we call
         * _bt_first() to get the first item in the scan.
         */
        if !BTScanPosIsValid(core::ptr::read(&(*so).currPos)) {
            res = _bt_first(scan, dir);
        } else {
            /*
             * Check to see if we should kill the previously-fetched tuple.
             */
            if (*scan).kill_prior_tuple {
                /*
                 * Yes, remember it for later. (We'll deal with all such
                 * tuples at once right before leaving the index page.)  The
                 * test for numKilled overrun is not just paranoia: if the
                 * caller reverses direction in the indexscan then the same
                 * item might get entered multiple times. It's not worth
                 * trying to optimize that, so we don't detect it, but instead
                 * just forget any excess entries.
                 */
                if (*so).killedItems.is_null() {
                    (*so).killedItems =
                        palloc(MaxTIDsPerBTreePage as usize * size_of::<c_int>())
                            as *mut c_int;
                }
                if (*so).numKilled < MaxTIDsPerBTreePage {
                    *(*so).killedItems.add((*so).numKilled as usize) = (*so).currPos.itemIndex;
                    (*so).numKilled += 1;
                }
            }

            /*
             * Now continue the scan.
             */
            res = _bt_next(scan, dir);
        }

        /* If we have a tuple, return it ... */
        if res {
            break;
        }
        /* ... otherwise see if we need another primitive index scan */
        if !((*so).numArrayKeys != 0 && _bt_start_prim_scan(scan as *mut _, dir)) {
            break;
        }
    }

    res
}

/*
 * btgetbitmap() -- gets all matching tuples, and adds them to a bitmap
 */
pub unsafe extern "C" fn btgetbitmap(scan: IndexScanDesc, tbm: *mut TIDBitmap) -> i64 {
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;
    let mut ntids: i64 = 0;
    let mut heapTid: ItemPointer;

    Assert!((*scan).heapRelation == std::ptr::null_mut());

    /* Each loop iteration performs another primitive index scan */
    loop {
        /* Fetch the first page & tuple */
        if _bt_first(scan, ForwardScanDirection) {
            /* Save tuple ID, and continue scanning */
            heapTid = &mut (*scan).xs_heaptid as *mut _ as ItemPointer;
            tbm_add_tuples(tbm, heapTid, 1, false);
            ntids += 1;

            loop {
                /*
                 * Advance to next tuple within page.  This is the same as the
                 * easy case in _bt_next().
                 */
                (*so).currPos.itemIndex += 1;
                if (*so).currPos.itemIndex > (*so).currPos.lastItem {
                    /* let _bt_next do the heavy lifting */
                    if !_bt_next(scan, ForwardScanDirection) {
                        break;
                    }
                }

                /* Save tuple ID, and continue scanning */
                heapTid = &mut (*(*so).currPos.items.add((*so).currPos.itemIndex as usize)).heapTid
                    as *mut _ as ItemPointer;
                tbm_add_tuples(tbm, heapTid, 1, false);
                ntids += 1;
            }
        }
        /* Now see if we need another primitive index scan */
        if !((*so).numArrayKeys != 0
            && _bt_start_prim_scan(scan as *mut _, ForwardScanDirection))
        {
            break;
        }
    }

    ntids
}

/*
 *	btbeginscan() -- start a scan on a btree index
 */
pub unsafe extern "C" fn btbeginscan(
    rel: Relation,
    nkeys: c_int,
    norderbys: c_int,
) -> IndexScanDesc {
    let mut scan: IndexScanDesc;
    let mut so: BTScanOpaque;

    /* no order by operators allowed */
    Assert!(norderbys == 0);

    /* get the scan */
    scan = RelationGetIndexScan(rel, nkeys, norderbys) as *mut _;

    /* allocate private workspace */
    so = palloc(size_of::<BTScanOpaqueData>()) as BTScanOpaque;
    BTScanPosInvalidate(&mut (*so).currPos as *mut BTScanPosData);
    BTScanPosInvalidate(&mut (*so).markPos as *mut BTScanPosData);
    if (*scan).numberOfKeys > 0 {
        (*so).keyData =
            palloc((*scan).numberOfKeys as usize * size_of::<ScanKeyData>()) as ScanKey;
    } else {
        (*so).keyData = std::ptr::null_mut();
    }

    (*so).skipScan = false;
    (*so).needPrimScan = false;
    (*so).scanBehind = false;
    (*so).oppositeDirCheck = false;
    (*so).arrayKeys = std::ptr::null_mut();
    (*so).orderProcs = std::ptr::null_mut();
    (*so).arrayContext = std::ptr::null_mut();

    (*so).killedItems = std::ptr::null_mut(); /* until needed */
    (*so).numKilled = 0;

    /*
     * We don't know yet whether the scan will be index-only, so we do not
     * allocate the tuple workspace arrays until btrescan.  However, we set up
     * scan->xs_itupdesc whether we'll need it or not, since that's so cheap.
     */
    (*so).currTuples = std::ptr::null_mut();
    (*so).markTuples = std::ptr::null_mut();

    (*scan).xs_itupdesc = RelationGetDescr(rel) as _;

    (*scan).opaque = so as *mut c_void;

    scan
}

/*
 *	btrescan() -- rescan an index relation
 */
pub unsafe extern "C" fn btrescan(
    scan: IndexScanDesc,
    scankey: ScanKey,
    nscankeys: c_int,
    orderbys: ScanKey,
    norderbys: c_int,
) {
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;

    /* we aren't holding any read locks, but gotta drop the pins */
    if BTScanPosIsValid(core::ptr::read(&(*so).currPos)) {
        /* Before leaving current page, deal with any killed items */
        if (*so).numKilled > 0 {
            _bt_killitems(scan as *mut _);
        }
        BTScanPosUnpinIfPinned(&mut (*so).currPos as *mut BTScanPosData);
        BTScanPosInvalidate(&mut (*so).currPos as *mut BTScanPosData);
    }

    /*
     * We prefer to eagerly drop leaf page pins before btgettuple returns.
     * This avoids making VACUUM wait to acquire a cleanup lock on the page.
     *
     * We cannot safely drop leaf page pins during index-only scans due to a
     * race condition involving VACUUM setting pages all-visible in the VM.
     * It's also unsafe for plain index scans that use a non-MVCC snapshot.
     *
     * When we drop pins eagerly, the mechanism that marks so->killedItems[]
     * index tuples LP_DEAD has to deal with concurrent TID recycling races.
     * The scheme used to detect unsafe TID recycling won't work when scanning
     * unlogged relations (since it involves saving an affected page's LSN).
     * Opt out of eager pin dropping during unlogged relation scans for now
     * (this is preferable to opting out of kill_prior_tuple LP_DEAD setting).
     *
     * Also opt out of dropping leaf page pins eagerly during bitmap scans.
     * Pins cannot be held for more than an instant during bitmap scans either
     * way, so we might as well avoid wasting cycles on acquiring page LSNs.
     *
     * See nbtree/README section on making concurrent TID recycling safe.
     *
     * Note: so->dropPin should never change across rescans.
     */
    (*so).dropPin = (!(*scan).xs_want_itup
        && IsMVCCSnapshot((*scan).xs_snapshot as *mut c_void)
        && RelationNeedsWAL((*scan).indexRelation)
        && (*scan).heapRelation != std::ptr::null_mut());

    (*so).markItemIndex = -1;
    (*so).needPrimScan = false;
    (*so).scanBehind = false;
    (*so).oppositeDirCheck = false;
    BTScanPosUnpinIfPinned(&mut (*so).markPos as *mut BTScanPosData);
    BTScanPosInvalidate(&mut (*so).markPos as *mut BTScanPosData);

    /*
     * Allocate tuple workspace arrays, if needed for an index-only scan and
     * not already done in a previous rescan call.  To save on palloc
     * overhead, both workspaces are allocated as one palloc block; only this
     * function and btendscan know that.
     *
     * NOTE: this data structure also makes it safe to return data from a
     * "name" column, even though btree name_ops uses an underlying storage
     * datatype of cstring.  The risk there is that "name" is supposed to be
     * padded to NAMEDATALEN, but the actual index tuple is probably shorter.
     * However, since we only return data out of tuples sitting in the
     * currTuples array, a fetch of NAMEDATALEN bytes can at worst pull some
     * data out of the markTuples array --- running off the end of memory for
     * a SIGSEGV is not possible.  Yeah, this is ugly as sin, but it beats
     * adding special-case treatment for name_ops elsewhere.
     */
    if (*scan).xs_want_itup && (*so).currTuples.is_null() {
        (*so).currTuples = palloc(BLCKSZ as usize * 2) as *mut c_char;
        (*so).markTuples = (*so).currTuples.add(BLCKSZ as usize);
    }

    /*
     * Reset the scan keys
     */
    if !scankey.is_null() && (*scan).numberOfKeys > 0 {
        memcpy(
            (*scan).keyData as *mut c_void,
            scankey as *const c_void,
            (*scan).numberOfKeys as usize * size_of::<ScanKeyData>(),
        );
    }
    (*so).numberOfKeys = 0; /* until _bt_preprocess_keys sets it */
    (*so).numArrayKeys = 0; /* ditto */
}

/*
 *	btendscan() -- close down a scan
 */
pub unsafe extern "C" fn btendscan(scan: IndexScanDesc) {
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;

    /* we aren't holding any read locks, but gotta drop the pins */
    if BTScanPosIsValid(core::ptr::read(&(*so).currPos)) {
        /* Before leaving current page, deal with any killed items */
        if (*so).numKilled > 0 {
            _bt_killitems(scan as *mut _);
        }
        BTScanPosUnpinIfPinned(&mut (*so).currPos as *mut BTScanPosData);
    }

    (*so).markItemIndex = -1;
    BTScanPosUnpinIfPinned(&mut (*so).markPos as *mut BTScanPosData);

    /* No need to invalidate positions, the RAM is about to be freed. */

    /* Release storage */
    if !(*so).keyData.is_null() {
        pfree((*so).keyData as *mut c_void);
    }
    /* so->arrayKeys and so->orderProcs are in arrayContext */
    if !(*so).arrayContext.is_null() {
        MemoryContextDelete((*so).arrayContext);
    }
    if !(*so).killedItems.is_null() {
        pfree((*so).killedItems as *mut c_void);
    }
    if !(*so).currTuples.is_null() {
        pfree((*so).currTuples as *mut c_void);
    }
    /* so->markTuples should not be pfree'd, see btrescan */
    pfree(so as *mut c_void);
}

/*
 *	btmarkpos() -- save current scan position
 */
pub unsafe extern "C" fn btmarkpos(scan: IndexScanDesc) {
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;

    /* There may be an old mark with a pin (but no lock). */
    BTScanPosUnpinIfPinned(&mut (*so).markPos as *mut BTScanPosData);

    /*
     * Just record the current itemIndex.  If we later step to next page
     * before releasing the marked position, _bt_steppage makes a full copy of
     * the currPos struct in markPos.  If (as often happens) the mark is moved
     * before we leave the page, we don't have to do that work.
     */
    if BTScanPosIsValid(core::ptr::read(&(*so).currPos)) {
        (*so).markItemIndex = (*so).currPos.itemIndex;
    } else {
        BTScanPosInvalidate(&mut (*so).markPos as *mut BTScanPosData);
        (*so).markItemIndex = -1;
    }
}

/*
 *	btrestrpos() -- restore scan to last saved position
 */
pub unsafe extern "C" fn btrestrpos(scan: IndexScanDesc) {
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;

    if (*so).markItemIndex >= 0 {
        /*
         * The scan has never moved to a new page since the last mark.  Just
         * restore the itemIndex.
         *
         * NB: In this case we can't count on anything in so->markPos to be
         * accurate.
         */
        (*so).currPos.itemIndex = (*so).markItemIndex;
    } else {
        /*
         * The scan moved to a new page after last mark or restore, and we are
         * now restoring to the marked page.  We aren't holding any read
         * locks, but if we're still holding the pin for the current position,
         * we must drop it.
         */
        if BTScanPosIsValid(core::ptr::read(&(*so).currPos)) {
            /* Before leaving current page, deal with any killed items */
            if (*so).numKilled > 0 {
                _bt_killitems(scan as *mut _);
            }
            BTScanPosUnpinIfPinned(&mut (*so).currPos as *mut BTScanPosData);
        }

        if BTScanPosIsValid(core::ptr::read(&(*so).markPos)) {
            /* bump pin on mark buffer for assignment to current buffer */
            if BTScanPosIsPinned(core::ptr::read(&(*so).markPos)) {
                IncrBufferRefCount((*so).markPos.buf);
            }
            memcpy(
                &mut (*so).currPos as *mut BTScanPosData as *mut c_void,
                &(*so).markPos as *const BTScanPosData as *const c_void,
                offset_of!(BTScanPosData, items) + size_of::<BTScanPosItem>(),
                /* C: offsetof(BTScanPosData, items[1]) +
                 *    so->markPos.lastItem * sizeof(BTScanPosItem) */
            );
            /* copy items up to lastItem */
            memcpy(
                (*so).currPos.items as *mut c_void,
                (*so).markPos.items as *const c_void,
                ((*so).markPos.lastItem as usize + 1) * size_of::<BTScanPosItem>(),
            );
            if !(*so).currTuples.is_null() {
                memcpy(
                    (*so).currTuples as *mut c_void,
                    (*so).markTuples as *const c_void,
                    (*so).markPos.nextTupleOffset as usize,
                );
            }
            /* Reset the scan's array keys (see _bt_steppage for why) */
            if (*so).numArrayKeys != 0 {
                _bt_start_array_keys(scan as *mut _, (*so).currPos.dir);
                (*so).needPrimScan = false;
            }
        } else {
            BTScanPosInvalidate(&mut (*so).currPos as *mut BTScanPosData);
        }
    }
}

/*
 * btestimateparallelscan -- estimate storage for BTParallelScanDescData
 */
pub unsafe extern "C" fn btestimateparallelscan(
    rel: Relation,
    nkeys: c_int,
    norderbys: c_int,
) -> Size {
    let nkeyatts: int16 = IndexRelationGetNumberOfKeyAttributes(rel);
    let mut estnbtreeshared: Size;
    let mut genericattrspace: Size;

    /*
     * Pessimistically assume that every input scan key will be output with
     * its own SAOP array
     */
    estnbtreeshared = offset_of!(BTParallelScanDescData, btps_arrElems)
        + size_of::<c_int>() * nkeys as usize;

    /* Single column indexes cannot possibly use a skip array */
    if nkeyatts == 1 {
        return estnbtreeshared;
    }

    /*
     * Pessimistically assume that all attributes prior to the least
     * significant attribute require a skip array (and an associated key)
     */
    genericattrspace = datumEstimateSpace(0 as Datum, false, true, size_of::<Datum>() as c_int);
    for attnum in 1..nkeyatts as c_int {
        let mut attr: *mut CompactAttribute;

        /*
         * We make the conservative assumption that every index column will
         * also require a skip array.
         *
         * Every skip array must have space to store its scan key's sk_flags.
         */
        estnbtreeshared = add_size(estnbtreeshared, size_of::<c_int>());

        /* Consider space required to store a datum of opclass input type */
        attr = TupleDescCompactAttr(RelationGetDescr(rel), attnum - 1);
        if (*attr).attbyval {
            /* This index attribute stores pass-by-value datums */
            let estfixed: Size = datumEstimateSpace(
                0 as Datum,
                false,
                true,
                (*attr).attlen,
            );

            estnbtreeshared = add_size(estnbtreeshared, estfixed);
            continue;
        }

        /*
         * This index attribute stores pass-by-reference datums.
         *
         * Assume that serializing this array will use just as much space as a
         * pass-by-value datum, in addition to space for the largest possible
         * whole index tuple (this is not just a per-datum portion of the
         * largest possible tuple because that'd be almost as large anyway).
         *
         * This is quite conservative, but it's not clear how we could do much
         * better.  The executor requires an up-front storage request size
         * that reliably covers the scan's high watermark memory usage.  We
         * can't be sure of the real high watermark until the scan is over.
         */
        estnbtreeshared = add_size(estnbtreeshared, genericattrspace);
        estnbtreeshared = add_size(estnbtreeshared, BTMaxItemSize);
    }

    estnbtreeshared
}

/*
 * _bt_parallel_serialize_arrays() -- Serialize parallel array state.
 *
 * Caller must have exclusively locked btscan->btps_lock when called.
 */
unsafe fn _bt_parallel_serialize_arrays(
    rel: Relation,
    btscan: BTParallelScanDesc,
    so: BTScanOpaque,
) {
    let mut datumshared: *mut c_char;

    /* Space for serialized datums begins immediately after btps_arrElems[] */
    datumshared = (*btscan).btps_arrElems.as_ptr().add((*so).numArrayKeys as usize)
        as *mut c_char;
    for i in 0..(*so).numArrayKeys as usize {
        let array: *mut BTArrayKeyInfo = (*so).arrayKeys.add(i);
        let skey: ScanKey = (*so).keyData.add((*array).scan_key as usize);

        if (*array).num_elems != -1 {
            /* Save SAOP array's cur_elem (no need to copy key/datum) */
            Assert!((*skey).sk_flags & SK_BT_SKIP as i32 == 0);
            /* C: btscan->btps_arrElems[i] = array->cur_elem; */
            let p: *mut c_int = (*btscan)
                .btps_arrElems
                .as_ptr()
                .add(i) as *mut c_int;
            *p = (*array).cur_elem;
            continue;
        }

        /* Save all mutable state associated with skip array's key */
        Assert!((*skey).sk_flags & SK_BT_SKIP as i32 != 0);
        memcpy(
            datumshared as *mut c_void,
            &(*skey).sk_flags as *const c_int as *const c_void,
            size_of::<c_int>(),
        );
        datumshared = datumshared.add(size_of::<c_int>());

        if (*skey).sk_flags & (SK_BT_MINVAL | SK_BT_MAXVAL) as i32 != 0 {
            /* No sk_argument datum to serialize */
            Assert!((*skey).sk_argument == 0);
            continue;
        }

        datumSerialize(
            (*skey).sk_argument,
            ((*skey).sk_flags & SK_ISNULL as i32) != 0,
            (*array).attbyval,
            (*array).attlen,
            &mut datumshared,
        );
    }
}

/*
 * _bt_parallel_restore_arrays() -- Restore serialized parallel array state.
 *
 * Caller must have exclusively locked btscan->btps_lock when called.
 */
unsafe fn _bt_parallel_restore_arrays(
    rel: Relation,
    btscan: BTParallelScanDesc,
    so: BTScanOpaque,
) {
    let mut datumshared: *mut c_char;

    /* Space for serialized datums begins immediately after btps_arrElems[] */
    datumshared = (*btscan).btps_arrElems.as_ptr().add((*so).numArrayKeys as usize)
        as *mut c_char;
    for i in 0..(*so).numArrayKeys as usize {
        let array: *mut BTArrayKeyInfo = (*so).arrayKeys.add(i);
        let skey: ScanKey = (*so).keyData.add((*array).scan_key as usize);
        let mut isnull: bool = false;

        if (*array).num_elems != -1 {
            /* Restore SAOP array using its saved cur_elem */
            Assert!((*skey).sk_flags & SK_BT_SKIP as i32 == 0);
            let p: *const c_int = (*btscan)
                .btps_arrElems
                .as_ptr()
                .add(i) as *const c_int;
            (*array).cur_elem = *p;
            (*skey).sk_argument = *(*array).elem_values.add((*array).cur_elem as usize);
            continue;
        }

        /* Restore skip array by restoring its key directly */
        if !(*array).attbyval && (*skey).sk_argument != 0 {
            pfree(DatumGetPointer((*skey).sk_argument) as *mut c_void);
        }
        (*skey).sk_argument = 0 as Datum;
        memcpy(
            &mut (*skey).sk_flags as *mut c_int as *mut c_void,
            datumshared as *const c_void,
            size_of::<c_int>(),
        );
        datumshared = datumshared.add(size_of::<c_int>());

        Assert!((*skey).sk_flags & SK_BT_SKIP as i32 != 0);

        if (*skey).sk_flags & (SK_BT_MINVAL | SK_BT_MAXVAL) as i32 != 0 {
            /* No sk_argument datum to restore */
            continue;
        }

        (*skey).sk_argument = datumRestore(&mut datumshared, &mut isnull as *mut bool);
        if isnull {
            Assert!((*skey).sk_argument == 0);
            Assert!((*skey).sk_flags & SK_SEARCHNULL as i32 != 0);
            Assert!((*skey).sk_flags & SK_ISNULL as i32 != 0);
        }
    }
}

/*
 * btinitparallelscan -- initialize BTParallelScanDesc for parallel btree scan
 */
pub unsafe extern "C" fn btinitparallelscan(target: *mut c_void) {
    let bt_target: BTParallelScanDesc = target as BTParallelScanDesc;

    LWLockInitialize(&mut (*bt_target).btps_lock as *mut LWLock,
                     LWTRANCHE_PARALLEL_BTREE_SCAN);
    (*bt_target).btps_nextScanPage = InvalidBlockNumber;
    (*bt_target).btps_lastCurrPage = InvalidBlockNumber;
    (*bt_target).btps_pageStatus = BTPARALLEL_NOT_INITIALIZED;
    ConditionVariableInit(&mut (*bt_target).btps_cv as *mut ConditionVariable);
}

/*
 *	btparallelrescan() -- reset parallel scan
 */
pub unsafe extern "C" fn btparallelrescan(scan: IndexScanDesc) {
    let mut btscan: BTParallelScanDesc;
    let parallel_scan: *mut ParallelIndexScanDescData = (*scan).parallel_scan;

    Assert!(!parallel_scan.is_null());

    btscan = OffsetToPointer(
        parallel_scan as *mut c_void,
        (*parallel_scan).ps_offset_am,
    ) as BTParallelScanDesc;

    /*
     * In theory, we don't need to acquire the LWLock here, because there
     * shouldn't be any other workers running at this point, but we do so for
     * consistency.
     */
    LWLockAcquire(&mut (*btscan).btps_lock as *mut LWLock, LW_EXCLUSIVE);
    (*btscan).btps_nextScanPage = InvalidBlockNumber;
    (*btscan).btps_lastCurrPage = InvalidBlockNumber;
    (*btscan).btps_pageStatus = BTPARALLEL_NOT_INITIALIZED;
    LWLockRelease(&mut (*btscan).btps_lock as *mut LWLock);
}

/*
 * _bt_parallel_seize() -- Begin the process of advancing the scan to a new
 *		page.  Other scans must wait until we call _bt_parallel_release()
 *		or _bt_parallel_done().
 *
 * The return value is true if we successfully seized the scan and false
 * if we did not.  The latter case occurs when no pages remain, or when
 * another primitive index scan is scheduled that caller's backend cannot
 * start just yet (only backends that call from _bt_first are capable of
 * starting primitive index scans, which they indicate by passing first=true).
 *
 * If the return value is true, *next_scan_page returns the next page of the
 * scan, and *last_curr_page returns the page that *next_scan_page came from.
 * An invalid *next_scan_page means the scan hasn't yet started, or that
 * caller needs to start the next primitive index scan (if it's the latter
 * case we'll set so.needPrimScan).
 *
 * Callers should ignore the value of *next_scan_page and *last_curr_page if
 * the return value is false.
 */
pub unsafe fn _bt_parallel_seize(
    scan: IndexScanDesc,
    next_scan_page: *mut BlockNumber,
    last_curr_page: *mut BlockNumber,
    first: bool,
) -> bool {
    let rel: Relation = (*scan).indexRelation;
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;
    let mut exit_loop: bool = false;
    let mut status: bool = true;
    let mut endscan: bool = false;
    let parallel_scan: *mut ParallelIndexScanDescData = (*scan).parallel_scan;
    let mut btscan: BTParallelScanDesc;

    *next_scan_page = InvalidBlockNumber;
    *last_curr_page = InvalidBlockNumber;

    /*
     * Reset so->currPos, and initialize moreLeft/moreRight such that the next
     * call to _bt_readnextpage treats this backend similarly to a serial
     * backend that steps from *last_curr_page to *next_scan_page (unless this
     * backend's so->currPos is initialized by _bt_readfirstpage before then).
     */
    BTScanPosInvalidate(&mut (*so).currPos as *mut BTScanPosData);
    (*so).currPos.moreLeft = true;
    (*so).currPos.moreRight = true;

    if first {
        /*
         * Initialize array related state when called from _bt_first, assuming
         * that this will be the first primitive index scan for the scan
         */
        (*so).needPrimScan = false;
        (*so).scanBehind = false;
        (*so).oppositeDirCheck = false;
    } else {
        /*
         * Don't attempt to seize the scan when it requires another primitive
         * index scan, since caller's backend cannot start it right now
         */
        if (*so).needPrimScan {
            return false;
        }
    }

    btscan = OffsetToPointer(
        parallel_scan as *mut c_void,
        (*parallel_scan).ps_offset_am,
    ) as BTParallelScanDesc;

    loop {
        LWLockAcquire(&mut (*btscan).btps_lock as *mut LWLock, LW_EXCLUSIVE);

        if (*btscan).btps_pageStatus == BTPARALLEL_DONE {
            /* We're done with this parallel index scan */
            status = false;
        } else if (*btscan).btps_pageStatus == BTPARALLEL_IDLE
            && (*btscan).btps_nextScanPage == P_NONE
        {
            /* End this parallel index scan */
            status = false;
            endscan = true;
        } else if (*btscan).btps_pageStatus == BTPARALLEL_NEED_PRIMSCAN {
            Assert!((*so).numArrayKeys != 0);

            if first {
                /* Can start scheduled primitive scan right away, so do so */
                (*btscan).btps_pageStatus = BTPARALLEL_ADVANCING;

                /* Restore scan's array keys from serialized values */
                _bt_parallel_restore_arrays(rel, btscan, so);
                exit_loop = true;
            } else {
                /*
                 * Don't attempt to seize the scan when it requires another
                 * primitive index scan, since caller's backend cannot start
                 * it right now
                 */
                status = false;
            }

            /*
             * Either way, update backend local state to indicate that a
             * pending primitive scan is required
             */
            (*so).needPrimScan = true;
            (*so).scanBehind = false;
            (*so).oppositeDirCheck = false;
        } else if (*btscan).btps_pageStatus != BTPARALLEL_ADVANCING {
            /*
             * We have successfully seized control of the scan for the purpose
             * of advancing it to a new page!
             */
            (*btscan).btps_pageStatus = BTPARALLEL_ADVANCING;
            Assert!((*btscan).btps_nextScanPage != P_NONE);
            *next_scan_page = (*btscan).btps_nextScanPage;
            *last_curr_page = (*btscan).btps_lastCurrPage;
            exit_loop = true;
        }
        LWLockRelease(&mut (*btscan).btps_lock as *mut LWLock);
        if exit_loop || !status {
            break;
        }
        ConditionVariableSleep(&mut (*btscan).btps_cv as *mut ConditionVariable,
                               WAIT_EVENT_BTREE_PAGE);
    }
    ConditionVariableCancelSleep();

    /* When the scan has reached the rightmost (or leftmost) page, end it */
    if endscan {
        _bt_parallel_done(scan);
    }

    status
}

/*
 * _bt_parallel_release() -- Complete the process of advancing the scan to a
 *		new page.  We now have the new value btps_nextScanPage; another backend
 *		can now begin advancing the scan.
 *
 * Callers whose scan uses array keys must save their curr_page argument so
 * that it can be passed to _bt_parallel_primscan_schedule, should caller
 * determine that another primitive index scan is required.
 *
 * If caller's next_scan_page is P_NONE, the scan has reached the index's
 * rightmost/leftmost page.  This is treated as reaching the end of the scan
 * within _bt_parallel_seize.
 *
 * Note: unlike the serial case, parallel scans don't need to remember both
 * sibling links.  next_scan_page is whichever link is next given the scan's
 * direction.  That's all we'll ever need, since the direction of a parallel
 * scan can never change.
 */
pub unsafe fn _bt_parallel_release(
    scan: IndexScanDesc,
    next_scan_page: BlockNumber,
    curr_page: BlockNumber,
) {
    let parallel_scan: *mut ParallelIndexScanDescData = (*scan).parallel_scan;
    let mut btscan: BTParallelScanDesc;

    Assert!(BlockNumberIsValid(next_scan_page));

    btscan = OffsetToPointer(
        parallel_scan as *mut c_void,
        (*parallel_scan).ps_offset_am,
    ) as BTParallelScanDesc;

    LWLockAcquire(&mut (*btscan).btps_lock as *mut LWLock, LW_EXCLUSIVE);
    (*btscan).btps_nextScanPage = next_scan_page;
    (*btscan).btps_lastCurrPage = curr_page;
    (*btscan).btps_pageStatus = BTPARALLEL_IDLE;
    LWLockRelease(&mut (*btscan).btps_lock as *mut LWLock);
    ConditionVariableSignal(&mut (*btscan).btps_cv as *mut ConditionVariable);
}

/*
 * _bt_parallel_done() -- Mark the parallel scan as complete.
 *
 * When there are no pages left to scan, this function should be called to
 * notify other workers.  Otherwise, they might wait forever for the scan to
 * advance to the next page.
 */
pub unsafe fn _bt_parallel_done(scan: IndexScanDesc) {
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;
    let parallel_scan: *mut ParallelIndexScanDescData = (*scan).parallel_scan;
    let mut btscan: BTParallelScanDesc;
    let mut status_changed: bool = false;

    Assert!(!BTScanPosIsValid(core::ptr::read(&(*so).currPos)));

    /* Do nothing, for non-parallel scans */
    if parallel_scan.is_null() {
        return;
    }

    /*
     * Should not mark parallel scan done when there's still a pending
     * primitive index scan
     */
    if (*so).needPrimScan {
        return;
    }

    btscan = OffsetToPointer(
        parallel_scan as *mut c_void,
        (*parallel_scan).ps_offset_am,
    ) as BTParallelScanDesc;

    /*
     * Mark the parallel scan as done, unless some other process did so
     * already
     */
    LWLockAcquire(&mut (*btscan).btps_lock as *mut LWLock, LW_EXCLUSIVE);
    Assert!((*btscan).btps_pageStatus != BTPARALLEL_NEED_PRIMSCAN);
    if (*btscan).btps_pageStatus != BTPARALLEL_DONE {
        (*btscan).btps_pageStatus = BTPARALLEL_DONE;
        status_changed = true;
    }
    LWLockRelease(&mut (*btscan).btps_lock as *mut LWLock);

    /* wake up all the workers associated with this parallel scan */
    if status_changed {
        ConditionVariableBroadcast(&mut (*btscan).btps_cv as *mut ConditionVariable);
    }
}

/*
 * _bt_parallel_primscan_schedule() -- Schedule another primitive index scan.
 *
 * Caller passes the curr_page most recently passed to _bt_parallel_release
 * by its backend.  Caller successfully schedules the next primitive index scan
 * if the shared parallel state hasn't been seized since caller's backend last
 * advanced the scan.
 */
pub unsafe fn _bt_parallel_primscan_schedule(
    scan: IndexScanDesc,
    curr_page: BlockNumber,
) {
    let rel: Relation = (*scan).indexRelation;
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;
    let parallel_scan: *mut ParallelIndexScanDescData = (*scan).parallel_scan;
    let mut btscan: BTParallelScanDesc;

    Assert!((*so).numArrayKeys != 0);

    btscan = OffsetToPointer(
        parallel_scan as *mut c_void,
        (*parallel_scan).ps_offset_am,
    ) as BTParallelScanDesc;

    LWLockAcquire(&mut (*btscan).btps_lock as *mut LWLock, LW_EXCLUSIVE);
    if (*btscan).btps_lastCurrPage == curr_page
        && (*btscan).btps_pageStatus == BTPARALLEL_IDLE
    {
        (*btscan).btps_nextScanPage = InvalidBlockNumber;
        (*btscan).btps_lastCurrPage = InvalidBlockNumber;
        (*btscan).btps_pageStatus = BTPARALLEL_NEED_PRIMSCAN;

        /* Serialize scan's current array keys */
        _bt_parallel_serialize_arrays(rel, btscan, so);
    }
    LWLockRelease(&mut (*btscan).btps_lock as *mut LWLock);
}

/*
 * Bulk deletion of all index entries pointing to a set of heap tuples.
 * The set of target tuples is specified via a callback routine that tells
 * whether any given heap tuple (identified by ItemPointer) is being deleted.
 *
 * Result: a palloc'd struct containing statistical info for VACUUM displays.
 */
pub unsafe extern "C" fn btbulkdelete(
    info: *mut IndexVacuumInfo,
    mut stats: *mut IndexBulkDeleteResult,
    callback: IndexBulkDeleteCallback,
    callback_state: *mut c_void,
) -> *mut IndexBulkDeleteResult {
    let rel: Relation = (*info).index;
    let mut cycleid: BTCycleId;

    /* allocate stats if first time through, else re-use existing struct */
    if stats.is_null() {
        stats = palloc0(size_of::<IndexBulkDeleteResult>())
            as *mut IndexBulkDeleteResult;
    }

    /* Establish the vacuum cycle ID to use for this scan */
    /* The ENSURE stuff ensures we clean up shared memory on failure */
    PG_ENSURE_ERROR_CLEANUP(
        _bt_end_vacuum_callback,
        PointerGetDatum(rel as *mut c_void),
    );
    cycleid = _bt_start_vacuum(rel);
    btvacuumscan(info, stats, callback, callback_state, cycleid);
    PG_END_ENSURE_ERROR_CLEANUP(
        _bt_end_vacuum_callback,
        PointerGetDatum(rel as *mut c_void),
    );
    _bt_end_vacuum(rel);

    stats
}

/*
 * Post-VACUUM cleanup.
 *
 * Result: a palloc'd struct containing statistical info for VACUUM displays.
 */
pub unsafe extern "C" fn btvacuumcleanup(
    info: *mut IndexVacuumInfo,
    mut stats: *mut IndexBulkDeleteResult,
) -> *mut IndexBulkDeleteResult {
    let mut num_delpages: BlockNumber;

    /* No-op in ANALYZE ONLY mode */
    if (*info).analyze_only {
        return stats;
    }

    /*
     * If btbulkdelete was called, we need not do anything (we just maintain
     * the information used within _bt_vacuum_needs_cleanup() by calling
     * _bt_set_cleanup_info() below).
     *
     * If btbulkdelete was _not_ called, then we have a choice to make: we
     * must decide whether or not a btvacuumscan() call is needed now (i.e.
     * whether the ongoing VACUUM operation can entirely avoid a physical scan
     * of the index).  A call to _bt_vacuum_needs_cleanup() decides it for us
     * now.
     */
    if stats.is_null() {
        /* Check if VACUUM operation can entirely avoid btvacuumscan() call */
        if !_bt_vacuum_needs_cleanup((*info).index) {
            return std::ptr::null_mut();
        }

        /*
         * Since we aren't going to actually delete any leaf items, there's no
         * need to go through all the vacuum-cycle-ID pushups here.
         *
         * Posting list tuples are a source of inaccuracy for cleanup-only
         * scans.  btvacuumscan() will assume that the number of index tuples
         * from each page can be used as num_index_tuples, even though
         * num_index_tuples is supposed to represent the number of TIDs in the
         * index.  This naive approach can underestimate the number of tuples
         * in the index significantly.
         *
         * We handle the problem by making num_index_tuples an estimate in
         * cleanup-only case.
         */
        stats = palloc0(size_of::<IndexBulkDeleteResult>())
            as *mut IndexBulkDeleteResult;
        btvacuumscan(info, stats, None, std::ptr::null_mut(), 0);
        (*stats).estimated_count = true;
    }

    /*
     * Maintain num_delpages value in metapage for _bt_vacuum_needs_cleanup().
     *
     * num_delpages is the number of deleted pages now in the index that were
     * not safe to place in the FSM to be recycled just yet.  num_delpages is
     * greater than 0 only when _bt_pagedel() actually deleted pages during
     * our call to btvacuumscan().  Even then, _bt_pendingfsm_finalize() must
     * have failed to place any newly deleted pages in the FSM just moments
     * ago.  (Actually, there are edge cases where recycling of the current
     * VACUUM's newly deleted pages does not even become safe by the time the
     * next VACUUM comes around.  See nbtree/README.)
     */
    Assert!((*stats).pages_deleted >= (*stats).pages_free);
    num_delpages = (*stats).pages_deleted - (*stats).pages_free;
    _bt_set_cleanup_info((*info).index, num_delpages);

    /*
     * It's quite possible for us to be fooled by concurrent page splits into
     * double-counting some index tuples, so disbelieve any total that exceeds
     * the underlying heap's count ... if we know that accurately.  Otherwise
     * this might just make matters worse.
     */
    if !(*info).estimated_count {
        if (*stats).num_index_tuples > (*info).num_heap_tuples {
            (*stats).num_index_tuples = (*info).num_heap_tuples;
        }
    }

    stats
}

/*
 * btvacuumscan --- scan the index for VACUUMing purposes
 *
 * This combines the functions of looking for leaf tuples that are deletable
 * according to the vacuum callback, looking for empty pages that can be
 * deleted, and looking for old deleted pages that can be recycled.  Both
 * btbulkdelete and btvacuumcleanup invoke this (the latter only if no
 * btbulkdelete call occurred and _bt_vacuum_needs_cleanup returned true).
 *
 * The caller is responsible for initially allocating/zeroing a stats struct
 * and for obtaining a vacuum cycle ID if necessary.
 */
unsafe fn btvacuumscan(
    info: *mut IndexVacuumInfo,
    stats: *mut IndexBulkDeleteResult,
    callback: IndexBulkDeleteCallback,
    callback_state: *mut c_void,
    cycleid: BTCycleId,
) {
    let rel: Relation = (*info).index;
    let mut vstate: BTVacState = core::mem::zeroed();
    let mut num_pages: BlockNumber;
    let mut needLock: bool;
    let mut p: BlockRangeReadStreamPrivate = core::mem::zeroed();
    let mut stream: *mut ReadStream = std::ptr::null_mut();

    /*
     * Reset fields that track information about the entire index now.  This
     * avoids double-counting in the case where a single VACUUM command
     * requires multiple scans of the index.
     *
     * Avoid resetting the tuples_removed and pages_newly_deleted fields here,
     * since they track information about the VACUUM command, and so must last
     * across each call to btvacuumscan().
     *
     * (Note that pages_free is treated as state about the whole index, not
     * the current VACUUM.  This is appropriate because RecordFreeIndexPage()
     * calls are idempotent, and get repeated for the same deleted pages in
     * some scenarios.  The point for us is to track the number of recyclable
     * pages in the index at the end of the VACUUM command.)
     */
    (*stats).num_pages = 0;
    (*stats).num_index_tuples = 0.0;
    (*stats).pages_deleted = 0;
    (*stats).pages_free = 0;

    /* Set up info to pass down to btvacuumpage */
    vstate.info = info;
    vstate.stats = stats;
    vstate.callback = callback;
    vstate.callback_state = callback_state;
    vstate.cycleid = cycleid;

    /* Create a temporary memory context to run _bt_pagedel in */
    vstate.pagedelcontext = AllocSetContextCreate!(
        CurrentMemoryContext,
        "_bt_pagedel",
        ALLOCSET_DEFAULT_SIZES
    );

    /* Initialize vstate fields used by _bt_pendingfsm_finalize */
    vstate.bufsize = 0;
    vstate.maxbufsize = 0;
    vstate.pendingpages = std::ptr::null_mut();
    vstate.npendingpages = 0;
    /* Consider applying _bt_pendingfsm_finalize optimization */
    _bt_pendingfsm_init(rel, &mut vstate as *mut BTVacState, callback.is_none());

    /*
     * The outer loop iterates over all index pages except the metapage, in
     * physical order (we hope the kernel will cooperate in providing
     * read-ahead for speed).  It is critical that we visit all leaf pages,
     * including ones added after we start the scan, else we might fail to
     * delete some deletable tuples.  Hence, we must repeatedly check the
     * relation length.  We must acquire the relation-extension lock while
     * doing so to avoid a race condition: if someone else is extending the
     * relation, there is a window where bufmgr/smgr have created a new
     * all-zero page but it hasn't yet been write-locked by _bt_getbuf(). If
     * we manage to scan such a page here, we'll improperly assume it can be
     * recycled.  Taking the lock synchronizes things enough to prevent a
     * problem: either num_pages won't include the new page, or _bt_getbuf
     * already has write lock on the buffer and it will be fully initialized
     * before we can examine it.  Also, we need not worry if a page is added
     * immediately after we look; the page splitting code already has
     * write-lock on the left page before it adds a right page, so we must
     * already have processed any tuples due to be moved into such a page.
     *
     * XXX: Now that new pages are locked with RBM_ZERO_AND_LOCK, I don't
     * think the use of the extension lock is still required.
     *
     * We can skip locking for new or temp relations, however, since no one
     * else could be accessing them.
     */
    needLock = !RELATION_IS_LOCAL(rel);

    p.current_blocknum = BTREE_METAPAGE + 1;

    /*
     * It is safe to use batchmode as block_range_read_stream_cb takes no
     * locks.
     */
    stream = read_stream_begin_relation(
        READ_STREAM_MAINTENANCE | READ_STREAM_FULL | READ_STREAM_USE_BATCHING,
        (*info).strategy as *mut BufferAccessStrategy,
        rel,
        MAIN_FORKNUM,
        block_range_read_stream_cb,
        &mut p as *mut BlockRangeReadStreamPrivate as *mut c_void,
        0,
    );
    loop {
        /* Get the current relation length */
        if needLock {
            LockRelationForExtension(rel, ExclusiveLock);
        }
        num_pages = RelationGetNumberOfBlocks(rel);
        if needLock {
            UnlockRelationForExtension(rel, ExclusiveLock);
        }

        if (*info).report_progress {
            pgstat_progress_update_param(PROGRESS_SCAN_BLOCKS_TOTAL, num_pages as i64);
        }

        /* Quit if we've scanned the whole relation */
        if p.current_blocknum >= num_pages {
            break;
        }

        p.last_exclusive = num_pages;

        /* Iterate over pages, then loop back to recheck relation length */
        loop {
            let mut current_block: BlockNumber;
            let mut buf: Buffer;

            /* call vacuum_delay_point while not holding any buffer lock */
            vacuum_delay_point(false);

            buf = read_stream_next_buffer(stream, std::ptr::null_mut());

            if !BufferIsValid(buf) {
                break;
            }

            current_block = btvacuumpage(&mut vstate as *mut BTVacState, buf);

            if (*info).report_progress {
                pgstat_progress_update_param(PROGRESS_SCAN_BLOCKS_DONE,
                                             current_block as i64);
            }
        }

        /*
         * We have to reset the read stream to use it again. After returning
         * InvalidBuffer, the read stream API won't invoke our callback again
         * until the stream has been reset.
         */
        read_stream_reset(stream);
    }

    read_stream_end(stream);

    /* Set statistics num_pages field to final size of index */
    (*stats).num_pages = num_pages;

    MemoryContextDelete(vstate.pagedelcontext);

    /*
     * If there were any calls to _bt_pagedel() during scan of the index then
     * see if any of the resulting pages can be placed in the FSM now.  When
     * it's not safe we'll have to leave it up to a future VACUUM operation.
     *
     * Finally, if we placed any pages in the FSM (either just now or during
     * the scan), forcibly update the upper-level FSM pages to ensure that
     * searchers can find them.
     */
    _bt_pendingfsm_finalize(rel, &mut vstate as *mut BTVacState);
    if (*stats).pages_free > 0 {
        IndexFreeSpaceMapVacuum(rel);
    }
}

/*
 * btvacuumpage --- VACUUM one page
 *
 * This processes a single page for btvacuumscan().  In some cases we must
 * backtrack to re-examine and VACUUM pages that were on buf's page during
 * a previous call here.  This is how we handle page splits (that happened
 * after our cycleid was acquired) whose right half page happened to reuse
 * a block that we might have processed at some point before it was
 * recycled (i.e. before the page split).
 *
 * Returns BlockNumber of a scanned page (not backtracked).
 */
unsafe fn btvacuumpage(vstate: *mut BTVacState, mut buf: Buffer) -> BlockNumber {
    let info: *mut IndexVacuumInfo = (*vstate).info;
    let stats: *mut IndexBulkDeleteResult = (*vstate).stats;
    let callback: IndexBulkDeleteCallback = (*vstate).callback;
    let callback_state: *mut c_void = (*vstate).callback_state;
    let rel: Relation = (*info).index;
    let heaprel: Relation = (*info).heaprel;
    let mut attempt_pagedel: bool;
    let mut blkno: BlockNumber;
    let mut backtrack_to: BlockNumber;
    let scanblkno: BlockNumber = BufferGetBlockNumber(buf);
    let mut page: Page;
    let mut opaque: BTPageOpaque;

    blkno = scanblkno;

    /* C label: backtrack */
    'backtrack: loop {
        attempt_pagedel = false;
        backtrack_to = P_NONE;

        _bt_lockbuf(rel, buf, BT_READ);
        page = BufferGetPage(buf);
        opaque = std::ptr::null_mut();
        if !PageIsNew(page) {
            _bt_checkpage(rel, buf);
            opaque = BTPageGetOpaque(page);
        }

        Assert!(blkno <= scanblkno);
        if blkno != scanblkno {
            /*
             * We're backtracking.
             *
             * We followed a right link to a sibling leaf page (a page that
             * happens to be from a block located before scanblkno).  The only
             * case we want to do anything with is a live leaf page having the
             * current vacuum cycle ID.
             *
             * The page had better be in a state that's consistent with what we
             * expect.  Check for conditions that imply corruption in passing.  It
             * can't be half-dead because only an interrupted VACUUM process can
             * leave pages in that state, so we'd definitely have dealt with it
             * back when the page was the scanblkno page (half-dead pages are
             * always marked fully deleted by _bt_pagedel(), barring corruption).
             */
            if opaque.is_null() || !P_ISLEAF(opaque) || P_ISHALFDEAD(opaque) {
                Assert!(false);
                ereport!(
                    LOG,
                    /* C also: errcode(ERRCODE_INDEX_CORRUPTED) */                    errmsg_internal!(
                        "right sibling {} of scanblkno {} unexpectedly in an inconsistent state in index \"{}\"",
                        blkno,
                        scanblkno,
                        std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                    )
                );
                _bt_relbuf(rel, buf);
                return scanblkno;
            }

            /*
             * We may have already processed the page in an earlier call, when the
             * page was scanblkno.  This happens when the leaf page split occurred
             * after the scan began, but before the right sibling page became the
             * scanblkno.
             *
             * Page may also have been deleted by current btvacuumpage() call,
             * since _bt_pagedel() sometimes deletes the right sibling page of
             * scanblkno in passing (it does so after we decided where to
             * backtrack to).  We don't need to process this page as a deleted
             * page a second time now (in fact, it would be wrong to count it as a
             * deleted page in the bulk delete statistics a second time).
             */
            if (*opaque).btpo_cycleid != (*vstate).cycleid || P_ISDELETED(opaque) {
                /* Done with current scanblkno (and all lower split pages) */
                _bt_relbuf(rel, buf);
                return scanblkno;
            }
        }

        if opaque.is_null() || BTPageIsRecyclable(page, heaprel) {
            /* Okay to recycle this page (which could be leaf or internal) */
            RecordFreeIndexPage(rel, blkno);
            (*stats).pages_deleted += 1;
            (*stats).pages_free += 1;
        } else if P_ISDELETED(opaque) {
            /*
             * Already deleted page (which could be leaf or internal).  Can't
             * recycle yet.
             */
            (*stats).pages_deleted += 1;
        } else if P_ISHALFDEAD(opaque) {
            /* Half-dead leaf page (from interrupted VACUUM) -- finish deleting */
            attempt_pagedel = true;

            /*
             * _bt_pagedel() will increment both pages_newly_deleted and
             * pages_deleted stats in all cases (barring corruption)
             */
        } else if P_ISLEAF(opaque) {
            let mut deletable: [OffsetNumber; MaxIndexTuplesPerPage as usize] =
                [0; MaxIndexTuplesPerPage as usize];
            let mut ndeletable: c_int;
            let mut updatable: [BTVacuumPosting; MaxIndexTuplesPerPage as usize] =
                [std::ptr::null_mut(); MaxIndexTuplesPerPage as usize];
            let mut nupdatable: c_int;
            let mut offnum: OffsetNumber;
            let mut minoff: OffsetNumber;
            let mut maxoff: OffsetNumber;
            let mut nhtidsdead: c_int;
            let mut nhtidslive: c_int;

            /*
             * Trade in the initial read lock for a full cleanup lock on this
             * page.  We must get such a lock on every leaf page over the course
             * of the vacuum scan, whether or not it actually contains any
             * deletable tuples --- see nbtree/README.
             */
            _bt_upgradelockbufcleanup(rel, buf);

            /*
             * Check whether we need to backtrack to earlier pages.  What we are
             * concerned about is a page split that happened since we started the
             * vacuum scan.  If the split moved tuples on the right half of the
             * split (i.e. the tuples that sort high) to a block that we already
             * passed over, then we might have missed the tuples.  We need to
             * backtrack now.  (Must do this before possibly clearing btpo_cycleid
             * or deleting scanblkno page below!)
             */
            if (*vstate).cycleid != 0
                && (*opaque).btpo_cycleid == (*vstate).cycleid
                && (*opaque).btpo_flags & BTP_SPLIT_END == 0
                && !P_RIGHTMOST(opaque)
                && (*opaque).btpo_next < scanblkno
            {
                backtrack_to = (*opaque).btpo_next;
            }

            ndeletable = 0;
            nupdatable = 0;
            minoff = P_FIRSTDATAKEY(opaque);
            maxoff = PageGetMaxOffsetNumber(page);
            nhtidsdead = 0;
            nhtidslive = 0;
            if let Some(cb) = callback {
                /* btbulkdelete callback tells us what to delete (or update) */
                offnum = minoff;
                while offnum <= maxoff {
                    let mut itup: IndexTuple;

                    itup = PageGetItem(page, PageGetItemId(page, offnum))
                        as IndexTuple;

                    Assert!(!BTreeTupleIsPivot(itup));
                    if !BTreeTupleIsPosting(itup) {
                        /* Regular tuple, standard table TID representation */
                        if cb(&mut (*itup).t_tid as *mut _ as _, callback_state) {
                            deletable[ndeletable as usize] = offnum;
                            ndeletable += 1;
                            nhtidsdead += 1;
                        } else {
                            nhtidslive += 1;
                        }
                    } else {
                        let mut vacposting: BTVacuumPosting;
                        let mut nremaining: c_int = 0;

                        /* Posting list tuple */
                        vacposting = btreevacuumposting(
                            vstate,
                            itup,
                            offnum,
                            &mut nremaining as *mut c_int,
                        );
                        if vacposting.is_null() {
                            /*
                             * All table TIDs from the posting tuple remain, so no
                             * delete or update required
                             */
                            Assert!(nremaining == BTreeTupleGetNPosting(itup));
                        } else if nremaining > 0 {
                            /*
                             * Store metadata about posting list tuple in
                             * updatable array for entire page.  Existing tuple
                             * will be updated during the later call to
                             * _bt_delitems_vacuum().
                             */
                            Assert!(nremaining < BTreeTupleGetNPosting(itup));
                            updatable[nupdatable as usize] = vacposting;
                            nupdatable += 1;
                            nhtidsdead +=
                                BTreeTupleGetNPosting(itup) - nremaining;
                        } else {
                            /*
                             * All table TIDs from the posting list must be
                             * deleted.  We'll delete the index tuple completely
                             * (no update required).
                             */
                            Assert!(nremaining == 0);
                            deletable[ndeletable as usize] = offnum;
                            ndeletable += 1;
                            nhtidsdead += BTreeTupleGetNPosting(itup);
                            pfree(vacposting as *mut c_void);
                        }

                        nhtidslive += nremaining;
                    }
                    offnum = OffsetNumberNext(offnum);
                }
            }

            /*
             * Apply any needed deletes or updates.  We issue just one
             * _bt_delitems_vacuum() call per page, so as to minimize WAL traffic.
             */
            if ndeletable > 0 || nupdatable > 0 {
                Assert!(nhtidsdead >= ndeletable + nupdatable);
                _bt_delitems_vacuum(
                    rel,
                    buf,
                    deletable.as_mut_ptr(),
                    ndeletable,
                    updatable.as_mut_ptr(),
                    nupdatable,
                );

                (*stats).tuples_removed += nhtidsdead as f64;
                /* must recompute maxoff */
                maxoff = PageGetMaxOffsetNumber(page);

                /* can't leak memory here */
                for i in 0..nupdatable as usize {
                    pfree(updatable[i] as *mut c_void);
                }
            } else {
                /*
                 * If the leaf page has been split during this vacuum cycle, it
                 * seems worth expending a write to clear btpo_cycleid even if we
                 * don't have any deletions to do.  (If we do, _bt_delitems_vacuum
                 * takes care of this.)  This ensures we won't process the page
                 * again.
                 *
                 * We treat this like a hint-bit update because there's no need to
                 * WAL-log it.
                 */
                Assert!(nhtidsdead == 0);
                if (*vstate).cycleid != 0
                    && (*opaque).btpo_cycleid == (*vstate).cycleid
                {
                    (*opaque).btpo_cycleid = 0;
                    MarkBufferDirtyHint(buf, true);
                }
            }

            /*
             * If the leaf page is now empty, try to delete it; else count the
             * live tuples (live table TIDs in posting lists are counted as
             * separate live tuples).  We don't delete when backtracking, though,
             * since that would require teaching _bt_pagedel() about backtracking
             * (doesn't seem worth adding more complexity to deal with that).
             *
             * We don't count the number of live TIDs during cleanup-only calls to
             * btvacuumscan (i.e. when callback is not set).  We count the number
             * of index tuples directly instead.  This avoids the expense of
             * directly examining all of the tuples on each page.  VACUUM will
             * treat num_index_tuples as an estimate in cleanup-only case, so it
             * doesn't matter that this underestimates num_index_tuples
             * significantly in some cases.
             */
            if minoff > maxoff {
                attempt_pagedel = blkno == scanblkno;
            } else if callback.is_some() {
                (*stats).num_index_tuples += nhtidslive as f64;
            } else {
                (*stats).num_index_tuples += (maxoff - minoff + 1) as f64;
            }

            Assert!(!attempt_pagedel || nhtidslive == 0);
        }

        if attempt_pagedel {
            let mut oldcontext: MemoryContext;

            /* Run pagedel in a temp context to avoid memory leakage */
            MemoryContextReset((*vstate).pagedelcontext);
            oldcontext = MemoryContextSwitchTo((*vstate).pagedelcontext);

            /*
             * _bt_pagedel maintains the bulk delete stats on our behalf;
             * pages_newly_deleted and pages_deleted are likely to be incremented
             * during call
             */
            Assert!(blkno == scanblkno);
            _bt_pagedel(rel, buf, vstate);

            MemoryContextSwitchTo(oldcontext);
            /* pagedel released buffer, so we shouldn't */
        } else {
            _bt_relbuf(rel, buf);
        }

        if backtrack_to != P_NONE {
            blkno = backtrack_to;

            /* check for vacuum delay while not holding any buffer lock */
            vacuum_delay_point(false);

            /*
             * We can't use _bt_getbuf() here because it always applies
             * _bt_checkpage(), which will barf on an all-zero page. We want to
             * recycle all-zero pages, not fail.  Also, we want to use a
             * nondefault buffer access strategy.
             */
            buf = ReadBufferExtended(
                rel,
                MAIN_FORKNUM,
                blkno,
                RBM_NORMAL,
                (*info).strategy as *mut BufferAccessStrategy,
            );
            /* goto backtrack */
            continue 'backtrack;
        }

        break 'backtrack;
    } /* end 'backtrack loop */

    scanblkno
}

/*
 * btreevacuumposting --- determine TIDs still needed in posting list
 *
 * Returns metadata describing how to build replacement tuple without the TIDs
 * that VACUUM needs to delete.  Returned value is NULL in the common case
 * where no changes are needed to caller's posting list tuple (we avoid
 * allocating memory here as an optimization).
 *
 * The number of TIDs that should remain in the posting list tuple is set for
 * caller in *nremaining.
 */
unsafe fn btreevacuumposting(
    vstate: *mut BTVacState,
    posting: IndexTuple,
    updatedoffset: OffsetNumber,
    nremaining: *mut c_int,
) -> BTVacuumPosting {
    let mut live: c_int = 0;
    let nitem: c_int = BTreeTupleGetNPosting(posting);
    let items: *mut ItemPointerData = BTreeTupleGetPosting(posting);
    let mut vacposting: BTVacuumPosting = std::ptr::null_mut();

    for i in 0..nitem as usize {
        if !((*vstate).callback.unwrap())(items.add(i) as _, (*vstate).callback_state) {
            /* Live table TID */
            live += 1;
        } else if vacposting.is_null() {
            /*
             * First dead table TID encountered.
             *
             * It's now clear that we need to delete one or more dead table
             * TIDs, so start maintaining metadata describing how to update
             * existing posting list tuple.
             */
            vacposting = palloc(
                offset_of!(BTVacuumPostingData, deletetids)
                    + nitem as usize * size_of::<uint16>(),
            ) as BTVacuumPosting;

            (*vacposting).itup = posting;
            (*vacposting).updatedoffset = updatedoffset;
            (*vacposting).ndeletedtids = 0;
            (*vacposting).deletetids[(*vacposting).ndeletedtids as usize] = i as uint16;
            (*vacposting).ndeletedtids += 1;
        } else {
            /* Second or subsequent dead table TID */
            (*vacposting).deletetids[(*vacposting).ndeletedtids as usize] = i as uint16;
            (*vacposting).ndeletedtids += 1;
        }
    }

    *nremaining = live;
    vacposting
}

/*
 *	btcanreturn() -- Check whether btree indexes support index-only scans.
 *
 * btrees always do, so this is trivial.
 */
pub unsafe extern "C" fn btcanreturn(index: Relation, attno: c_int) -> bool {
    true
}

/*
 * btgettreeheight() -- Compute tree height for use by btcostestimate().
 */
pub unsafe extern "C" fn btgettreeheight(rel: Relation) -> c_int {
    _bt_getrootheight(rel)
}

pub unsafe extern "C" fn bttranslatestrategy(strategy: StrategyNumber, opfamily: Oid) -> CompareType {
    match strategy {
        BTLessStrategyNumber => COMPARE_LT,
        BTLessEqualStrategyNumber => COMPARE_LE,
        BTEqualStrategyNumber => COMPARE_EQ,
        BTGreaterEqualStrategyNumber => COMPARE_GE,
        BTGreaterStrategyNumber => COMPARE_GT,
        _ => COMPARE_INVALID,
    }
}

pub unsafe extern "C" fn bttranslatecmptype(cmptype: CompareType, opfamily: Oid) -> StrategyNumber {
    match cmptype {
        COMPARE_LT => BTLessStrategyNumber,
        COMPARE_LE => BTLessEqualStrategyNumber,
        COMPARE_EQ => BTEqualStrategyNumber,
        COMPARE_GE => BTGreaterEqualStrategyNumber,
        COMPARE_GT => BTGreaterStrategyNumber,
        _ => InvalidStrategy,
    }
}
