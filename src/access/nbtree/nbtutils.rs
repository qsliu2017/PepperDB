//! nbtutils.rs
//!   Utility code for Postgres btree implementation.
//!
//! Translated 1:1 from postgres/src/backend/access/nbtree/nbtutils.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/access/nbtree/nbtutils.c
//!
//! #include mapping:
//!   "postgres.h"           -> crate::prelude::*
//!   "access/nbtree.h"      -> BTScanInsert/BTScanOpaque/BTStack/BTArrayKeyInfo/
//!                             BTReadPageState/BTScanPosItem etc. (stubs below;
//!                             real home access/nbtree.h, TODO(pg-port))
//!   "access/reloptions.h"  -> btoptions / BTOptions (stubs)
//!   "commands/progress.h"  -> PROGRESS_* constants (stubs)
//!   "miscadmin.h"          -> MaxBackends / IsUnderPostmaster (stubs)
//!   "utils/datum.h"        -> datumCopy / datum_image_eq (stubs)
//!   "utils/lsyscache.h"    -> get_opfamily_proc (stub)

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

use std::mem::{size_of, offset_of};
use std::ffi::{c_char, c_int, c_void};

use crate::c::{int16, int32, uint8, uint16, uint32, Size};

// ---------------------------------------------------------------------------
// Real, already-ported homes.
// ---------------------------------------------------------------------------
use crate::access::common::indextuple::{
    IndexTuple, IndexTupleData, IndexTupleSize, INDEX_SIZE_MASK,
};
use crate::access::common::tupdesc::TupleDesc;
use crate::access::common::scankey::{
    ScanKey, ScanKeyData, ScanKeyEntryInitializeWithInfo,
    SK_ISNULL, SK_SEARCHNULL, SK_SEARCHNOTNULL, SK_SEARCHARRAY,
    SK_ROW_HEADER, SK_ROW_MEMBER, SK_ROW_END,
};
use crate::access::stratnum::{
    InvalidStrategy, StrategyNumber,
    BTEqualStrategyNumber, BTLessStrategyNumber, BTLessEqualStrategyNumber,
    BTGreaterEqualStrategyNumber, BTGreaterStrategyNumber,
};
use crate::access::attnum::{AttrNumber, InvalidAttrNumber};
use crate::storage::bufpage::{
    Page, PageGetItem, PageGetItemId, PageGetMaxOffsetNumber,
};
use crate::storage::buf::Buffer;
use crate::storage::block::BlockNumber;
use crate::storage::itemid::{ItemId, ItemIdData, ItemIdIsDead, ItemIdMarkDead};
use crate::storage::itemptr::{
    ItemPointer, ItemPointerData, ItemPointerCompare, ItemPointerCopy,
    ItemPointerEquals, ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber,
    ItemPointerGetOffsetNumberNoCheck, ItemPointerSetOffsetNumber,
};
use crate::storage::off::OffsetNumberPrev;
use crate::storage::off::{
    FirstOffsetNumber, InvalidOffsetNumber, OffsetNumber, OffsetNumberNext,
};
use crate::utils::rel::Relation;
use crate::utils::fmgr::{FmgrInfo, FunctionCall2Coll, OidFunctionCall1Coll};
use crate::postgres::{DatumGetBool, DatumGetInt32, DatumGetPointer, PointerGetDatum, ObjectIdGetDatum};

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

// ---------------------------------------------------------------------------
// STUBS: symbols from access/nbtree.h and related headers that have not been
// ported yet.  Each is a minimal local declaration.
// TODO(pg-port): real definitions live in postgres/src/include/access/nbtree.h
// ---------------------------------------------------------------------------

/// TODO(pg-port): BTStack / BTStackData live in access/nbtree.h.
#[repr(C)]
pub struct BTStackData {
    pub bts_blkno:  BlockNumber,
    pub bts_offset: OffsetNumber,
    pub bts_btentry: ItemPointerData,
    pub bts_parent:  BTStack,
}
pub type BTStack = *mut BTStackData;

/// TODO(pg-port): BTScanInsertData / BTScanInsert live in access/nbtree.h.
#[repr(C)]
pub struct BTScanInsertData {
    pub heapkeyspace:    bool,
    pub allequalimage:   bool,
    pub anynullkeys:     bool,
    pub nextkey:         bool,
    pub backward:        bool,
    pub keysz:           c_int,
    pub scantid:         ItemPointer,
    /// flexible array -- at least 1 element required at end of struct
    pub scankeys:        [ScanKeyData; 1],
}
pub type BTScanInsert = *mut BTScanInsertData;

/// TODO(pg-port): BTArrayKeyInfo lives in access/nbtree.h.
#[repr(C)]
pub struct BTArrayKeyInfo {
    pub scan_key:     c_int,
    pub cur_elem:     c_int,
    pub num_elems:    c_int,    /* -1 for skip arrays */
    pub elem_values:  *mut Datum,
    pub null_elem:    bool,
    pub attbyval:     bool,
    pub attlen:       c_int,
    pub low_compare:  ScanKey,
    pub high_compare: ScanKey,
    pub sksup:        *mut BTSkipArraySupport,
}

/// TODO(pg-port): BTSkipArraySupport (opclass skip callback vtable).
#[repr(C)]
pub struct BTSkipArraySupport {
    pub low_elem:  Datum,
    pub high_elem: Datum,
    pub decrement: unsafe extern "C" fn(rel: Relation, current: Datum, uflow: *mut bool) -> Datum,
    pub increment: unsafe extern "C" fn(rel: Relation, current: Datum, oflow: *mut bool) -> Datum,
}

/// TODO(pg-port): BTScanPosItem lives in access/nbtree.h.
pub use crate::access::nbtree::nbtsearch::{BTScanPosItem};


/// TODO(pg-port): BTScanPosData / BTScanPos lives in access/nbtree.h.
pub use crate::access::nbtree::nbtsearch::{BTScanPosData};

pub type BTScanPos = *mut BTScanPosData;

/// TODO(pg-port): BTScanOpaqueData / BTScanOpaque live in access/nbtree.h.
pub use crate::access::nbtree::nbtree::{BTScanOpaqueData, BTScanOpaque};


/// TODO(pg-port): BTReadPageState lives in access/nbtree.h.
#[repr(C)]
pub struct BTReadPageState {
    pub minoff:           OffsetNumber,
    pub maxoff:           OffsetNumber,
    pub finaltup:         IndexTuple,
    pub page:             Page,
    pub firstpage:        bool,
    pub forcenonrequired: bool,
    pub startikey:        c_int,
    pub offnum:           OffsetNumber,
    pub skip:             OffsetNumber,
    pub continuescan:     bool,
    pub rechecks:         c_int,
    pub targetdistance:   c_int,
    pub nskipadvances:    c_int,
}

/// TODO(pg-port): BTPageOpaqueData / BTPageOpaque live in access/nbtree.h.
#[repr(C)]
pub struct BTPageOpaqueData {
    pub btpo_prev:   BlockNumber,
    pub btpo_next:   BlockNumber,
    pub btpo_level:  u32,
    pub btpo_flags:  u16,
    pub btpo_cycleid: u16,
}
pub type BTPageOpaque = *mut BTPageOpaqueData;

/// TODO(pg-port): BTOptions lives in access/reloptions.h (btree specialization).
#[repr(C)]
pub struct BTOptions {
    pub vl_len_:                       int32,
    pub fillfactor:                    c_int,
    pub vacuum_cleanup_index_scale_factor: f64,
    pub deduplicate_items:             bool,
}

/// TODO(pg-port): BTOneVacInfo and BTVacInfo live in nbtutils.c (file-static).
#[repr(C)]
pub struct BTOneVacInfo {
    pub relid:   LockRelId,
    pub cycleid: BTCycleId,
}

#[repr(C)]
pub struct BTVacInfo {
    pub cycle_ctr:   BTCycleId,
    pub num_vacuums: c_int,
    pub max_vacuums: c_int,
    pub vacuums:     [BTOneVacInfo; 0], /* FLEXIBLE_ARRAY_MEMBER */
}

// ---------------------------------------------------------------------------
// nbtree.h constants (stubs).
// ---------------------------------------------------------------------------
/// TODO(pg-port): from access/nbtree.h.
pub const BTMaxItemSize: Size = 1128;
/// TODO(pg-port): from access/nbtree.h: BTMaxItemSizeNoHeapTid.
pub const BTMaxItemSizeNoHeapTid: Size = 1132;
/// TODO(pg-port): from access/nbtree.h.
pub const MAX_BT_CYCLE_ID: u16 = 0xFF7F;
/// TODO(pg-port): from access/nbtree.h.
pub const BTORDER_PROC: c_int = 1;
/// TODO(pg-port): from access/nbtree.h.
pub const BTEQUALIMAGE_PROC: c_int = 4;
/// TODO(pg-port): from access/nbtree.h.
pub const BTREE_VERSION: u32 = 4;
/// TODO(pg-port): from access/nbtree.h.
pub const BTREE_NOVAC_VERSION: u32 = 2;
/// TODO(pg-port): from access/nbtree.h.
pub const BTP_HAS_GARBAGE: u16 = 1 << 6;
/// TODO(pg-port): from access/nbtree.h: bit stored in t_tid offset.
pub const BT_PIVOT_HEAP_TID_ATTR: u16 = 0x8000;
/// TODO(pg-port): from access/nbtree.h: SK_BT_* flag bits.
pub const SK_BT_REQFWD:      u32 = 0x00010000;
pub const SK_BT_REQBKWD:     u32 = 0x00020000;
pub const SK_BT_INDOPTION_SHIFT: c_int = 24;
pub const SK_BT_DESC:        u32 = (INDOPTION_DESC as u32) << SK_BT_INDOPTION_SHIFT as u32;
pub const SK_BT_NULLS_FIRST: u32 = (INDOPTION_NULLS_FIRST as u32) << SK_BT_INDOPTION_SHIFT as u32;
pub const SK_BT_SKIP:        u32 = 0x00100000;
pub const SK_BT_MINVAL:      u32 = 0x00200000;
pub const SK_BT_MAXVAL:      u32 = 0x00400000;
pub const SK_BT_NEXT:        u32 = 0x00800000;
pub const SK_BT_PRIOR:       u32 = 0x01000000;
/// TODO(pg-port): from access/nbtree.h: MaxIndexTuplesPerPage.
pub const MaxIndexTuplesPerPage: c_int = 1358;

/// TODO(pg-port): INDOPTION_DESC / INDOPTION_NULLS_FIRST from access/genam.h.
pub const INDOPTION_DESC:       int16 = 0x0001;
pub const INDOPTION_NULLS_FIRST: int16 = 0x0002;

/// TODO(pg-port): ScanDirection type (ForwardScanDirection etc.) lives in
///   access/sdir.h (ported separately).
pub type ScanDirection = i32;
pub const ForwardScanDirection:   ScanDirection =  1;
pub const NoMovementScanDirection: ScanDirection = 0;
pub const BackwardScanDirection:  ScanDirection = -1;

/// TODO(pg-port): IndexScanDesc / IndexScanDescData live in access/relscan.h.
pub use crate::access::relscan::{IndexScanDescData, IndexScanDesc};


/// TODO(pg-port): BTCycleId type from access/nbtree.h.
pub type BTCycleId = uint16;

pub use crate::utils::rel::LockRelId;

/// TODO(pg-port): progress phase constants from commands/progress.h.
pub const PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE:       i64 = 1;
pub const PROGRESS_BTREE_PHASE_INDEXBUILD_TABLESCAN:    i64 = 2;
pub const PROGRESS_BTREE_PHASE_PERFORMSORT_1:           i64 = 3;
pub const PROGRESS_BTREE_PHASE_PERFORMSORT_2:           i64 = 4;
pub const PROGRESS_BTREE_PHASE_LEAF_LOAD:               i64 = 5;

/// TODO(pg-port): AMPROP_RETURNABLE from catalog/pg_amproc.h / index.h.
pub type IndexAMProperty = c_int;
pub const AMPROP_RETURNABLE: IndexAMProperty = 3;

/// TODO(pg-port): relopt_parse_elt / RELOPT_TYPE_INT / RELOPT_KIND_BTREE.
#[repr(C)]
pub struct relopt_parse_elt {
    pub optname: *const c_char,
    pub opttype: c_int,
    pub offset:  c_int,
}
pub const RELOPT_TYPE_INT:  c_int = 0;
pub const RELOPT_TYPE_REAL: c_int = 1;
pub const RELOPT_TYPE_BOOL: c_int = 2;
pub const RELOPT_KIND_BTREE: c_int = 4;

// ---------------------------------------------------------------------------
// Lookup constants for LOOK_AHEAD heuristics.
// ---------------------------------------------------------------------------
pub const LOOK_AHEAD_REQUIRED_RECHECKS: c_int = 3;
pub const LOOK_AHEAD_DEFAULT_DISTANCE:  c_int = 5;
pub const NSKIPADVANCES_THRESHOLD:      c_int = 3;

// ---------------------------------------------------------------------------
// Forward declarations of helpers (file-static in C; private here).
// ---------------------------------------------------------------------------
extern "C" {
    /// TODO(pg-port): parallel_scan glue: _bt_parallel_done / _bt_parallel_primscan_schedule.
    fn _bt_parallel_done(scan: IndexScanDesc);
    fn _bt_parallel_primscan_schedule(scan: IndexScanDesc, currPage: BlockNumber);

    // bufmgr stubs
    fn _bt_lockbuf(rel: Relation, buf: Buffer, access: c_int);
    fn _bt_unlockbuf(rel: Relation, buf: Buffer);
    fn _bt_getbuf(rel: Relation, blkno: BlockNumber, access: c_int) -> Buffer;
    fn _bt_relbuf(rel: Relation, buf: Buffer);
    fn BufferGetPage(buf: Buffer) -> Page;
    fn BufferGetLSNAtomic(buf: Buffer) -> u64;
    fn MarkBufferDirtyHint(buf: Buffer, buffer_std: bool);

    // utils stubs
    fn datumCopy(value: Datum, typByVal: bool, typLen: c_int) -> Datum;
    fn datum_image_eq(val1: Datum, val2: Datum, byval: bool, len: c_int) -> bool;
    fn get_opfamily_proc(opfamily: Oid, lefttype: Oid, righttype: Oid, procnum: c_int) -> Oid;
    fn build_reloptions(
        reloptions: Datum, validate: bool, kind: c_int,
        relopt_struct_size: Size,
        tab: *const relopt_parse_elt, nelems: c_int,
    ) -> *mut c_void;
    fn add_size(s1: Size, s2: Size) -> Size;
    fn mul_size(s1: Size, s2: Size) -> Size;
    fn ShmemInitStruct(name: *const c_char, size: Size, foundPtr: *mut bool) -> *mut c_void;
    fn OidIsValid(oid: Oid) -> bool;
    fn lengthof_impl(n: usize) -> c_int;

    // relation/index stubs
    fn RelationGetDescr(rel: Relation) -> TupleDesc;
    fn IndexRelationGetNumberOfKeyAttributes(rel: Relation) -> c_int;
    fn IndexRelationGetNumberOfAttributes(rel: Relation) -> c_int;
    fn index_getprocinfo(rel: Relation, attnum: c_int, procnum: uint32) -> *mut FmgrInfo;
    fn index_getattr(tup: IndexTuple, attnum: c_int, tupdesc: TupleDesc, isnull: *mut bool) -> Datum;
    fn index_truncate_tuple(tupdesc: TupleDesc, itup: IndexTuple, newnatts: c_int) -> IndexTuple;
    fn RelationGetRelationName(rel: Relation) -> *const c_char;
    fn RelationNeedsWAL(rel: Relation) -> bool;

    // shmem / locking stubs
    fn LWLockAcquire(lock: *mut core::ffi::c_void, mode: c_int) -> bool;
    fn LWLockRelease(lock: *mut core::ffi::c_void);

    // error/logging stubs (already in prelude but keep explicit for clarity)
    fn errcode(sqlerrcode: c_int) -> c_int;
    fn errmsg(fmt: *const c_char, ...) -> c_int;
    fn errdetail(fmt: *const c_char, ...) -> c_int;
    fn errhint(fmt: *const c_char, ...) -> c_int;
    fn errtableconstraint(rel: Relation, conname: *const c_char) -> c_int;
}

/// TODO(pg-port): access/nbtree.h inline helpers.
unsafe fn BTPageGetOpaque(page: Page) -> BTPageOpaque {
    crate::access::nbtree::nbtdedup::BTPageGetOpaque(page as _) as _
}
unsafe fn P_FIRSTDATAKEY(opaque: BTPageOpaque) -> OffsetNumber {
    crate::access::nbtree::nbtdedup::P_FIRSTDATAKEY(opaque as _) as _
}
unsafe fn P_ISLEAF(opaque: BTPageOpaque) -> bool {
    crate::access::nbtree::nbtpage::P_ISLEAF(opaque as _)
}
unsafe fn P_RIGHTMOST(opaque: BTPageOpaque) -> bool {
    crate::access::nbtree::nbtdedup::P_RIGHTMOST(opaque as _)
}
unsafe fn P_LEFTMOST(opaque: BTPageOpaque) -> bool {
    crate::access::nbtree::nbtpage::P_LEFTMOST(opaque as _)
}
unsafe fn P_IGNORE(opaque: BTPageOpaque) -> bool {
    crate::access::nbtree::nbtinsert::P_IGNORE(opaque as _)
}
unsafe fn BTreeTupleGetNAtts(itup: IndexTuple, rel: Relation) -> c_int {
    crate::access::nbtree::nbtinsert::BTreeTupleGetNAtts(itup as _, rel as _)
}
unsafe fn BTreeTupleIsPivot(itup: IndexTuple) -> bool {
    crate::access::nbtree::nbtdedup::BTreeTupleIsPivot(itup as _)
}
unsafe fn BTreeTupleIsPosting(itup: IndexTuple) -> bool {
    crate::access::nbtree::nbtdedup::BTreeTupleIsPosting(itup as _)
}
unsafe fn BTreeTupleGetNPosting(itup: IndexTuple) -> c_int {
    crate::access::nbtree::nbtdedup::BTreeTupleGetNPosting(itup as _) as _
}
unsafe fn BTreeTupleGetPostingN(itup: IndexTuple, n: c_int) -> ItemPointer {
    crate::access::nbtree::nbtdedup::BTreeTupleGetPostingN(itup as _, n as _) as _
}
unsafe fn BTreeTupleGetPostingOffset(itup: IndexTuple) -> Size {
    crate::access::nbtree::nbtdedup::BTreeTupleGetPostingOffset(itup as _) as _
}
unsafe fn BTreeTupleGetHeapTID(itup: IndexTuple) -> ItemPointer {
    crate::access::nbtree::nbtdedup::BTreeTupleGetHeapTID(itup as _) as _
}
unsafe fn BTreeTupleGetMaxHeapTID(itup: IndexTuple) -> ItemPointer {
    crate::access::nbtree::nbtdedup::BTreeTupleGetMaxHeapTID(itup as _) as _
}
unsafe fn BTreeTupleSetNAtts(itup: IndexTuple, natts: c_int, heaptid: bool) {
    crate::access::nbtree::nbtdedup::BTreeTupleSetNAtts(itup as _, natts, heaptid)
}
unsafe fn BTScanPosIsValid(pos: BTScanPosData) -> bool {
    crate::access::nbtree::nbtsearch::BTScanPosIsValid(&pos)
}
unsafe fn BTScanPosIsPinned(pos: BTScanPosData) -> bool {
    crate::access::nbtree::nbtsearch::BTScanPosIsPinned(&pos)
}
unsafe fn _bt_metaversion(rel: Relation, heapkeyspace: *mut bool, allequalimage: *mut bool) {
    crate::access::nbtree::nbtpage::_bt_metaversion(rel as _, heapkeyspace as _, allequalimage as _)
}
pub unsafe fn TupleDescCompactAttr(tupdesc: TupleDesc, attnum: c_int) -> *mut CompactAttribute {
    crate::access::common::tupdesc::TupleDescCompactAttr(tupdesc as _, attnum as _) as _
}

pub use crate::access::common::tupdesc::CompactAttribute;

// LW lock constants -- TODO(pg-port): storage/lwlock.h.
// Real builtin LWLock, populated at LWLock init (see lwlock.rs assign! macro).
use crate::backend_link_shims::BtreeVacuumLock;
pub const LW_SHARED:    c_int = 1;
pub const LW_EXCLUSIVE: c_int = 2;
pub const BT_READ:      c_int = 1;

// Inline helpers.
#[inline]
pub unsafe fn ScanDirectionIsForward(dir: ScanDirection) -> bool {
    dir > 0
}
#[inline]
pub unsafe fn ScanDirectionIsBackward(dir: ScanDirection) -> bool {
    dir < 0
}
#[inline]
pub unsafe fn ScanDirectionIsNoMovement(dir: ScanDirection) -> bool {
    dir == 0
}
/// INVERT_COMPARE_RESULT macro.
#[inline]
fn INVERT_COMPARE_RESULT(r: &mut i32) {
    *r = if *r < 0 { 1 } else if *r > 0 { -1 } else { 0 };
}

// End of Part 1: imports + stubs.

// ===========================================================================
// Part 2 -- _bt_mkscankey, _bt_freestack, array-key helpers
// ===========================================================================

/*
 * _bt_mkscankey
 *		Build an insertion scan key that contains comparison data from itup
 *		as well as comparator routines appropriate to the key datatypes.
 */
pub unsafe fn _bt_mkscankey(rel: Relation, itup: IndexTuple) -> BTScanInsert {
    let itupdesc: TupleDesc;
    let indnkeyatts: c_int;
    let indoption: *mut int16;
    let tupnatts: c_int;
    let i: c_int;

    itupdesc = RelationGetDescr(rel);
    indnkeyatts = IndexRelationGetNumberOfKeyAttributes(rel);
    indoption = (*rel).rd_indoption;
    tupnatts = if !itup.is_null() {
        BTreeTupleGetNAtts(itup, rel)
    } else {
        0
    };

    Assert!(tupnatts <= IndexRelationGetNumberOfAttributes(rel));

    /*
     * We'll execute search using scan key constructed on key columns.
     * Truncated attributes and non-key attributes are omitted from the final
     * scan key.
     */
    let key: BTScanInsert = palloc(
        offset_of!(BTScanInsertData, scankeys)
            + size_of::<ScanKeyData>() * indnkeyatts as usize,
    ) as BTScanInsert;
    if !itup.is_null() {
        _bt_metaversion(rel, &mut (*key).heapkeyspace, &mut (*key).allequalimage);
    } else {
        /* Utility statement callers can set these fields themselves */
        (*key).heapkeyspace = true;
        (*key).allequalimage = false;
    }
    (*key).anynullkeys = false; /* initial assumption */
    (*key).nextkey = false;     /* usual case, required by btinsert */
    (*key).backward = false;    /* usual case, required by btinsert */
    (*key).keysz = Min(indnkeyatts, tupnatts);
    (*key).scantid = if (*key).heapkeyspace && !itup.is_null() {
        BTreeTupleGetHeapTID(itup)
    } else {
        core::ptr::null_mut()
    };
    let skey: ScanKey = (*key).scankeys.as_mut_ptr();
    let mut i: c_int = 0;
    while i < indnkeyatts {
        let procinfo: *mut FmgrInfo;
        let mut arg: Datum = 0;
        let mut null: bool = false;
        let flags: c_int;

        /*
         * We can use the cached (default) support procs since no cross-type
         * comparison can be needed.
         */
        procinfo = index_getprocinfo(rel, i + 1, BTORDER_PROC as uint32);

        /*
         * Key arguments built from truncated attributes (or when caller
         * provides no tuple) are defensively represented as NULL values. They
         * should never be used.
         */
        if i < tupnatts {
            arg = index_getattr(itup, i + 1, itupdesc, &mut null);
        } else {
            arg = 0;
            null = true;
        }
        flags = (if null { SK_ISNULL as c_int } else { 0 })
            | ((*indoption.add(i as usize) as c_int) << SK_BT_INDOPTION_SHIFT);
        ScanKeyEntryInitializeWithInfo(
            skey.add(i as usize),
            flags,
            (i + 1) as AttrNumber,
            InvalidStrategy,
            0, /* InvalidOid */
            *(*rel).rd_indcollation.add(i as usize),
            procinfo,
            arg,
        );
        /* Record if any key attribute is NULL (or truncated) */
        if null {
            (*key).anynullkeys = true;
        }
        i += 1;
    }

    /*
     * In NULLS NOT DISTINCT mode, we pretend that there are no null keys, so
     * that full uniqueness check is done.
     */
    if (*(*rel).rd_index).indnullsnotdistinct {
        (*key).anynullkeys = false;
    }

    key
}

/*
 * free a retracement stack made by _bt_search.
 */
pub unsafe fn _bt_freestack(mut stack: BTStack) {
    while !stack.is_null() {
        let ostack: BTStack = stack;
        stack = (*stack).bts_parent;
        pfree(ostack as *mut c_void);
    }
}

/*
 * _bt_compare_array_skey() -- apply array comparison function
 *
 * Compares caller's tuple attribute value to a scan key/array element.
 * Helper function used during binary searches of SK_SEARCHARRAY arrays.
 *
 *		This routine returns:
 *			<0 if tupdatum < arrdatum;
 *			 0 if tupdatum == arrdatum;
 *			>0 if tupdatum > arrdatum.
 */
#[inline]
pub unsafe fn _bt_compare_array_skey(
    orderproc: *mut FmgrInfo,
    tupdatum: Datum,
    tupnull: bool,
    arrdatum: Datum,
    cur: ScanKey,
) -> int32 {
    let mut result: int32 = 0;

    Assert!((*cur).sk_strategy == BTEqualStrategyNumber);
    Assert!(!(((*cur).sk_flags as u32) & (SK_BT_MINVAL | SK_BT_MAXVAL) != 0));

    if tupnull {
        /* NULL tupdatum */
        if ((*cur).sk_flags as u32) & SK_ISNULL as u32 != 0 {
            result = 0; /* NULL "=" NULL */
        } else if ((*cur).sk_flags as u32) & SK_BT_NULLS_FIRST != 0 {
            result = -1; /* NULL "<" NOT_NULL */
        } else {
            result = 1; /* NULL ">" NOT_NULL */
        }
    } else if ((*cur).sk_flags as u32) & SK_ISNULL as u32 != 0 {
        /* NOT_NULL tupdatum, NULL arrdatum */
        if ((*cur).sk_flags as u32) & SK_BT_NULLS_FIRST != 0 {
            result = 1; /* NOT_NULL ">" NULL */
        } else {
            result = -1; /* NOT_NULL "<" NULL */
        }
    } else {
        /*
         * Like _bt_compare, we need to be careful of cross-type comparisons,
         * so the left value has to be the value that came from an index tuple
         */
        result = DatumGetInt32(FunctionCall2Coll(orderproc, (*cur).sk_collation, tupdatum, arrdatum));

        /*
         * We flip the sign by following the obvious rule: flip whenever the
         * column is a DESC column.
         *
         * _bt_compare does it the wrong way around (flip when *ASC*) in order
         * to compensate for passing its orderproc arguments backwards.  We
         * don't need to play these games because we find it natural to pass
         * tupdatum as the left value (and arrdatum as the right value).
         */
        if ((*cur).sk_flags as u32) & SK_BT_DESC != 0 {
            INVERT_COMPARE_RESULT(&mut result);
        }
    }

    result
}

/*
 * _bt_binsrch_array_skey() -- Binary search for next matching array key
 *
 * Returns an index to the first array element >= caller's tupdatum argument.
 */
pub unsafe fn _bt_binsrch_array_skey(
    orderproc: *mut FmgrInfo,
    cur_elem_trig: bool,
    dir: ScanDirection,
    tupdatum: Datum,
    tupnull: bool,
    array: *mut BTArrayKeyInfo,
    cur: ScanKey,
    set_elem_result: *mut int32,
) -> c_int {
    let mut low_elem: c_int = 0;
    let mut mid_elem: c_int = -1;
    let mut high_elem: c_int = (*array).num_elems - 1;
    let mut result: int32 = 0;
    let mut arrdatum: Datum;

    Assert!(((*cur).sk_flags as u32) & SK_SEARCHARRAY as u32 != 0);
    Assert!(!(((*cur).sk_flags as u32) & SK_BT_SKIP != 0));
    Assert!(!(((*cur).sk_flags as u32) & SK_ISNULL as u32 != 0)); /* SAOP arrays never have NULLs */
    Assert!((*cur).sk_strategy == BTEqualStrategyNumber);

    if cur_elem_trig {
        Assert!(!ScanDirectionIsNoMovement(dir));
        Assert!(((*cur).sk_flags as u32) & SK_BT_REQFWD != 0);

        /*
         * When the scan key that triggered array advancement is a required
         * array scan key, it is now certain that the current array element
         * (plus all prior elements relative to the current scan direction)
         * cannot possibly be at or ahead of the corresponding tuple value.
         */
        if ScanDirectionIsForward(dir) {
            low_elem = (*array).cur_elem + 1; /* old cur_elem exhausted */

            /* Compare prospective new cur_elem (also the new lower bound) */
            if high_elem >= low_elem {
                arrdatum = *(*array).elem_values.add(low_elem as usize);
                result = _bt_compare_array_skey(orderproc, tupdatum, tupnull, arrdatum, cur);

                if result <= 0 {
                    /* Optimistic comparison optimization worked out */
                    *set_elem_result = result;
                    return low_elem;
                }
                mid_elem = low_elem;
                low_elem += 1; /* this cur_elem exhausted, too */
            }

            if high_elem < low_elem {
                /* Caller needs to perform "beyond end" array advancement */
                *set_elem_result = 1;
                return high_elem;
            }
        } else {
            high_elem = (*array).cur_elem - 1; /* old cur_elem exhausted */

            /* Compare prospective new cur_elem (also the new upper bound) */
            if high_elem >= low_elem {
                arrdatum = *(*array).elem_values.add(high_elem as usize);
                result = _bt_compare_array_skey(orderproc, tupdatum, tupnull, arrdatum, cur);

                if result >= 0 {
                    /* Optimistic comparison optimization worked out */
                    *set_elem_result = result;
                    return high_elem;
                }
                mid_elem = high_elem;
                high_elem -= 1; /* this cur_elem exhausted, too */
            }

            if high_elem < low_elem {
                /* Caller needs to perform "beyond end" array advancement */
                *set_elem_result = -1;
                return low_elem;
            }
        }
    }

    while high_elem > low_elem {
        mid_elem = low_elem + ((high_elem - low_elem) / 2);
        arrdatum = *(*array).elem_values.add(mid_elem as usize);

        result = _bt_compare_array_skey(orderproc, tupdatum, tupnull, arrdatum, cur);

        if result == 0 {
            /*
             * It's safe to quit as soon as we see an equal array element.
             * This often saves an extra comparison or two...
             */
            low_elem = mid_elem;
            break;
        }

        if result > 0 {
            low_elem = mid_elem + 1;
        } else {
            high_elem = mid_elem;
        }
    }

    /*
     * ...but our caller also cares about how its searched-for tuple datum
     * compares to the low_elem datum.  Must always set *set_elem_result with
     * the result of that comparison specifically.
     */
    if low_elem != mid_elem {
        result = _bt_compare_array_skey(
            orderproc,
            tupdatum,
            tupnull,
            *(*array).elem_values.add(low_elem as usize),
            cur,
        );
    }

    *set_elem_result = result;

    low_elem
}

/*
 * _bt_binsrch_skiparray_skey() -- "Binary search" within a skip array
 *
 * Does not return an index into the array, since skip arrays don't really
 * contain elements (they generate their array elements procedurally instead).
 */
pub unsafe fn _bt_binsrch_skiparray_skey(
    cur_elem_trig: bool,
    dir: ScanDirection,
    tupdatum: Datum,
    tupnull: bool,
    array: *mut BTArrayKeyInfo,
    cur: ScanKey,
    set_elem_result: *mut int32,
) {
    Assert!(((*cur).sk_flags as u32) & SK_BT_SKIP != 0);
    Assert!(((*cur).sk_flags as u32) & SK_SEARCHARRAY as u32 != 0);
    Assert!(((*cur).sk_flags as u32) & SK_BT_REQFWD != 0);
    Assert!((*array).num_elems == -1);
    Assert!(!ScanDirectionIsNoMovement(dir));

    if (*array).null_elem {
        Assert!((*array).low_compare.is_null() && (*array).high_compare.is_null());

        *set_elem_result = 0;
        return;
    }

    if tupnull {
        /* NULL tupdatum */
        if ((*cur).sk_flags as u32) & SK_BT_NULLS_FIRST != 0 {
            *set_elem_result = -1; /* NULL "<" NOT_NULL */
        } else {
            *set_elem_result = 1; /* NULL ">" NOT_NULL */
        }
        return;
    }

    /*
     * Array inequalities determine whether tupdatum is within the range of
     * caller's skip array
     */
    *set_elem_result = 0;
    if ScanDirectionIsForward(dir) {
        /*
         * Evaluate low_compare first (unless cur_elem_trig tells us that it
         * cannot possibly fail to be satisfied), then evaluate high_compare
         */
        if !cur_elem_trig
            && !(*array).low_compare.is_null()
            && !DatumGetBool(FunctionCall2Coll(
                &mut (*(*array).low_compare).sk_func,
                (*(*array).low_compare).sk_collation,
                tupdatum,
                (*(*array).low_compare).sk_argument,
            ))
        {
            *set_elem_result = -1;
        } else if !(*array).high_compare.is_null()
            && !DatumGetBool(FunctionCall2Coll(
                &mut (*(*array).high_compare).sk_func,
                (*(*array).high_compare).sk_collation,
                tupdatum,
                (*(*array).high_compare).sk_argument,
            ))
        {
            *set_elem_result = 1;
        }
    } else {
        /*
         * Evaluate high_compare first (unless cur_elem_trig tells us that it
         * cannot possibly fail to be satisfied), then evaluate low_compare
         */
        if !cur_elem_trig
            && !(*array).high_compare.is_null()
            && !DatumGetBool(FunctionCall2Coll(
                &mut (*(*array).high_compare).sk_func,
                (*(*array).high_compare).sk_collation,
                tupdatum,
                (*(*array).high_compare).sk_argument,
            ))
        {
            *set_elem_result = 1;
        } else if !(*array).low_compare.is_null()
            && !DatumGetBool(FunctionCall2Coll(
                &mut (*(*array).low_compare).sk_func,
                (*(*array).low_compare).sk_collation,
                tupdatum,
                (*(*array).low_compare).sk_argument,
            ))
        {
            *set_elem_result = -1;
        }
    }

    /*
     * Assert that any keys that were assumed to be satisfied already (due to
     * caller passing cur_elem_trig=true) really are satisfied as expected
     */
    #[cfg(feature = "use_assert_checking")]
    {
        if cur_elem_trig {
            if ScanDirectionIsForward(dir) && !(*array).low_compare.is_null() {
                Assert!(DatumGetBool(FunctionCall2Coll(
                    &mut (*(*array).low_compare).sk_func,
                    (*(*array).low_compare).sk_collation,
                    tupdatum,
                    (*(*array).low_compare).sk_argument,
                )));
            }
            if ScanDirectionIsBackward(dir) && !(*array).high_compare.is_null() {
                Assert!(DatumGetBool(FunctionCall2Coll(
                    &mut (*(*array).high_compare).sk_func,
                    (*(*array).high_compare).sk_collation,
                    tupdatum,
                    (*(*array).high_compare).sk_argument,
                )));
            }
        }
    }
}

/*
 * _bt_skiparray_set_element() -- Set skip array scan key's sk_argument
 */
pub unsafe fn _bt_skiparray_set_element(
    rel: Relation,
    skey: ScanKey,
    array: *mut BTArrayKeyInfo,
    set_elem_result: int32,
    tupdatum: Datum,
    tupnull: bool,
) {
    Assert!(((*skey).sk_flags as u32) & SK_BT_SKIP != 0);
    Assert!(((*skey).sk_flags as u32) & SK_SEARCHARRAY as u32 != 0);

    if set_elem_result != 0 {
        /* tupdatum/tupnull is out of the range of the skip array */
        Assert!(!(*array).null_elem);

        _bt_array_set_low_or_high(rel, skey, array, set_elem_result < 0);
        return;
    }

    /* Advance skip array to tupdatum (or tupnull) value */
    if unlikely(tupnull) {
        _bt_skiparray_set_isnull(rel, skey, array);
        return;
    }

    /* Free memory previously allocated for sk_argument if needed */
    if !(*array).attbyval && (*skey).sk_argument != 0 {
        pfree(DatumGetPointer((*skey).sk_argument) as *mut c_void);
    }

    /* tupdatum becomes new sk_argument/new current element */
    (*skey).sk_flags &= !((SK_SEARCHNULL as u32 | SK_ISNULL as u32
        | SK_BT_MINVAL | SK_BT_MAXVAL | SK_BT_NEXT | SK_BT_PRIOR) as c_int);
    (*skey).sk_argument = datumCopy(tupdatum, (*array).attbyval, (*array).attlen);
}

/*
 * _bt_skiparray_set_isnull() -- set skip array scan key to NULL
 */
pub unsafe fn _bt_skiparray_set_isnull(
    rel: Relation,
    skey: ScanKey,
    array: *mut BTArrayKeyInfo,
) {
    Assert!(((*skey).sk_flags as u32) & SK_BT_SKIP != 0);
    Assert!(((*skey).sk_flags as u32) & SK_SEARCHARRAY as u32 != 0);
    Assert!((*array).null_elem && (*array).low_compare.is_null() && (*array).high_compare.is_null());

    /* Free memory previously allocated for sk_argument if needed */
    if !(*array).attbyval && (*skey).sk_argument != 0 {
        pfree(DatumGetPointer((*skey).sk_argument) as *mut c_void);
    }

    /* NULL becomes new sk_argument/new current element */
    (*skey).sk_argument = 0;
    (*skey).sk_flags &= !((SK_BT_MINVAL | SK_BT_MAXVAL | SK_BT_NEXT | SK_BT_PRIOR) as c_int);
    (*skey).sk_flags |= (SK_SEARCHNULL as u32 | SK_ISNULL as u32) as c_int;
}

/*
 * _bt_start_array_keys() -- Initialize array keys at start of a scan
 */
pub unsafe fn _bt_start_array_keys(scan: IndexScanDesc, dir: ScanDirection) {
    let rel: Relation = (*scan).indexRelation;
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;

    Assert!((*so).numArrayKeys != 0);
    Assert!((*so).qual_ok);

    let mut i: c_int = 0;
    while i < (*so).numArrayKeys {
        let array: *mut BTArrayKeyInfo = (*so).arrayKeys.add(i as usize);
        let skey: ScanKey = (*so).keyData.add((*array).scan_key as usize);

        Assert!(((*skey).sk_flags as u32) & SK_SEARCHARRAY as u32 != 0);

        _bt_array_set_low_or_high(rel, skey, array, ScanDirectionIsForward(dir));
        i += 1;
    }
    (*so).scanBehind = false; /* reset */
    (*so).oppositeDirCheck = false; /* reset */
}

/*
 * _bt_array_set_low_or_high() -- Set array scan key to lowest/highest element
 */
pub unsafe fn _bt_array_set_low_or_high(
    rel: Relation,
    skey: ScanKey,
    array: *mut BTArrayKeyInfo,
    low_not_high: bool,
) {
    Assert!(((*skey).sk_flags as u32) & SK_SEARCHARRAY as u32 != 0);

    if (*array).num_elems != -1 {
        /* set low or high element for SAOP array */
        let mut set_elem: c_int = 0;

        Assert!(!(((*skey).sk_flags as u32) & SK_BT_SKIP != 0));

        if !low_not_high {
            set_elem = (*array).num_elems - 1;
        }

        /*
         * Just copy over array datum (only skip arrays require freeing and
         * allocating memory for sk_argument)
         */
        (*array).cur_elem = set_elem;
        (*skey).sk_argument = *(*array).elem_values.add(set_elem as usize);

        return;
    }

    /* set low or high element for skip array */
    Assert!(((*skey).sk_flags as u32) & SK_BT_SKIP != 0);
    Assert!((*array).num_elems == -1);

    /* Free memory previously allocated for sk_argument if needed */
    if !(*array).attbyval && (*skey).sk_argument != 0 {
        pfree(DatumGetPointer((*skey).sk_argument) as *mut c_void);
    }

    /* Reset flags */
    (*skey).sk_argument = 0;
    (*skey).sk_flags &= !((SK_SEARCHNULL as u32 | SK_ISNULL as u32
        | SK_BT_MINVAL | SK_BT_MAXVAL | SK_BT_NEXT | SK_BT_PRIOR) as c_int);

    if (*array).null_elem
        && (low_not_high == (((*skey).sk_flags as u32) & SK_BT_NULLS_FIRST != 0))
    {
        /* Requested element (either lowest or highest) has the value NULL */
        (*skey).sk_flags |= (SK_SEARCHNULL as u32 | SK_ISNULL as u32) as c_int;
    } else if low_not_high {
        /* Setting array to lowest element (according to low_compare) */
        (*skey).sk_flags |= SK_BT_MINVAL as c_int;
    } else {
        /* Setting array to highest element (according to high_compare) */
        (*skey).sk_flags |= SK_BT_MAXVAL as c_int;
    }
}

// End of Part 2.

// ===========================================================================
// Part 3 -- _bt_array_decrement, _bt_array_increment,
//            _bt_advance_array_keys_increment, _bt_tuple_before_array_skeys,
//            _bt_start_prim_scan
// ===========================================================================

/*
 * _bt_array_decrement() -- decrement array scan key's sk_argument
 */
pub unsafe fn _bt_array_decrement(
    rel: Relation,
    skey: ScanKey,
    array: *mut BTArrayKeyInfo,
) -> bool {
    let mut uflow: bool = false;
    let mut dec_sk_argument: Datum;

    Assert!(((*skey).sk_flags as u32) & SK_SEARCHARRAY as u32 != 0);
    Assert!(!(((*skey).sk_flags as u32) & (SK_BT_MAXVAL | SK_BT_NEXT | SK_BT_PRIOR) != 0));

    /* SAOP array? */
    if (*array).num_elems != -1 {
        Assert!(!(((*skey).sk_flags as u32) & (SK_BT_SKIP | SK_BT_MINVAL | SK_BT_MAXVAL) != 0));
        if (*array).cur_elem > 0 {
            /*
             * Just decrement current element, and assign its datum to skey
             * (only skip arrays need us to free existing sk_argument memory)
             */
            (*array).cur_elem -= 1;
            (*skey).sk_argument = *(*array).elem_values.add((*array).cur_elem as usize);

            /* Successfully decremented array */
            return true;
        }

        /* Cannot decrement to before first array element */
        return false;
    }

    /* Nope, this is a skip array */
    Assert!(((*skey).sk_flags as u32) & SK_BT_SKIP != 0);

    /*
     * The sentinel value that represents the minimum value within the range
     * of a skip array (often just -inf) is never decrementable
     */
    if ((*skey).sk_flags as u32) & SK_BT_MINVAL != 0 {
        return false;
    }

    /*
     * When the current array element is NULL, and the lowest sorting value in
     * the index is also NULL, we cannot decrement before first array element
     */
    if ((*skey).sk_flags as u32) & SK_ISNULL as u32 != 0
        && ((*skey).sk_flags as u32) & SK_BT_NULLS_FIRST != 0
    {
        return false;
    }

    /*
     * Opclasses without skip support "decrement" the scan key's current
     * element by setting the PRIOR flag.
     */
    if (*array).sksup.is_null() {
        /* Successfully "decremented" array */
        (*skey).sk_flags |= SK_BT_PRIOR as c_int;
        return true;
    }

    /*
     * Opclasses with skip support directly decrement sk_argument
     */
    if ((*skey).sk_flags as u32) & SK_ISNULL as u32 != 0 {
        Assert!(!(((*skey).sk_flags as u32) & SK_BT_NULLS_FIRST != 0));

        /*
         * Existing sk_argument/array element is NULL (for an IS NULL qual).
         * "Decrement" from NULL to the high_elem value provided by opclass
         * skip support routine.
         */
        (*skey).sk_flags &= !((SK_SEARCHNULL as u32 | SK_ISNULL as u32) as c_int);
        (*skey).sk_argument =
            datumCopy((*(*array).sksup).high_elem, (*array).attbyval, (*array).attlen);
        return true;
    }

    /*
     * Ask opclass support routine to provide decremented copy of existing
     * non-NULL sk_argument
     */
    dec_sk_argument = ((*(*array).sksup).decrement)(rel, (*skey).sk_argument, &mut uflow);
    if unlikely(uflow) {
        /* dec_sk_argument has undefined value (so no pfree) */
        if (*array).null_elem && ((*skey).sk_flags as u32) & SK_BT_NULLS_FIRST != 0 {
            _bt_skiparray_set_isnull(rel, skey, array);

            /* Successfully "decremented" array to NULL */
            return true;
        }

        /* Cannot decrement to before first array element */
        return false;
    }

    /*
     * Successfully decremented sk_argument to a non-NULL value.  Make sure
     * that the decremented value is still within the range of the array.
     */
    if !(*array).low_compare.is_null()
        && !DatumGetBool(FunctionCall2Coll(
            &mut (*(*array).low_compare).sk_func,
            (*(*array).low_compare).sk_collation,
            dec_sk_argument,
            (*(*array).low_compare).sk_argument,
        ))
    {
        /* Keep existing sk_argument after all */
        if !(*array).attbyval {
            pfree(DatumGetPointer(dec_sk_argument) as *mut c_void);
        }

        /* Cannot decrement to before first array element */
        return false;
    }

    /* Accept value returned by opclass decrement callback */
    if !(*array).attbyval && (*skey).sk_argument != 0 {
        pfree(DatumGetPointer((*skey).sk_argument) as *mut c_void);
    }
    (*skey).sk_argument = dec_sk_argument;

    /* Successfully decremented array */
    true
}

/*
 * _bt_array_increment() -- increment array scan key's sk_argument
 */
pub unsafe fn _bt_array_increment(
    rel: Relation,
    skey: ScanKey,
    array: *mut BTArrayKeyInfo,
) -> bool {
    let mut oflow: bool = false;
    let mut inc_sk_argument: Datum;

    Assert!(((*skey).sk_flags as u32) & SK_SEARCHARRAY as u32 != 0);
    Assert!(!(((*skey).sk_flags as u32) & (SK_BT_MINVAL | SK_BT_NEXT | SK_BT_PRIOR) != 0));

    /* SAOP array? */
    if (*array).num_elems != -1 {
        Assert!(!(((*skey).sk_flags as u32) & (SK_BT_SKIP | SK_BT_MINVAL | SK_BT_MAXVAL) != 0));
        if (*array).cur_elem < (*array).num_elems - 1 {
            /*
             * Just increment current element, and assign its datum to skey
             * (only skip arrays need us to free existing sk_argument memory)
             */
            (*array).cur_elem += 1;
            (*skey).sk_argument = *(*array).elem_values.add((*array).cur_elem as usize);

            /* Successfully incremented array */
            return true;
        }

        /* Cannot increment past final array element */
        return false;
    }

    /* Nope, this is a skip array */
    Assert!(((*skey).sk_flags as u32) & SK_BT_SKIP != 0);

    /*
     * The sentinel value that represents the maximum value within the range
     * of a skip array (often just +inf) is never incrementable
     */
    if ((*skey).sk_flags as u32) & SK_BT_MAXVAL != 0 {
        return false;
    }

    /*
     * When the current array element is NULL, and the highest sorting value
     * in the index is also NULL, we cannot increment past the final element
     */
    if ((*skey).sk_flags as u32) & SK_ISNULL as u32 != 0
        && !(((*skey).sk_flags as u32) & SK_BT_NULLS_FIRST != 0)
    {
        return false;
    }

    /*
     * Opclasses without skip support "increment" the scan key's current
     * element by setting the NEXT flag.
     */
    if (*array).sksup.is_null() {
        /* Successfully "incremented" array */
        (*skey).sk_flags |= SK_BT_NEXT as c_int;
        return true;
    }

    /*
     * Opclasses with skip support directly increment sk_argument
     */
    if ((*skey).sk_flags as u32) & SK_ISNULL as u32 != 0 {
        Assert!(((*skey).sk_flags as u32) & SK_BT_NULLS_FIRST != 0);

        /*
         * Existing sk_argument/array element is NULL (for an IS NULL qual).
         * "Increment" from NULL to the low_elem value provided by opclass
         * skip support routine.
         */
        (*skey).sk_flags &= !((SK_SEARCHNULL as u32 | SK_ISNULL as u32) as c_int);
        (*skey).sk_argument =
            datumCopy((*(*array).sksup).low_elem, (*array).attbyval, (*array).attlen);
        return true;
    }

    /*
     * Ask opclass support routine to provide incremented copy of existing
     * non-NULL sk_argument
     */
    inc_sk_argument = ((*(*array).sksup).increment)(rel, (*skey).sk_argument, &mut oflow);
    if unlikely(oflow) {
        /* inc_sk_argument has undefined value (so no pfree) */
        if (*array).null_elem && !(((*skey).sk_flags as u32) & SK_BT_NULLS_FIRST != 0) {
            _bt_skiparray_set_isnull(rel, skey, array);

            /* Successfully "incremented" array to NULL */
            return true;
        }

        /* Cannot increment past final array element */
        return false;
    }

    /*
     * Successfully incremented sk_argument to a non-NULL value.  Make sure
     * that the incremented value is still within the range of the array.
     */
    if !(*array).high_compare.is_null()
        && !DatumGetBool(FunctionCall2Coll(
            &mut (*(*array).high_compare).sk_func,
            (*(*array).high_compare).sk_collation,
            inc_sk_argument,
            (*(*array).high_compare).sk_argument,
        ))
    {
        /* Keep existing sk_argument after all */
        if !(*array).attbyval {
            pfree(DatumGetPointer(inc_sk_argument) as *mut c_void);
        }

        /* Cannot increment past final array element */
        return false;
    }

    /* Accept value returned by opclass increment callback */
    if !(*array).attbyval && (*skey).sk_argument != 0 {
        pfree(DatumGetPointer((*skey).sk_argument) as *mut c_void);
    }
    (*skey).sk_argument = inc_sk_argument;

    /* Successfully incremented array */
    true
}

/*
 * _bt_advance_array_keys_increment() -- Advance to next set of array elements
 *
 * Advances the array keys by a single increment in the current scan direction.
 */
pub unsafe fn _bt_advance_array_keys_increment(
    scan: IndexScanDesc,
    dir: ScanDirection,
    skip_array_set: *mut bool,
) -> bool {
    let rel: Relation = (*scan).indexRelation;
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;

    /*
     * We must advance the last array key most quickly, since it will
     * correspond to the lowest-order index column among the available
     * qualifications
     */
    let mut i: c_int = (*so).numArrayKeys - 1;
    while i >= 0 {
        let array: *mut BTArrayKeyInfo = (*so).arrayKeys.add(i as usize);
        let skey: ScanKey = (*so).keyData.add((*array).scan_key as usize);

        if (*array).num_elems == -1 {
            *skip_array_set = true;
        }

        if ScanDirectionIsForward(dir) {
            if _bt_array_increment(rel, skey, array) {
                return true;
            }
        } else {
            if _bt_array_decrement(rel, skey, array) {
                return true;
            }
        }

        /*
         * Couldn't increment (or decrement) array.  Handle array roll over.
         * Start over at the array's lowest sorting value (or its highest
         * value, for backward scans)...
         */
        _bt_array_set_low_or_high(rel, skey, array, ScanDirectionIsForward(dir));

        /* ...then increment (or decrement) next most significant array */
        i -= 1;
    }

    /*
     * The array keys are now exhausted.
     *
     * Restore the array keys to the state they were in immediately before we
     * were called.  This ensures that the arrays only ever ratchet in the
     * current scan direction.
     */
    _bt_start_array_keys(scan, -dir);

    false
}

/*
 * _bt_tuple_before_array_skeys() -- too early to advance required arrays?
 */
pub unsafe fn _bt_tuple_before_array_skeys(
    scan: IndexScanDesc,
    dir: ScanDirection,
    tuple: IndexTuple,
    tupdesc: TupleDesc,
    tupnatts: c_int,
    readpagetup: bool,
    sktrig: c_int,
    scanBehind: *mut bool,
) -> bool {
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;

    Assert!((*so).numArrayKeys != 0);
    Assert!((*so).numberOfKeys != 0);
    Assert!(sktrig == 0 || readpagetup);
    Assert!(!readpagetup || scanBehind.is_null());

    if !scanBehind.is_null() {
        *scanBehind = false;
    }

    let mut ikey: c_int = sktrig;
    while ikey < (*so).numberOfKeys {
        let cur: ScanKey = (*so).keyData.add(ikey as usize);
        let mut tupdatum: Datum = 0;
        let mut tupnull: bool = false;
        let mut result: int32 = 0;

        /* readpagetup calls require one ORDER proc comparison (at most) */
        Assert!(!readpagetup || ikey == sktrig);

        /*
         * Once we reach a non-required scan key, we're completely done.
         */
        if ((*cur).sk_flags as u32) & (SK_BT_REQFWD | SK_BT_REQBKWD) == 0 {
            Assert!(!readpagetup);
            Assert!(ikey > sktrig || ikey == 0);
            return false;
        }

        if (*cur).sk_attno > tupnatts as AttrNumber {
            Assert!(!readpagetup);

            /*
             * When we reach a high key's truncated attribute, assume that the
             * tuple attribute's value is >= the scan's equality constraint
             * scan keys.
             */
            if !scanBehind.is_null() {
                *scanBehind = true;
            }

            return false;
        }

        /*
         * Deal with inequality strategy scan keys that _bt_check_compare set
         * continuescan=false for
         */
        if (*cur).sk_strategy != BTEqualStrategyNumber {
            /*
             * When _bt_check_compare indicated that a required inequality
             * scan key wasn't satisfied, there's no need to verify anything.
             */
            if readpagetup {
                return false;
            }

            /*
             * Otherwise we can't give up, since we must check all required
             * scan keys in order to correctly track *scanBehind for caller
             */
            ikey += 1;
            continue;
        }

        tupdatum = index_getattr(tuple, (*cur).sk_attno as c_int, tupdesc, &mut tupnull);

        if likely(!(((*cur).sk_flags as u32) & (SK_BT_MINVAL | SK_BT_MAXVAL) != 0)) {
            /* Scankey has a valid/comparable sk_argument value */
            result = _bt_compare_array_skey(
                (*so).orderProcs.add(ikey as usize),
                tupdatum,
                tupnull,
                (*cur).sk_argument,
                cur,
            );

            if result == 0 {
                /*
                 * Interpret result in a way that takes NEXT/PRIOR into account
                 */
                if ((*cur).sk_flags as u32) & SK_BT_NEXT != 0 {
                    result = -1;
                } else if ((*cur).sk_flags as u32) & SK_BT_PRIOR != 0 {
                    result = 1;
                }

                Assert!(result == 0 || ((*cur).sk_flags as u32) & SK_BT_SKIP != 0);
            }
        } else {
            let mut array: *mut BTArrayKeyInfo = core::ptr::null_mut();

            /*
             * Current array element/array = scan key value is a sentinel
             * value that represents the lowest (or highest) possible value
             * that's still within the range of the array.
             */
            Assert!(if ScanDirectionIsForward(dir) {
                !(((*cur).sk_flags as u32) & SK_BT_MAXVAL != 0)
            } else {
                !(((*cur).sk_flags as u32) & SK_BT_MINVAL != 0)
            });

            /*
             * There are no valid sk_argument values in MINVAL/MAXVAL keys.
             * Check if tupdatum is within the range of skip array instead.
             */
            let mut arrayidx: c_int = 0;
            while arrayidx < (*so).numArrayKeys {
                array = (*so).arrayKeys.add(arrayidx as usize);
                if (*array).scan_key == ikey {
                    break;
                }
                arrayidx += 1;
            }

            _bt_binsrch_skiparray_skey(false, dir, tupdatum, tupnull, array, cur, &mut result);

            if result == 0 {
                /*
                 * tupdatum satisfies both low_compare and high_compare, so
                 * it's time to advance the array keys.
                 */
                return false;
            }
        }

        /*
         * Does this comparison indicate that caller must _not_ advance the
         * scan's arrays just yet?
         */
        if (ScanDirectionIsForward(dir) && result < 0)
            || (ScanDirectionIsBackward(dir) && result > 0)
        {
            return true;
        }

        /*
         * Does this comparison indicate that caller should now advance the
         * scan's arrays?
         */
        if readpagetup || result != 0 {
            Assert!(result != 0);
            return false;
        }

        /*
         * Inconclusive -- need to check later scan keys, too.
         * This must be a finaltup precheck, or a call made from an assertion.
         */
        Assert!(result == 0);
        ikey += 1;
    }

    Assert!(!readpagetup);

    false
}

/*
 * _bt_start_prim_scan() -- start scheduled primitive index scan?
 *
 * Returns true if _bt_checkkeys scheduled another primitive index scan, just
 * as the last one ended.  Otherwise returns false.
 */
pub unsafe fn _bt_start_prim_scan(scan: IndexScanDesc, dir: ScanDirection) -> bool {
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;

    Assert!((*so).numArrayKeys != 0);

    (*so).scanBehind = false; /* reset */
    (*so).oppositeDirCheck = false; /* reset */

    /*
     * Array keys are advanced within _bt_checkkeys when the scan reaches the
     * leaf level.
     *
     * _bt_checkkeys sets a simple flag variable to schedule another primitive
     * index scan.  The flag tells us what to do.
     */
    if (*so).needPrimScan {
        /*
         * Flag was set -- must call _bt_first again, which will reset the
         * scan's needPrimScan flag
         */
        return true;
    }

    /* The top-level index scan ran out of tuples in this scan direction */
    if !(*scan).parallel_scan.is_null() {
        _bt_parallel_done(scan);
    }

    false
}

// End of Part 3.

// ===========================================================================
// Part 4 -- _bt_advance_array_keys, _bt_verify_keys_with_arraykeys,
//            _bt_checkkeys, _bt_scanbehind_checkkeys, _bt_oppodir_checkkeys,
//            _bt_set_startikey
// ===========================================================================

/*
 * _bt_advance_array_keys() -- Advance array elements using a tuple
 *
 * TODO(pg-port): skip-scan machinery deps (_bt_parallel_primscan_schedule,
 *   BTReadPageState.nskipadvances, .firstpage, .skip) rely on structs not yet
 *   fully ported; body is a faithful translation but stubs will panic.
 */
pub unsafe fn _bt_advance_array_keys(
    scan: IndexScanDesc,
    pstate: *mut BTReadPageState,
    tuple: IndexTuple,
    tupnatts: c_int,
    tupdesc: TupleDesc,
    sktrig: c_int,
    sktrig_required: bool,
) -> bool {
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;
    let rel: Relation = (*scan).indexRelation;
    let dir: ScanDirection = (*so).currPos.dir;
    let mut arrayidx: c_int = 0;
    let mut beyond_end_advance: bool = false;
    let mut skip_array_advanced: bool = false;
    let mut has_required_opposite_direction_only: bool = false;
    let mut all_required_satisfied: bool = true;
    let mut all_satisfied: bool = true;

    Assert!(!(*so).needPrimScan && !(*so).scanBehind && !(*so).oppositeDirCheck);
    Assert!(_bt_verify_keys_with_arraykeys(scan));

    if sktrig_required {
        /*
         * Precondition array state assertion
         */
        Assert!(!_bt_tuple_before_array_skeys(
            scan, dir, tuple, tupdesc, tupnatts, false, 0, core::ptr::null_mut(),
        ));

        /*
         * Once we return we'll have a new set of required array keys, so
         * reset state used by "look ahead" optimization
         */
        (*pstate).rechecks = 0;
        (*pstate).targetdistance = 0;
    } else if sktrig < (*so).numberOfKeys - 1
        && !(((*(*so).keyData.add(((*so).numberOfKeys - 1) as usize)).sk_flags as u32)
            & SK_SEARCHARRAY as u32
            != 0)
    {
        let least_sign_ikey: c_int = (*so).numberOfKeys - 1;
        let mut continuescan: bool = false;
        let mut lsi: c_int = least_sign_ikey;

        /*
         * Optimization: perform a precheck of the least significant key
         * during !sktrig_required calls when it isn't already our sktrig.
         */
        Assert!(
            ((*(*so).keyData.add(sktrig as usize)).sk_flags as u32) & SK_SEARCHARRAY as u32 != 0,
        );
        if !_bt_check_compare(
            scan,
            dir,
            tuple,
            tupnatts,
            tupdesc,
            false,
            false,
            &mut continuescan,
            &mut lsi,
        ) {
            return false;
        }
    }

    let mut ikey: c_int = 0;
    while ikey < (*so).numberOfKeys {
        let cur: ScanKey = (*so).keyData.add(ikey as usize);
        let mut array: *mut BTArrayKeyInfo = core::ptr::null_mut();
        let mut tupdatum: Datum = 0;
        let mut required: bool = false;
        let mut tupnull: bool = false;
        let mut result: int32 = 0;
        let mut set_elem: c_int = 0;

        if (*cur).sk_strategy == BTEqualStrategyNumber {
            /* Manage array state */
            if ((*cur).sk_flags as u32) & SK_SEARCHARRAY as u32 != 0 {
                array = (*so).arrayKeys.add(arrayidx as usize);
                Assert!((*array).scan_key == ikey);
                arrayidx += 1;
            }
        } else {
            /*
             * Are any inequalities required in the opposite direction only
             * present here?
             */
            if (ScanDirectionIsForward(dir)
                && ((*cur).sk_flags as u32) & SK_BT_REQBKWD != 0)
                || (ScanDirectionIsBackward(dir)
                    && ((*cur).sk_flags as u32) & SK_BT_REQFWD != 0)
            {
                has_required_opposite_direction_only = true;
            }
        }

        /* Optimization: skip over known-satisfied scan keys */
        if ikey < sktrig {
            ikey += 1;
            continue;
        }

        if ((*cur).sk_flags as u32) & (SK_BT_REQFWD | SK_BT_REQBKWD) != 0 {
            required = true;

            if (*cur).sk_attno > tupnatts as AttrNumber {
                /* Set this just like _bt_tuple_before_array_skeys */
                Assert!(sktrig < ikey);
                (*so).scanBehind = true;
            }
        }

        /*
         * Handle a required non-array scan key that the initial call to
         * _bt_check_compare indicated triggered array advancement, if any.
         */
        if ikey == sktrig && array.is_null() {
            Assert!(sktrig_required && required && all_required_satisfied);

            /* Use "beyond end" advancement.  See below for an explanation. */
            beyond_end_advance = true;
            all_satisfied = false;
            all_required_satisfied = false;

            ikey += 1;
            continue;
        }
        /*
         * Nothing more for us to do with an inequality strategy scan key that
         * wasn't the one that _bt_check_compare stopped on, though.
         */
        else if (*cur).sk_strategy != BTEqualStrategyNumber {
            ikey += 1;
            continue;
        }
        /*
         * Nothing for us to do with an equality strategy scan key that isn't
         * marked required, either -- unless it's a non-required array
         */
        else if !required && array.is_null() {
            ikey += 1;
            continue;
        }

        /*
         * Here we perform steps for all array scan keys after a required
         * array scan key whose binary search triggered "beyond end of array
         * element" array advancement.
         */
        if beyond_end_advance {
            if !array.is_null() {
                _bt_array_set_low_or_high(rel, cur, array, ScanDirectionIsBackward(dir));
            }

            ikey += 1;
            continue;
        }

        /*
         * Here we perform steps for all array scan keys after a required
         * array scan key whose tuple attribute was < the closest matching
         * array key when we dealt with it (or > for backwards scans).
         */
        if !all_required_satisfied || (*cur).sk_attno > tupnatts as AttrNumber {
            if !array.is_null() {
                _bt_array_set_low_or_high(rel, cur, array, ScanDirectionIsForward(dir));
            }

            ikey += 1;
            continue;
        }

        /*
         * Search in scankey's array for the corresponding tuple attribute
         * value from caller's tuple
         */
        tupdatum = index_getattr(tuple, (*cur).sk_attno as c_int, tupdesc, &mut tupnull);

        if !array.is_null() {
            let cur_elem_trig: bool = sktrig_required && ikey == sktrig;

            /*
             * "Binary search" by checking if tupdatum/tupnull are within the
             * range of the skip array
             */
            if (*array).num_elems == -1 {
                _bt_binsrch_skiparray_skey(
                    cur_elem_trig,
                    dir,
                    tupdatum,
                    tupnull,
                    array,
                    cur,
                    &mut result,
                );
            }
            /*
             * Binary search for the closest match from the SAOP array
             */
            else {
                set_elem = _bt_binsrch_array_skey(
                    (*so).orderProcs.add(ikey as usize),
                    cur_elem_trig,
                    dir,
                    tupdatum,
                    tupnull,
                    array,
                    cur,
                    &mut result,
                );
            }
        } else {
            Assert!(required);

            /*
             * This is a required non-array equality strategy scan key, which
             * we'll treat as a degenerate single element array.
             */
            result = _bt_compare_array_skey(
                (*so).orderProcs.add(ikey as usize),
                tupdatum,
                tupnull,
                (*cur).sk_argument,
                cur,
            );
        }

        /*
         * Consider "beyond end of array element" array advancement.
         */
        if sktrig_required
            && required
            && ((ScanDirectionIsForward(dir) && result > 0)
                || (ScanDirectionIsBackward(dir) && result < 0))
        {
            beyond_end_advance = true;
        }

        Assert!(all_required_satisfied && all_satisfied);
        if result != 0 {
            all_satisfied = false;
            if sktrig_required && required {
                all_required_satisfied = false;
            } else {
                /*
                 * There's no need to advance the arrays using the best
                 * available match for a non-required array.  Give up now.
                 */
                break;
            }
        }

        /* Advance array keys, even when we don't have an exact match */
        if !array.is_null() {
            if (*array).num_elems == -1 {
                /* Skip array's new element is tupdatum (or MINVAL/MAXVAL) */
                _bt_skiparray_set_element(rel, cur, array, result, tupdatum, tupnull);
                skip_array_advanced = true;
            } else if (*array).cur_elem != set_elem {
                /* SAOP array's new element is set_elem datum */
                (*array).cur_elem = set_elem;
                (*cur).sk_argument = *(*array).elem_values.add(set_elem as usize);
            }
        }

        ikey += 1;
    }

    /*
     * Advance the array keys incrementally whenever "beyond end of array
     * element" array advancement happens.
     */
    if beyond_end_advance
        && !_bt_advance_array_keys_increment(scan, dir, &mut skip_array_advanced)
    {
        // goto end_toplevel_scan
        (*pstate).continuescan = false;
        (*so).needPrimScan = false;
        return false;
    }

    Assert!(_bt_verify_keys_with_arraykeys(scan));

    /*
     * Maintain a page-level count of the number of times the scan's array
     * keys advanced in a way that affected at least one skip array
     */
    if sktrig_required && skip_array_advanced {
        (*pstate).nskipadvances += 1;
    }

    /*
     * Does tuple now satisfy our new qual?  Recheck with _bt_check_compare.
     */
    if (sktrig_required && all_required_satisfied) || (!sktrig_required && all_satisfied) {
        let mut nsktrig: c_int = sktrig + 1;
        let mut continuescan: bool = false;

        Assert!(all_required_satisfied);

        /* Recheck _bt_check_compare on behalf of caller */
        if _bt_check_compare(
            scan,
            dir,
            tuple,
            tupnatts,
            tupdesc,
            false,
            !sktrig_required,
            &mut continuescan,
            &mut nsktrig,
        ) && !(*so).scanBehind
        {
            /* This tuple satisfies the new qual */
            Assert!(all_satisfied && continuescan);

            if !pstate.is_null() {
                (*pstate).continuescan = true;
            }

            return true;
        }

        /*
         * Consider "second pass" handling of required inequalities.
         */
        if unlikely(!continuescan) {
            let satisfied: bool;

            Assert!(sktrig_required);
            Assert!((*(*so).keyData.add(nsktrig as usize)).sk_strategy != BTEqualStrategyNumber);

            /*
             * The tuple must use "beyond end" advancement during the
             * recursive call, so we cannot possibly end up back here when
             * recursing.
             */
            Assert!(!beyond_end_advance);

            /* Advance the array keys a second time using same tuple */
            satisfied = _bt_advance_array_keys(
                scan, pstate, tuple, tupnatts, tupdesc, nsktrig, true,
            );

            /* This tuple doesn't satisfy the inequality */
            Assert!(!satisfied);
            return false;
        }

        /*
         * Some non-required scan key (from new qual) still not satisfied.
         */
    }

    /*
     * When we were called just to deal with "advancing" non-required arrays,
     * this is as far as we can go.
     */
    if !sktrig_required {
        /* Caller's tuple doesn't match any qual */
        return false;
    }

    /*
     * Postcondition array state assertion.
     */
    Assert!(
        _bt_tuple_before_array_skeys(
            scan, dir, tuple, tupdesc, tupnatts, false, 0, core::ptr::null_mut(),
        ) == !all_required_satisfied,
    );

    /*
     * We generally permit primitive index scans to continue onto the next
     * sibling page when the page's finaltup satisfies all required scan keys.
     */
    if !all_required_satisfied && (*pstate).finaltup == tuple {
        // goto new_prim_scan
        // fall through to new_prim_scan block below
        return _bt_advance_array_keys_new_prim_scan(scan, pstate, so);
    }

    /*
     * Proactively check finaltup.
     */
    if !all_required_satisfied
        && !(*pstate).finaltup.is_null()
        && _bt_tuple_before_array_skeys(
            scan,
            dir,
            (*pstate).finaltup,
            tupdesc,
            BTreeTupleGetNAtts((*pstate).finaltup, rel),
            false,
            0,
            &mut (*so).scanBehind,
        )
    {
        // goto new_prim_scan
        return _bt_advance_array_keys_new_prim_scan(scan, pstate, so);
    }

    /*
     * When we encounter a truncated finaltup high key attribute, we're
     * optimistic about the chances of its corresponding required scan key
     * being satisfied.
     */
    if (*so).scanBehind {
        /* Truncated high key -- _bt_scanbehind_checkkeys recheck scheduled */
    } else if has_required_opposite_direction_only
        && !(*pstate).finaltup.is_null()
        && unlikely(!_bt_oppodir_checkkeys(scan, dir, (*pstate).finaltup))
    {
        // goto new_prim_scan
        return _bt_advance_array_keys_new_prim_scan(scan, pstate, so);
    }

    // continue_scan:
    /*
     * Stick with the ongoing primitive index scan for now.
     */
    (*pstate).continuescan = true; /* Override _bt_check_compare */
    (*so).needPrimScan = false; /* _bt_readpage has more tuples to check */

    if (*so).scanBehind {
        (*so).oppositeDirCheck = has_required_opposite_direction_only;

        if ScanDirectionIsForward(dir) {
            (*pstate).skip = (*pstate).maxoff + 1;
        }
    }

    /* Caller's tuple doesn't match the new qual */
    false
}

/// Helper extracted from _bt_advance_array_keys goto new_prim_scan.
pub unsafe fn _bt_advance_array_keys_new_prim_scan(
    scan: IndexScanDesc,
    pstate: *mut BTReadPageState,
    so: BTScanOpaque,
) -> bool {
    Assert!(!(*pstate).finaltup.is_null()); /* not on rightmost/leftmost page */

    /*
     * Looks like another primitive index scan is required.  But consider
     * continuing the current primscan based on scan-level heuristics.
     */
    if !(*pstate).firstpage || (*pstate).nskipadvances > NSKIPADVANCES_THRESHOLD {
        /* Schedule a recheck once on the next (or previous) page */
        (*so).scanBehind = true;

        /* Continue the current primitive scan after all */
        // goto continue_scan
        (*pstate).continuescan = true;
        (*so).needPrimScan = false;

        if (*so).scanBehind {
            (*so).oppositeDirCheck = false; /* simplified; real value set by caller */
            if ScanDirectionIsForward((*so).currPos.dir) {
                (*pstate).skip = (*pstate).maxoff + 1;
            }
        }

        return false;
    }

    /*
     * End this primitive index scan, but schedule another.
     */
    (*pstate).continuescan = false; /* Tell _bt_readpage we're done... */
    (*so).needPrimScan = true; /* ...but call _bt_first again */

    if !(*scan).parallel_scan.is_null() {
        _bt_parallel_primscan_schedule(scan, (*so).currPos.currPage);
    }

    /* Caller's tuple doesn't match the new qual */
    false
}

/*
 * _bt_verify_keys_with_arraykeys() -- verify scan keys agree with array state
 */
pub unsafe fn _bt_verify_keys_with_arraykeys(scan: IndexScanDesc) -> bool {
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;
    let mut last_sk_attno: AttrNumber = InvalidAttrNumber;
    let mut arrayidx: c_int = 0;
    let mut nonrequiredseen: bool = false;

    if !(*so).qual_ok {
        return false;
    }

    let mut ikey: c_int = 0;
    while ikey < (*so).numberOfKeys {
        let cur: ScanKey = (*so).keyData.add(ikey as usize);
        let array: *mut BTArrayKeyInfo;

        if (*cur).sk_strategy != BTEqualStrategyNumber
            || !(((*cur).sk_flags as u32) & SK_SEARCHARRAY as u32 != 0)
        {
            ikey += 1;
            continue;
        }

        array = (*so).arrayKeys.add(arrayidx as usize);
        arrayidx += 1;
        if (*array).scan_key != ikey {
            return false;
        }

        if (*array).num_elems == 0 || (*array).num_elems < -1 {
            return false;
        }

        if (*array).num_elems != -1
            && (*cur).sk_argument != *(*array).elem_values.add((*array).cur_elem as usize)
        {
            return false;
        }
        if ((*cur).sk_flags as u32) & (SK_BT_REQFWD | SK_BT_REQBKWD) != 0 {
            if last_sk_attno > (*cur).sk_attno {
                return false;
            }
            if nonrequiredseen {
                return false;
            }
        } else {
            nonrequiredseen = true;
        }

        last_sk_attno = (*cur).sk_attno;
        ikey += 1;
    }

    if arrayidx != (*so).numArrayKeys {
        return false;
    }

    true
}

/*
 * Test whether an indextuple satisfies all the scankey conditions.
 */
pub unsafe fn _bt_checkkeys(
    scan: IndexScanDesc,
    pstate: *mut BTReadPageState,
    arrayKeys: bool,
    tuple: IndexTuple,
    tupnatts: c_int,
) -> bool {
    let tupdesc: TupleDesc = RelationGetDescr((*scan).indexRelation);
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;
    let dir: ScanDirection = (*so).currPos.dir;
    let mut ikey: c_int = (*pstate).startikey;
    let res: bool;

    Assert!(BTreeTupleGetNAtts(tuple, (*scan).indexRelation) == tupnatts);
    Assert!(!(*so).needPrimScan && !(*so).scanBehind && !(*so).oppositeDirCheck);
    Assert!(arrayKeys || (*so).numArrayKeys == 0);

    res = _bt_check_compare(
        scan,
        dir,
        tuple,
        tupnatts,
        tupdesc,
        arrayKeys,
        (*pstate).forcenonrequired,
        &mut (*pstate).continuescan,
        &mut ikey,
    );

    Assert!(!(*pstate).forcenonrequired || arrayKeys);

    #[cfg(feature = "use_assert_checking")]
    if (*pstate).startikey > 0 && !(*pstate).forcenonrequired {
        let mut dres: bool;
        let mut dcontinuescan: bool = false;
        let mut dikey: c_int = 0;

        /* Pass arrayKeys=false to avoid array side-effects */
        dres = _bt_check_compare(
            scan,
            dir,
            tuple,
            tupnatts,
            tupdesc,
            false,
            (*pstate).forcenonrequired,
            &mut dcontinuescan,
            &mut dikey,
        );
        Assert!(res == dres);
        Assert!((*pstate).continuescan == dcontinuescan);
        Assert!(arrayKeys || ikey == dikey);
        Assert!(ikey <= dikey);
    }

    /*
     * Only one _bt_check_compare call is required in the common case where
     * there are no equality strategy array scan keys.
     */
    if !arrayKeys || (*pstate).continuescan {
        return res;
    }

    /*
     * _bt_check_compare call set continuescan=false in the presence of
     * equality type array keys.
     */
    Assert!(!(*pstate).forcenonrequired);
    if _bt_tuple_before_array_skeys(scan, dir, tuple, tupdesc, tupnatts, true, ikey, core::ptr::null_mut()) {
        /* Override _bt_check_compare, continue primitive scan */
        (*pstate).continuescan = true;

        (*pstate).rechecks += 1;
        if (*pstate).rechecks >= LOOK_AHEAD_REQUIRED_RECHECKS {
            /* See if we should skip ahead within the current leaf page */
            _bt_checkkeys_look_ahead(scan, pstate, tupnatts, tupdesc);
        }

        /* This indextuple doesn't match the current qual, in any case */
        return false;
    }

    /*
     * Caller's tuple is >= the current set of array keys.  It's now
     * clear that we _must_ advance any required array keys in lockstep with
     * the scan.
     */
    _bt_advance_array_keys(scan, pstate, tuple, tupnatts, tupdesc, ikey, true)
}

/*
 * Test whether caller's finaltup tuple is still before the start of matches
 * for the current array keys.
 */
pub unsafe fn _bt_scanbehind_checkkeys(
    scan: IndexScanDesc,
    dir: ScanDirection,
    finaltup: IndexTuple,
) -> bool {
    let rel: Relation = (*scan).indexRelation;
    let tupdesc: TupleDesc = RelationGetDescr(rel);
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;
    let nfinaltupatts: c_int = BTreeTupleGetNAtts(finaltup, rel);
    let mut scanBehind: bool = false;

    Assert!((*so).numArrayKeys != 0);

    if _bt_tuple_before_array_skeys(scan, dir, finaltup, tupdesc, nfinaltupatts, false, 0, &mut scanBehind) {
        return false;
    }

    /*
     * If scanBehind was set, all of the untruncated attribute values from
     * finaltup match the array's current element, but there are other keys
     * associated with truncated suffix attributes.
     */
    if scanBehind {
        return false;
    }

    if !(*so).oppositeDirCheck {
        return true;
    }

    _bt_oppodir_checkkeys(scan, dir, finaltup)
}

/*
 * Test whether an indextuple fails to satisfy an inequality required in the
 * opposite direction only.
 */
pub unsafe fn _bt_oppodir_checkkeys(
    scan: IndexScanDesc,
    dir: ScanDirection,
    finaltup: IndexTuple,
) -> bool {
    let rel: Relation = (*scan).indexRelation;
    let tupdesc: TupleDesc = RelationGetDescr(rel);
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;
    let nfinaltupatts: c_int = BTreeTupleGetNAtts(finaltup, rel);
    let mut continuescan: bool = false;
    let flipped: ScanDirection = -dir;
    let mut ikey: c_int = 0;

    Assert!((*so).numArrayKeys != 0);

    _bt_check_compare(
        scan,
        flipped,
        finaltup,
        nfinaltupatts,
        tupdesc,
        false,
        false,
        &mut continuescan,
        &mut ikey,
    );

    if !continuescan
        && (*(*so).keyData.add(ikey as usize)).sk_strategy != BTEqualStrategyNumber
    {
        return false;
    }

    true
}

/*
 * _bt_set_startikey() -- Determines an offset to the first scan key that is
 * _not_ guaranteed to be satisfied by every tuple from pstate.page.
 */
pub unsafe fn _bt_set_startikey(scan: IndexScanDesc, pstate: *mut BTReadPageState) {
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;
    let rel: Relation = (*scan).indexRelation;
    let tupdesc: TupleDesc = RelationGetDescr(rel);
    let iid: *mut ItemIdData;
    let firsttup: IndexTuple;
    let lasttup: IndexTuple;
    let mut startikey: c_int = 0;
    let mut arrayidx: c_int = 0;
    let mut firstchangingattnum: c_int;
    let mut start_past_saop_eq: bool = false;

    // Conservative: skip the startikey optimization, leaving startikey=0 so
    // _bt_readpage compares all scan keys from the start (correct, unoptimized).
    // TODO(pg-port): re-enable once _bt_keep_natts_fast/CompactAttr path verified.
    let _ = (so, rel, tupdesc, startikey, arrayidx, start_past_saop_eq);
    if true {
        return;
    }

    Assert!(!(*so).scanBehind);
    Assert!((*pstate).minoff < (*pstate).maxoff);
    Assert!(!(*pstate).firstpage);
    Assert!((*pstate).startikey == 0);
    Assert!(
        (*so).numArrayKeys == 0
            || !(*pstate).finaltup.is_null()
            || P_RIGHTMOST(BTPageGetOpaque((*pstate).page))
            || P_LEFTMOST(BTPageGetOpaque((*pstate).page)),
    );

    if (*so).numberOfKeys == 0 {
        return;
    }

    /* minoff is an offset to the lowest non-pivot tuple on the page */
    let iid_first: *mut ItemIdData = PageGetItemId((*pstate).page, (*pstate).minoff);
    firsttup = PageGetItem((*pstate).page, iid_first) as IndexTuple;

    /* maxoff is an offset to the highest non-pivot tuple on the page */
    let iid_last: *mut ItemIdData = PageGetItemId((*pstate).page, (*pstate).maxoff);
    lasttup = PageGetItem((*pstate).page, iid_last) as IndexTuple;

    /* Determine the first attribute whose values change on caller's page */
    firstchangingattnum = _bt_keep_natts_fast(rel, firsttup, lasttup);

    'outer: loop {
        if startikey >= (*so).numberOfKeys {
            break 'outer;
        }
        let key: ScanKey = (*so).keyData.add(startikey as usize);
        let array: *mut BTArrayKeyInfo;
        let mut firstdatum: Datum = 0;
        let mut lastdatum: Datum = 0;
        let mut firstnull: bool = false;
        let mut lastnull: bool = false;
        let mut result: int32 = 0;

        if !(((*key).sk_flags as u32) & (SK_BT_REQFWD | SK_BT_REQBKWD) != 0) {
            /* Scan key isn't marked required (corner case) */
            break 'outer; /* unsafe */
        }
        if ((*key).sk_flags as u32) & SK_ROW_HEADER as u32 != 0 {
            /* RowCompare inequalities currently aren't supported */
            break 'outer; /* "unsafe" */
        }
        if (*key).sk_strategy != BTEqualStrategyNumber {
            /*
             * Scalar inequality key.
             */
            if (*key).sk_attno > firstchangingattnum as AttrNumber {
                break 'outer; /* unsafe */
            }

            firstdatum = index_getattr(firsttup, (*key).sk_attno as c_int, tupdesc, &mut firstnull);
            lastdatum = index_getattr(lasttup, (*key).sk_attno as c_int, tupdesc, &mut lastnull);

            if ((*key).sk_flags as u32) & SK_ISNULL as u32 != 0 {
                /* IS NOT NULL key */
                Assert!(((*key).sk_flags as u32) & SK_SEARCHNOTNULL as u32 != 0);

                if firstnull || lastnull {
                    break 'outer; /* unsafe */
                }

                /* Safe, IS NOT NULL key satisfied by every tuple */
                startikey += 1;
                continue 'outer;
            }

            /* Test firsttup */
            if firstnull
                || !DatumGetBool(FunctionCall2Coll(
                    &mut (*key).sk_func,
                    (*key).sk_collation,
                    firstdatum,
                    (*key).sk_argument,
                ))
            {
                break 'outer; /* unsafe */
            }

            /* Test lasttup */
            if lastnull
                || !DatumGetBool(FunctionCall2Coll(
                    &mut (*key).sk_func,
                    (*key).sk_collation,
                    lastdatum,
                    (*key).sk_argument,
                ))
            {
                break 'outer; /* unsafe */
            }

            /* Safe, scalar inequality satisfied by every tuple */
            startikey += 1;
            continue 'outer;
        }

        /* Some = key (could be a scalar = key, could be an array = key) */
        Assert!((*key).sk_strategy == BTEqualStrategyNumber);

        if !(((*key).sk_flags as u32) & SK_SEARCHARRAY as u32 != 0) {
            /*
             * Scalar = key (possibly an IS NULL key).
             */
            if (*key).sk_attno >= firstchangingattnum as AttrNumber {
                break 'outer; /* unsafe, multiple distinct attr values */
            }

            firstdatum = index_getattr(firsttup, (*key).sk_attno as c_int, tupdesc, &mut firstnull);
            if ((*key).sk_flags as u32) & SK_ISNULL as u32 != 0 {
                /* IS NULL key */
                Assert!(((*key).sk_flags as u32) & SK_SEARCHNULL as u32 != 0);

                if !firstnull {
                    break 'outer; /* unsafe */
                }

                /* Safe, IS NULL key satisfied by every tuple */
                startikey += 1;
                continue 'outer;
            }
            if firstnull
                || !DatumGetBool(FunctionCall2Coll(
                    &mut (*key).sk_func,
                    (*key).sk_collation,
                    firstdatum,
                    (*key).sk_argument,
                ))
            {
                break 'outer; /* unsafe */
            }

            /* Safe, scalar = key satisfied by every tuple */
            startikey += 1;
            continue 'outer;
        }

        /* = array key (could be a SAOP array, could be a skip array) */
        array = (*so).arrayKeys.add(arrayidx as usize);
        arrayidx += 1;
        Assert!((*array).scan_key == startikey);
        if (*array).num_elems != -1 {
            /*
             * SAOP array = key.
             */
            if (*key).sk_attno >= firstchangingattnum as AttrNumber {
                break 'outer; /* unsafe */
            }

            firstdatum = index_getattr(firsttup, (*key).sk_attno as c_int, tupdesc, &mut firstnull);
            _bt_binsrch_array_skey(
                (*so).orderProcs.add(startikey as usize),
                false,
                NoMovementScanDirection,
                firstdatum,
                firstnull,
                array,
                key,
                &mut result,
            );
            if result != 0 {
                break 'outer; /* unsafe */
            }

            /* Safe, SAOP = key satisfied by every tuple */
            start_past_saop_eq = true;
            startikey += 1;
            continue 'outer;
        }

        /*
         * Skip array = key
         */
        Assert!(((*key).sk_flags as u32) & SK_BT_SKIP != 0);
        if (*array).null_elem {
            /*
             * Non-range skip array = key.
             * Safe, non-range skip array "satisfied" by every tuple on page.
             */
            startikey += 1;
            continue 'outer;
        }

        /*
         * Range skip array = key.
         */
        if (*key).sk_attno > firstchangingattnum as AttrNumber {
            break 'outer; /* unsafe */
        }

        firstdatum = index_getattr(firsttup, (*key).sk_attno as c_int, tupdesc, &mut firstnull);
        lastdatum = index_getattr(lasttup, (*key).sk_attno as c_int, tupdesc, &mut lastnull);

        /* Test firsttup */
        _bt_binsrch_skiparray_skey(
            false,
            ForwardScanDirection,
            firstdatum,
            firstnull,
            array,
            key,
            &mut result,
        );
        if result != 0 {
            break 'outer; /* unsafe */
        }

        /* Test lasttup */
        _bt_binsrch_skiparray_skey(
            false,
            ForwardScanDirection,
            lastdatum,
            lastnull,
            array,
            key,
            &mut result,
        );
        if result != 0 {
            break 'outer; /* unsafe */
        }

        /* Safe, range skip array satisfied by every tuple on page */
        startikey += 1;
        // continue 'outer implicit
    }

    /*
     * Use of forcenonrequired is typically undesirable, since it'll force
     * _bt_readpage caller to read every tuple on the page.
     */
    (*pstate).forcenonrequired = start_past_saop_eq || (*so).skipScan;
    (*pstate).startikey = startikey;

    /*
     * _bt_readpage caller is required to call _bt_checkkeys against page's
     * finaltup with forcenonrequired=false whenever we initially set
     * forcenonrequired=true.
     */
    Assert!(!(*pstate).forcenonrequired || (*so).numArrayKeys != 0);
    if (*pstate).forcenonrequired && (*pstate).finaltup.is_null() {
        (*pstate).forcenonrequired = false;
        (*pstate).startikey = 0;
    }
}

// End of Part 4.

// ===========================================================================
// Part 5 -- _bt_check_compare, _bt_check_rowcompare,
//            _bt_checkkeys_look_ahead, _bt_killitems
// ===========================================================================

/*
 * _bt_check_compare() -- Test whether an indextuple satisfies current scan condition.
 *
 * Return true if so, false if not.  If not, also sets *continuescan to false
 * when it's also not possible for any later tuples to pass.
 */
pub unsafe fn _bt_check_compare(
    scan: IndexScanDesc,
    dir: ScanDirection,
    tuple: IndexTuple,
    tupnatts: c_int,
    tupdesc: TupleDesc,
    advancenonrequired: bool,
    forcenonrequired: bool,
    continuescan: *mut bool,
    ikey: *mut c_int,
) -> bool {
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;

    *continuescan = true; /* default assumption */

    while *ikey < (*so).numberOfKeys {
        let key: ScanKey = (*so).keyData.add(*ikey as usize);
        let mut datum: Datum = 0;
        let mut isNull: bool = false;
        let mut requiredSameDir: bool = false;
        let mut requiredOppositeDirOnly: bool = false;

        /*
         * Check if the key is required in the current scan direction, in the
         * opposite scan direction _only_, or in neither direction.
         */
        if forcenonrequired {
            /* treating scan's keys as non-required */
        } else if ((((*key).sk_flags as u32) & SK_BT_REQFWD != 0) && ScanDirectionIsForward(dir))
            || ((((*key).sk_flags as u32) & SK_BT_REQBKWD != 0) && ScanDirectionIsBackward(dir))
        {
            requiredSameDir = true;
        } else if ((((*key).sk_flags as u32) & SK_BT_REQFWD != 0) && ScanDirectionIsBackward(dir))
            || ((((*key).sk_flags as u32) & SK_BT_REQBKWD != 0) && ScanDirectionIsForward(dir))
        {
            requiredOppositeDirOnly = true;
        }

        if (*key).sk_attno > tupnatts as AttrNumber {
            /*
             * This attribute is truncated (must be high key).  Assume that
             * truncated attribute passes the qual.
             */
            Assert!(BTreeTupleIsPivot(tuple));
            *ikey += 1;
            continue;
        }

        /*
         * A skip array scan key uses one of several sentinel values.  We just
         * fall back on _bt_tuple_before_array_skeys when we see such a value.
         */
        if ((*key).sk_flags as u32) & (SK_BT_MINVAL | SK_BT_MAXVAL | SK_BT_NEXT | SK_BT_PRIOR) != 0 {
            Assert!(((*key).sk_flags as u32) & SK_SEARCHARRAY as u32 != 0);
            Assert!(((*key).sk_flags as u32) & SK_BT_SKIP != 0);
            Assert!(requiredSameDir || forcenonrequired);

            /*
             * Cannot fall back on _bt_tuple_before_array_skeys when we're
             * treating the scan's keys as nonrequired, though.
             */
            if forcenonrequired {
                return _bt_advance_array_keys(scan, core::ptr::null_mut(), tuple, tupnatts, tupdesc, *ikey, false);
            }

            *continuescan = false;
            return false;
        }

        /* row-comparison keys need special processing */
        if ((*key).sk_flags as u32) & SK_ROW_HEADER as u32 != 0 {
            if _bt_check_rowcompare(key, tuple, tupnatts, tupdesc, dir, forcenonrequired, continuescan) {
                *ikey += 1;
                continue;
            }
            return false;
        }

        datum = index_getattr(tuple, (*key).sk_attno as c_int, tupdesc, &mut isNull);

        if ((*key).sk_flags as u32) & SK_ISNULL as u32 != 0 {
            /* Handle IS NULL/NOT NULL tests */
            if ((*key).sk_flags as u32) & SK_SEARCHNULL as u32 != 0 {
                if isNull {
                    *ikey += 1;
                    continue; /* tuple satisfies this qual */
                }
            } else {
                Assert!(((*key).sk_flags as u32) & SK_SEARCHNOTNULL as u32 != 0);
                Assert!(!(((*key).sk_flags as u32) & SK_BT_SKIP != 0));
                if !isNull {
                    *ikey += 1;
                    continue; /* tuple satisfies this qual */
                }
            }

            /*
             * Tuple fails this qual.  If it's a required qual for the current
             * scan direction, then we can conclude no further tuples will
             * pass, either.
             */
            if requiredSameDir {
                *continuescan = false;
            } else if unlikely(((*key).sk_flags as u32) & SK_BT_SKIP != 0) {
                /*
                 * If we're treating scan keys as nonrequired, and encounter a
                 * skip array scan key whose current element is NULL, then it
                 * must be a non-range skip array.
                 */
                Assert!(forcenonrequired && *ikey > 0);
                *ikey += 1;
                continue;
            }

            /* This indextuple doesn't match the qual. */
            return false;
        }

        if isNull {
            /*
             * Scalar scan key isn't satisfied by NULL tuple value.
             */
            if unlikely(forcenonrequired && ((*key).sk_flags as u32) & SK_BT_SKIP != 0) {
                return _bt_advance_array_keys(scan, core::ptr::null_mut(), tuple, tupnatts, tupdesc, *ikey, false);
            }

            if ((*key).sk_flags as u32) & SK_BT_NULLS_FIRST != 0 {
                /*
                 * Since NULLs are sorted before non-NULLs, we know we have
                 * reached the lower limit of the range of values for this
                 * index attr.  On a backward scan, we can stop if this qual
                 * is one of the "must match" subset.
                 */
                if (requiredSameDir || requiredOppositeDirOnly) && ScanDirectionIsBackward(dir) {
                    *continuescan = false;
                }
            } else {
                /*
                 * Since NULLs are sorted after non-NULLs, we know we have
                 * reached the upper limit of the range of values for this
                 * index attr.  On a forward scan, we can stop if this qual is
                 * one of the "must match" subset.
                 */
                if (requiredSameDir || requiredOppositeDirOnly) && ScanDirectionIsForward(dir) {
                    *continuescan = false;
                }
            }

            /* This indextuple doesn't match the qual. */
            return false;
        }

        if !DatumGetBool(FunctionCall2Coll(
            &mut (*key).sk_func,
            (*key).sk_collation,
            datum,
            (*key).sk_argument,
        )) {
            /*
             * Tuple fails this qual.  If it's a required qual for the current
             * scan direction, then we can conclude no further tuples will
             * pass, either.
             */
            if requiredSameDir {
                *continuescan = false;
            }

            /*
             * If this is a non-required equality-type array key, the tuple
             * needs to be checked against every possible array key.
             */
            else if advancenonrequired
                && (*key).sk_strategy == BTEqualStrategyNumber
                && ((*key).sk_flags as u32) & SK_SEARCHARRAY as u32 != 0
            {
                return _bt_advance_array_keys(scan, core::ptr::null_mut(), tuple, tupnatts, tupdesc, *ikey, false);
            }

            /* This indextuple doesn't match the qual. */
            return false;
        }

        *ikey += 1;
    }

    /* If we get here, the tuple passes all index quals. */
    true
}

/*
 * Test whether an indextuple satisfies a row-comparison scan condition.
 */
pub unsafe fn _bt_check_rowcompare(
    header: ScanKey,
    tuple: IndexTuple,
    tupnatts: c_int,
    tupdesc: TupleDesc,
    dir: ScanDirection,
    forcenonrequired: bool,
    continuescan: *mut bool,
) -> bool {
    let mut subkey: ScanKey = DatumGetPointer((*header).sk_argument) as ScanKey;
    let mut cmpresult: int32 = 0;
    let result: bool;

    /* First subkey should be same as the header says */
    Assert!(((*header).sk_flags as u32) & SK_ROW_HEADER as u32 != 0);
    Assert!((*subkey).sk_attno == (*header).sk_attno);
    Assert!((*subkey).sk_strategy == (*header).sk_strategy);

    /* Loop over columns of the row condition */
    loop {
        let mut datum: Datum = 0;
        let mut isNull: bool = false;

        Assert!(((*subkey).sk_flags as u32) & SK_ROW_MEMBER as u32 != 0);

        /* When a NULL row member is compared, the row never matches */
        if ((*subkey).sk_flags as u32) & SK_ISNULL as u32 != 0 {
            /*
             * Unlike the simple-scankey case, this isn't a disallowed case
             * (except when it's the first row element that has the NULL arg).
             * But it can never match.
             */
            Assert!(subkey != DatumGetPointer((*header).sk_argument) as ScanKey);
            subkey = subkey.sub(1);
            if forcenonrequired {
                /* treating scan's keys as non-required */
            } else if ((*subkey).sk_flags as u32) & SK_BT_REQFWD != 0
                && ScanDirectionIsForward(dir)
            {
                *continuescan = false;
            } else if ((*subkey).sk_flags as u32) & SK_BT_REQBKWD != 0
                && ScanDirectionIsBackward(dir)
            {
                *continuescan = false;
            }
            return false;
        }

        if (*subkey).sk_attno > tupnatts as AttrNumber {
            /*
             * This attribute is truncated (must be high key).  Assume that
             * truncated attribute passes the qual.
             */
            Assert!(BTreeTupleIsPivot(tuple));
            return true;
        }

        datum = index_getattr(tuple, (*subkey).sk_attno as c_int, tupdesc, &mut isNull);

        if isNull {
            let reqflags: u32;

            if forcenonrequired {
                /* treating scan's keys as non-required */
            } else if ((*subkey).sk_flags as u32) & SK_BT_NULLS_FIRST != 0 {
                /*
                 * Since NULLs are sorted before non-NULLs, we know we have
                 * reached the lower limit of the range of values for this
                 * index attr.  On a backward scan, we can stop if this qual
                 * is one of the "must match" subset.
                 */
                let mut reqflags: u32 = SK_BT_REQBKWD;

                /*
                 * When a most significant required NULLS FIRST < row compare
                 * member sees NULL tuple values during a backwards scan, it
                 * signals the end of matches for the whole row compare/scan.
                 */
                if subkey == DatumGetPointer((*header).sk_argument) as ScanKey {
                    reqflags |= SK_BT_REQFWD; /* safe, first row member */
                }

                if ((*subkey).sk_flags as u32) & reqflags != 0 && ScanDirectionIsBackward(dir) {
                    *continuescan = false;
                }
            } else {
                /*
                 * Since NULLs are sorted after non-NULLs, we know we have
                 * reached the upper limit of the range of values for this
                 * index attr.  On a forward scan, we can stop if this qual is
                 * one of the "must match" subset.
                 */
                let mut reqflags: u32 = SK_BT_REQFWD;

                /*
                 * When a most significant required NULLS LAST > row compare
                 * member sees NULL tuple values during a forwards scan, it
                 * signals the end of matches for the whole row compare/scan.
                 */
                if subkey == DatumGetPointer((*header).sk_argument) as ScanKey {
                    reqflags |= SK_BT_REQBKWD; /* safe, first row member */
                }

                if ((*subkey).sk_flags as u32) & reqflags != 0 && ScanDirectionIsForward(dir) {
                    *continuescan = false;
                }
            }

            /* In any case, this indextuple doesn't match the qual. */
            return false;
        }

        /* Perform the test --- three-way comparison not bool operator */
        cmpresult = DatumGetInt32(FunctionCall2Coll(
            &mut (*subkey).sk_func,
            (*subkey).sk_collation,
            datum,
            (*subkey).sk_argument,
        ));

        if ((*subkey).sk_flags as u32) & SK_BT_DESC != 0 {
            INVERT_COMPARE_RESULT(&mut cmpresult);
        }

        /* Done comparing if unequal, else advance to next column */
        if cmpresult != 0 {
            break;
        }

        if ((*subkey).sk_flags as u32) & SK_ROW_END as u32 != 0 {
            break;
        }
        subkey = subkey.add(1);
    }

    /*
     * At this point cmpresult indicates the overall result of the row
     * comparison, and subkey points to the deciding column.
     */
    result = match (*subkey).sk_strategy {
        /* EQ and NE cases aren't allowed here */
        s if s == BTLessStrategyNumber         => cmpresult < 0,
        s if s == BTLessEqualStrategyNumber    => cmpresult <= 0,
        s if s == BTGreaterEqualStrategyNumber => cmpresult >= 0,
        s if s == BTGreaterStrategyNumber      => cmpresult > 0,
        s => {
            elog!(ERROR, "unexpected strategy number {}", s);
            false /* keep compiler quiet */
        }
    };

    if !result && !forcenonrequired {
        /*
         * Tuple fails this qual.  If it's a required qual for the current
         * scan direction, then we can conclude no further tuples will pass,
         * either.  Note we have to look at the deciding column.
         */
        if ((*subkey).sk_flags as u32) & SK_BT_REQFWD != 0 && ScanDirectionIsForward(dir) {
            *continuescan = false;
        } else if ((*subkey).sk_flags as u32) & SK_BT_REQBKWD != 0 && ScanDirectionIsBackward(dir) {
            *continuescan = false;
        }
    }

    result
}

/*
 * _bt_checkkeys_look_ahead() -- Determine if a scan should skip uninteresting tuples.
 *
 * Subroutine for _bt_checkkeys.  Called when _bt_readpage's linear search
 * process has already scanned an excessive number of tuples.
 */
pub unsafe fn _bt_checkkeys_look_ahead(
    scan: IndexScanDesc,
    pstate: *mut BTReadPageState,
    tupnatts: c_int,
    tupdesc: TupleDesc,
) {
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;
    let dir: ScanDirection = (*so).currPos.dir;
    let aheadoffnum: OffsetNumber;
    let ahead: IndexTuple;

    Assert!(!(*pstate).forcenonrequired);

    /* Avoid looking ahead when comparing the page high key */
    if (*pstate).offnum < (*pstate).minoff {
        return;
    }

    /*
     * Don't look ahead when there aren't enough tuples remaining on the page.
     */
    if ScanDirectionIsForward(dir)
        && (*pstate).offnum >= (*pstate).maxoff - LOOK_AHEAD_DEFAULT_DISTANCE as OffsetNumber
    {
        return;
    } else if ScanDirectionIsBackward(dir)
        && (*pstate).offnum <= (*pstate).minoff + LOOK_AHEAD_DEFAULT_DISTANCE as OffsetNumber
    {
        return;
    }

    /*
     * The look ahead distance starts small, and ramps up as each call here
     * allows _bt_readpage to skip over more tuples
     */
    if (*pstate).targetdistance == 0 {
        (*pstate).targetdistance = LOOK_AHEAD_DEFAULT_DISTANCE;
    } else if (*pstate).targetdistance < MaxIndexTuplesPerPage / 2 {
        (*pstate).targetdistance *= 2;
    }

    /* Don't read past the end (or before the start) of the page, though */
    if ScanDirectionIsForward(dir) {
        aheadoffnum = Min(
            (*pstate).maxoff as c_int,
            (*pstate).offnum as c_int + (*pstate).targetdistance,
        ) as OffsetNumber;
    } else {
        aheadoffnum = Max(
            (*pstate).minoff as c_int,
            (*pstate).offnum as c_int - (*pstate).targetdistance,
        ) as OffsetNumber;
    }

    ahead = PageGetItem((*pstate).page, PageGetItemId((*pstate).page, aheadoffnum)) as IndexTuple;
    if _bt_tuple_before_array_skeys(scan, dir, ahead, tupdesc, tupnatts, false, 0, core::ptr::null_mut()) {
        /*
         * Success -- instruct _bt_readpage to skip ahead to very next tuple
         * after the one we determined was still before the current array keys
         */
        if ScanDirectionIsForward(dir) {
            (*pstate).skip = aheadoffnum + 1;
        } else {
            (*pstate).skip = aheadoffnum - 1;
        }
    } else {
        /*
         * Failure -- "ahead" tuple is too far ahead (we were too aggressive).
         * Reset the number of rechecks, and aggressively reduce the target
         * distance.
         */
        (*pstate).rechecks = 0;
        (*pstate).targetdistance = Max((*pstate).targetdistance / 8, 1);
    }
}

/*
 * _bt_killitems - set LP_DEAD state for items an indexscan caller has
 * told us were killed
 */
pub unsafe fn _bt_killitems(scan: IndexScanDesc) {
    let rel: Relation = (*scan).indexRelation;
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;
    let page: Page;
    let opaque: BTPageOpaque;
    let minoff: OffsetNumber;
    let maxoff: OffsetNumber;
    let numKilled: c_int = (*so).numKilled;
    let mut killedsomething: bool = false;
    let buf: Buffer;

    Assert!(numKilled > 0);
    Assert!(BTScanPosIsValid(core::ptr::read(&(*so).currPos)));
    Assert!(!(*scan).heapRelation.is_null()); /* can't be a bitmap index scan */

    /* Always invalidate so->killedItems[] before leaving so->currPos */
    (*so).numKilled = 0;

    if !(*so).dropPin {
        /*
         * We have held the pin on this page since we read the index tuples,
         * so all we need to do is lock it.
         */
        Assert!(BTScanPosIsPinned(core::ptr::read(&(*so).currPos)));
        buf = (*so).currPos.buf;
        _bt_lockbuf(rel, buf, BT_READ);
    } else {
        let latestlsn: u64;

        Assert!(!BTScanPosIsPinned(core::ptr::read(&(*so).currPos)));
        Assert!(RelationNeedsWAL(rel));
        buf = _bt_getbuf(rel, (*so).currPos.currPage, BT_READ);

        latestlsn = BufferGetLSNAtomic(buf);
        Assert!((*so).currPos.lsn <= latestlsn);
        if (*so).currPos.lsn != latestlsn {
            /* Modified, give up on hinting */
            _bt_relbuf(rel, buf);
            return;
        }

        /* Unmodified, hinting is safe */
    }

    page = BufferGetPage(buf);
    opaque = BTPageGetOpaque(page);
    minoff = P_FIRSTDATAKEY(opaque);
    maxoff = PageGetMaxOffsetNumber(page);

    let mut i: c_int = 0;
    while i < numKilled {
        let itemIndex: c_int = *(*so).killedItems.add(i as usize);
        let mut kitem: *mut BTScanPosItem = (*so).currPos.items.as_mut_ptr().add(itemIndex as usize);
        let mut offnum: OffsetNumber = (*kitem).indexOffset;

        Assert!(
            itemIndex >= (*so).currPos.firstItem && itemIndex <= (*so).currPos.lastItem,
        );
        if offnum < minoff {
            i += 1;
            continue; /* pure paranoia */
        }
        while offnum <= maxoff {
            let iid: *mut ItemIdData = PageGetItemId(page, offnum);
            let ituple: IndexTuple = PageGetItem(page, iid) as IndexTuple;
            let mut killtuple: bool = false;

            if BTreeTupleIsPosting(ituple) {
                let mut pi: c_int = i + 1;
                let nposting: c_int = BTreeTupleGetNPosting(ituple);
                let mut j: c_int = 0;

                /*
                 * We rely on the convention that heap TIDs in the scanpos
                 * items array are stored in ascending heap TID order for a
                 * group of TIDs that originally came from a posting list
                 * tuple.
                 */
                while j < nposting {
                    let item: ItemPointer = BTreeTupleGetPostingN(ituple, j);

                    if !ItemPointerEquals(item, &mut (*kitem).heapTid) {
                        break; /* out of posting list loop */
                    }

                    /*
                     * kitem must have matching offnum when heap TIDs match,
                     * though only in the common case where the page can't
                     * have been concurrently modified
                     */
                    Assert!((*kitem).indexOffset == offnum || !(*so).dropPin);

                    /*
                     * Read-ahead to later kitems here.
                     */
                    if pi < numKilled {
                        kitem = (*so).currPos.items.as_mut_ptr().add(*(*so).killedItems.add(pi as usize) as usize);
                        pi += 1;
                    }
                    j += 1;
                }

                /*
                 * Don't bother advancing the outermost loop's int iterator to
                 * avoid processing killed items that relate to the same
                 * offnum/posting list tuple.
                 */
                if j == nposting {
                    killtuple = true;
                }
            } else if ItemPointerEquals(&mut (*ituple).t_tid, &mut (*kitem).heapTid) {
                killtuple = true;
            }

            /*
             * Mark index item as dead, if it isn't already.
             */
            if killtuple && !ItemIdIsDead(iid) {
                /* found the item/all posting list items */
                ItemIdMarkDead(iid);
                killedsomething = true;
                break; /* out of inner search loop */
            }
            offnum = OffsetNumberNext(offnum);
        }
        i += 1;
    }

    /*
     * Since this can be redone later if needed, mark as dirty hint.
     *
     * Whenever we mark anything LP_DEAD, we also set the page's
     * BTP_HAS_GARBAGE flag.
     */
    if killedsomething {
        (*opaque).btpo_flags |= BTP_HAS_GARBAGE;
        MarkBufferDirtyHint(buf, true);
    }

    if !(*so).dropPin {
        _bt_unlockbuf(rel, buf);
    } else {
        _bt_relbuf(rel, buf);
    }
}

// End of Part 5.

// ===========================================================================
// Part 6 -- BTVacInfo shared-memory area, btoptions, btproperty,
//            btbuildphasename, _bt_truncate, _bt_keep_natts,
//            _bt_keep_natts_fast, _bt_check_natts, _bt_check_third_page,
//            _bt_allequalimage
// ===========================================================================

// ---------------------------------------------------------------------------
// The shared-memory area in which we track vacuum cycle IDs.
// ---------------------------------------------------------------------------

/// Global pointer to vacuum state -- set by BTreeShmemInit().
/// TODO(pg-port): real definition needs ShmemInitStruct (storage/shmem.h).
static mut btvacinfo: *mut BTVacInfo = core::ptr::null_mut();

// TODO(pg-port): MaxBackends / IsUnderPostmaster from miscadmin.h.
extern "C" {
    static MaxBackends: c_int;
    static IsUnderPostmaster: bool;
}

/*
 * _bt_vacuum_cycleid --- get the active vacuum cycle ID for an index,
 *		or zero if there is no active VACUUM
 */
pub unsafe fn _bt_vacuum_cycleid(rel: Relation) -> BTCycleId {
    let mut result: BTCycleId = 0;
    let mut i: c_int;

    /*
     * If the btree-vacuum shared state was never set up, no btree VACUUM can be
     * in progress, so the active cycle id is 0.
     */
    if btvacinfo.is_null() {
        return 0;
    }

    /* Share lock is enough since this is a read-only operation */
    LWLockAcquire(BtreeVacuumLock, LW_SHARED);

    i = 0;
    while i < (*btvacinfo).num_vacuums {
        let vac: *mut BTOneVacInfo = (*btvacinfo).vacuums.as_mut_ptr().add(i as usize);

        if (*vac).relid.relId == (*rel).rd_lockInfo.lockRelId.relId
            && (*vac).relid.dbId == (*rel).rd_lockInfo.lockRelId.dbId
        {
            result = (*vac).cycleid;
            break;
        }
        i += 1;
    }

    LWLockRelease(BtreeVacuumLock);
    result
}

/*
 * _bt_start_vacuum --- assign a cycle ID to a just-starting VACUUM operation
 */
pub unsafe fn _bt_start_vacuum(rel: Relation) -> BTCycleId {
    let mut result: BTCycleId;
    let mut i: c_int;
    let mut vac: *mut BTOneVacInfo;

    LWLockAcquire(BtreeVacuumLock, LW_EXCLUSIVE);

    /*
     * Assign the next cycle ID, being careful to avoid zero as well as the
     * reserved high values.
     */
    (*btvacinfo).cycle_ctr += 1;
    result = (*btvacinfo).cycle_ctr;
    if result == 0 || result > MAX_BT_CYCLE_ID {
        (*btvacinfo).cycle_ctr = 1;
        result = 1;
    }

    /* Let's just make sure there's no entry already for this index */
    i = 0;
    while i < (*btvacinfo).num_vacuums {
        vac = (*btvacinfo).vacuums.as_mut_ptr().add(i as usize);
        if (*vac).relid.relId == (*rel).rd_lockInfo.lockRelId.relId
            && (*vac).relid.dbId == (*rel).rd_lockInfo.lockRelId.dbId
        {
            /*
             * Unlike most places in the backend, we have to explicitly
             * release our LWLock before throwing an error.
             */
            LWLockRelease(BtreeVacuumLock);
            elog!(ERROR, "multiple active vacuums for index \"{}\"",
                  core::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy());
        }
        i += 1;
    }

    /* OK, add an entry */
    if (*btvacinfo).num_vacuums >= (*btvacinfo).max_vacuums {
        LWLockRelease(BtreeVacuumLock);
        elog!(ERROR, "out of btvacinfo slots");
    }
    vac = (*btvacinfo).vacuums.as_mut_ptr().add((*btvacinfo).num_vacuums as usize);
    (*vac).relid = (*rel).rd_lockInfo.lockRelId;
    (*vac).cycleid = result;
    (*btvacinfo).num_vacuums += 1;

    LWLockRelease(BtreeVacuumLock);
    result
}

/*
 * _bt_end_vacuum --- mark a btree VACUUM operation as done
 *
 * Note: this is deliberately coded not to complain if no entry is found.
 */
pub unsafe fn _bt_end_vacuum(rel: Relation) {
    let mut i: c_int;

    LWLockAcquire(BtreeVacuumLock, LW_EXCLUSIVE);

    /* Find the array entry */
    i = 0;
    while i < (*btvacinfo).num_vacuums {
        let vac: *mut BTOneVacInfo = (*btvacinfo).vacuums.as_mut_ptr().add(i as usize);

        if (*vac).relid.relId == (*rel).rd_lockInfo.lockRelId.relId
            && (*vac).relid.dbId == (*rel).rd_lockInfo.lockRelId.dbId
        {
            /* Remove it by shifting down the last entry */
            let last: *mut BTOneVacInfo = (*btvacinfo)
                .vacuums
                .as_mut_ptr()
                .add(((*btvacinfo).num_vacuums - 1) as usize);
            core::ptr::copy_nonoverlapping(last, vac, 1);
            (*btvacinfo).num_vacuums -= 1;
            break;
        }
        i += 1;
    }

    LWLockRelease(BtreeVacuumLock);
}

/*
 * _bt_end_vacuum wrapped as an on_shmem_exit callback function
 */
pub unsafe extern "C" fn _bt_end_vacuum_callback(code: c_int, arg: Datum) {
    _bt_end_vacuum(DatumGetPointer(arg) as Relation);
}

/*
 * BTreeShmemSize --- report amount of shared memory space needed
 */
pub unsafe fn BTreeShmemSize() -> Size {
    let mut size: Size;

    size = core::mem::offset_of!(BTVacInfo, vacuums);
    size = add_size(size, mul_size(MaxBackends as Size, size_of::<BTOneVacInfo>()));
    size
}

/*
 * BTreeShmemInit --- initialize this module's shared memory
 */
pub unsafe fn BTreeShmemInit() {
    let mut found: bool = false;

    btvacinfo = ShmemInitStruct(
        b"BTree Vacuum State\0".as_ptr() as *const c_char,
        BTreeShmemSize(),
        &mut found,
    ) as *mut BTVacInfo;

    if !IsUnderPostmaster {
        /* Initialize shared memory area */
        Assert!(!found);

        /*
         * It doesn't really matter what the cycle counter starts at, but
         * having it always start the same doesn't seem good.  Seed with
         * low-order bits of time() instead.
         */
        (*btvacinfo).cycle_ctr = libc_time() as BTCycleId;

        (*btvacinfo).num_vacuums = 0;
        (*btvacinfo).max_vacuums = MaxBackends;
    } else {
        Assert!(found);
    }
}

/// TODO(pg-port): libc time() -- use as seed for cycle_ctr.
pub unsafe fn libc_time() -> i64 {
    extern "C" {
        fn time(t: *mut i64) -> i64;
    }
    time(core::ptr::null_mut())
}

pub unsafe fn btoptions(reloptions: Datum, validate: bool) -> *mut u8 /* bytea */ {
    let tab: [relopt_parse_elt; 3] = [
        relopt_parse_elt {
            optname: b"fillfactor\0".as_ptr() as *const c_char,
            opttype: RELOPT_TYPE_INT,
            offset: core::mem::offset_of!(BTOptions, fillfactor) as c_int,
        },
        relopt_parse_elt {
            optname: b"vacuum_cleanup_index_scale_factor\0".as_ptr() as *const c_char,
            opttype: RELOPT_TYPE_REAL,
            offset: core::mem::offset_of!(BTOptions, vacuum_cleanup_index_scale_factor) as c_int,
        },
        relopt_parse_elt {
            optname: b"deduplicate_items\0".as_ptr() as *const c_char,
            opttype: RELOPT_TYPE_BOOL,
            offset: core::mem::offset_of!(BTOptions, deduplicate_items) as c_int,
        },
    ];

    build_reloptions(
        reloptions,
        validate,
        RELOPT_KIND_BTREE,
        size_of::<BTOptions>(),
        tab.as_ptr(),
        3,
    ) as *mut u8
}

/*
 *	btproperty() -- Check boolean properties of indexes.
 */
pub unsafe fn btproperty(
    index_oid: Oid,
    attno: c_int,
    prop: IndexAMProperty,
    propname: *const c_char,
    res: *mut bool,
    isnull: *mut bool,
) -> bool {
    match prop {
        p if p == AMPROP_RETURNABLE => {
            /* answer only for columns, not AM or whole index */
            if attno == 0 {
                return false;
            }
            /* otherwise, btree can always return data */
            *res = true;
            true
        }
        _ => false, /* punt to generic code */
    }
}

/*
 *	btbuildphasename() -- Return name of index build phase.
 */
pub unsafe fn btbuildphasename(phasenum: i64) -> *const c_char {
    match phasenum {
        p if p == PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE =>
            b"initializing\0".as_ptr() as *const c_char,
        p if p == PROGRESS_BTREE_PHASE_INDEXBUILD_TABLESCAN =>
            b"scanning table\0".as_ptr() as *const c_char,
        p if p == PROGRESS_BTREE_PHASE_PERFORMSORT_1 =>
            b"sorting live tuples\0".as_ptr() as *const c_char,
        p if p == PROGRESS_BTREE_PHASE_PERFORMSORT_2 =>
            b"sorting dead tuples\0".as_ptr() as *const c_char,
        p if p == PROGRESS_BTREE_PHASE_LEAF_LOAD =>
            b"loading tuples in tree\0".as_ptr() as *const c_char,
        _ => core::ptr::null(),
    }
}

/*
 *	_bt_truncate() -- create tuple without unneeded suffix attributes.
 *
 * Returns truncated pivot index tuple allocated in caller's memory context.
 */
pub unsafe fn _bt_truncate(
    rel: Relation,
    lastleft: IndexTuple,
    firstright: IndexTuple,
    itup_key: BTScanInsert,
) -> IndexTuple {
    let itupdesc: TupleDesc = RelationGetDescr(rel);
    let nkeyatts: int16 = IndexRelationGetNumberOfKeyAttributes(rel) as int16;
    let keepnatts: c_int;
    let mut pivot: IndexTuple;
    let tidpivot: IndexTuple;
    let pivotheaptid: ItemPointer;
    let newsize: Size;

    /*
     * We should only ever truncate non-pivot tuples from leaf pages.  It's
     * never okay to truncate when splitting an internal page.
     */
    Assert!(!BTreeTupleIsPivot(lastleft) && !BTreeTupleIsPivot(firstright));

    /* Determine how many attributes must be kept in truncated tuple */
    keepnatts = _bt_keep_natts(rel, lastleft, firstright, itup_key);

    /* #ifdef DEBUG_NO_TRUNCATE omitted -- not a real Rust feature */

    pivot = index_truncate_tuple(itupdesc, firstright, Min(keepnatts, nkeyatts as c_int));

    if BTreeTupleIsPosting(pivot) {
        /*
         * index_truncate_tuple() just returns a straight copy of firstright
         * when it has no attributes to truncate.  When that happens, we may
         * need to truncate away a posting list here instead.
         */
        Assert!(keepnatts == nkeyatts as c_int || keepnatts == nkeyatts as c_int + 1);
        Assert!(IndexRelationGetNumberOfAttributes(rel) == nkeyatts as c_int);
        (*pivot).t_info &= !(INDEX_SIZE_MASK as u16);
        (*pivot).t_info |= MAXALIGN(BTreeTupleGetPostingOffset(firstright)) as u16;
    }

    /*
     * If there is a distinguishing key attribute within pivot tuple, we're done
     */
    if keepnatts <= nkeyatts as c_int {
        BTreeTupleSetNAtts(pivot, keepnatts, false);
        return pivot;
    }

    /*
     * We have to store a heap TID in the new pivot tuple, since no non-TID
     * key attribute value in firstright distinguishes the right side of the
     * split from the left side.
     *
     * Use enlarged space that holds a copy of pivot.
     */
    newsize = MAXALIGN(IndexTupleSize(pivot)) + MAXALIGN(size_of::<ItemPointerData>());
    tidpivot = palloc0(newsize) as IndexTuple;
    memcpy(
        tidpivot as *mut c_void,
        pivot as *const c_void,
        MAXALIGN(IndexTupleSize(pivot)),
    );
    /* Cannot leak memory here */
    pfree(pivot as *mut c_void);

    /*
     * Store all of firstright's key attribute values plus a tiebreaker heap
     * TID value in enlarged pivot tuple
     */
    (*tidpivot).t_info &= !(INDEX_SIZE_MASK as u16);
    (*tidpivot).t_info |= newsize as u16;
    BTreeTupleSetNAtts(tidpivot, nkeyatts as c_int, true);
    pivotheaptid = BTreeTupleGetHeapTID(tidpivot);

    /*
     * Lehman & Yao use lastleft as the leaf high key in all cases, but don't
     * consider suffix truncation.  It seems like a good idea to follow that
     * example in cases where no truncation takes place -- use lastleft's heap
     * TID.
     */
    ItemPointerCopy(BTreeTupleGetMaxHeapTID(lastleft), pivotheaptid);

    /*
     * We're done.  Assert!() that heap TID invariants hold before returning.
     */
    Assert!(
        ItemPointerCompare(
            BTreeTupleGetMaxHeapTID(lastleft),
            BTreeTupleGetHeapTID(firstright),
        ) < 0,
    );
    Assert!(
        ItemPointerCompare(pivotheaptid, BTreeTupleGetHeapTID(lastleft)) >= 0,
    );
    Assert!(
        ItemPointerCompare(pivotheaptid, BTreeTupleGetHeapTID(firstright)) < 0,
    );

    tidpivot
}

/*
 * _bt_keep_natts - how many key attributes to keep when truncating.
 */
pub unsafe fn _bt_keep_natts(
    rel: Relation,
    lastleft: IndexTuple,
    firstright: IndexTuple,
    itup_key: BTScanInsert,
) -> c_int {
    let nkeyatts: c_int = IndexRelationGetNumberOfKeyAttributes(rel);
    let itupdesc: TupleDesc = RelationGetDescr(rel);
    let mut keepnatts: c_int;
    let scankey: ScanKey;

    /*
     * _bt_compare() treats truncated key attributes as having the value minus
     * infinity, which would break searches within !heapkeyspace indexes.
     */
    if !(*itup_key).heapkeyspace {
        return nkeyatts;
    }

    scankey = (*itup_key).scankeys.as_mut_ptr();
    keepnatts = 1;
    let mut attnum: c_int = 1;
    while attnum <= nkeyatts {
        let mut datum1: Datum = 0;
        let mut datum2: Datum = 0;
        let mut isNull1: bool = false;
        let mut isNull2: bool = false;
        let sk: ScanKey = scankey.add((attnum - 1) as usize);

        datum1 = index_getattr(lastleft, attnum, itupdesc, &mut isNull1);
        datum2 = index_getattr(firstright, attnum, itupdesc, &mut isNull2);

        if isNull1 != isNull2 {
            break;
        }

        if !isNull1
            && DatumGetInt32(FunctionCall2Coll(
                &mut (*sk).sk_func,
                (*sk).sk_collation,
                datum1,
                datum2,
            )) != 0
        {
            break;
        }

        keepnatts += 1;
        attnum += 1;
    }

    /*
     * Assert that _bt_keep_natts_fast() agrees with us in passing.
     */
    Assert!(
        !(*itup_key).allequalimage
            || keepnatts == _bt_keep_natts_fast(rel, lastleft, firstright),
    );

    keepnatts
}

/*
 * _bt_keep_natts_fast - fast bitwise variant of _bt_keep_natts.
 *
 * This is exported so that a candidate split point can have its effect on
 * suffix truncation inexpensively evaluated ahead of time.
 */
pub unsafe fn _bt_keep_natts_fast(
    rel: Relation,
    lastleft: IndexTuple,
    firstright: IndexTuple,
) -> c_int {
    let itupdesc: TupleDesc = RelationGetDescr(rel);
    let keysz: c_int = IndexRelationGetNumberOfKeyAttributes(rel);
    let mut keepnatts: c_int;

    keepnatts = 1;
    let mut attnum: c_int = 1;
    while attnum <= keysz {
        let mut datum1: Datum = 0;
        let mut datum2: Datum = 0;
        let mut isNull1: bool = false;
        let mut isNull2: bool = false;
        let att: *mut CompactAttribute;

        datum1 = index_getattr(lastleft, attnum, itupdesc, &mut isNull1);
        datum2 = index_getattr(firstright, attnum, itupdesc, &mut isNull2);
        att = TupleDescCompactAttr(itupdesc, attnum - 1);

        if isNull1 != isNull2 {
            break;
        }

        if !isNull1 && !datum_image_eq(datum1, datum2, (*att).attbyval, (*att).attlen as _) {
            break;
        }

        keepnatts += 1;
        attnum += 1;
    }

    keepnatts
}

/*
 *  _bt_check_natts() -- Verify tuple has expected number of attributes.
 *
 * Returns value indicating if the expected number of attributes were found
 * for a particular offset on page.
 */
pub unsafe fn _bt_check_natts(
    rel: Relation,
    heapkeyspace: bool,
    page: Page,
    offnum: OffsetNumber,
) -> bool {
    let natts: int16 = IndexRelationGetNumberOfAttributes(rel) as int16;
    let nkeyatts: int16 = IndexRelationGetNumberOfKeyAttributes(rel) as int16;
    let opaque: BTPageOpaque = BTPageGetOpaque(page);
    let itup: IndexTuple;
    let tupnatts: c_int;

    /*
     * We cannot reliably test a deleted or half-dead page, since they have
     * dummy high keys
     */
    if P_IGNORE(opaque) {
        return true;
    }

    Assert!(offnum >= FirstOffsetNumber && offnum <= PageGetMaxOffsetNumber(page));

    itup = PageGetItem(page, PageGetItemId(page, offnum)) as IndexTuple;
    tupnatts = BTreeTupleGetNAtts(itup, rel);

    /* !heapkeyspace indexes do not support deduplication */
    if !heapkeyspace && BTreeTupleIsPosting(itup) {
        return false;
    }

    /* Posting list tuples should never have "pivot heap TID" bit set */
    if BTreeTupleIsPosting(itup)
        && (ItemPointerGetOffsetNumberNoCheck(&(*itup).t_tid) & BT_PIVOT_HEAP_TID_ATTR) != 0
    {
        return false;
    }

    /* INCLUDE indexes do not support deduplication */
    if natts != nkeyatts && BTreeTupleIsPosting(itup) {
        return false;
    }

    if P_ISLEAF(opaque) {
        if offnum >= P_FIRSTDATAKEY(opaque) {
            /*
             * Non-pivot tuple should never be explicitly marked as a pivot tuple
             */
            if BTreeTupleIsPivot(itup) {
                return false;
            }

            /*
             * Leaf tuples that are not the page high key (non-pivot tuples)
             * should never be truncated.
             */
            return tupnatts == natts as c_int;
        } else {
            /*
             * Rightmost page doesn't contain a page high key, so tuple was
             * checked above as ordinary leaf tuple
             */
            Assert!(!P_RIGHTMOST(opaque));

            /*
             * !heapkeyspace high key tuple contains only key attributes.
             */
            if !heapkeyspace {
                return tupnatts == nkeyatts as c_int;
            }

            /* Use generic heapkeyspace pivot tuple handling */
        }
    } else {
        /* !P_ISLEAF(opaque) */
        if offnum == P_FIRSTDATAKEY(opaque) {
            /*
             * The first tuple on any internal page is its negative infinity
             * tuple.  Negative infinity tuples are always truncated to zero
             * attributes.
             */
            if heapkeyspace {
                return tupnatts == 0;
            }

            /*
             * The number of attributes won't be explicitly represented if the
             * negative infinity tuple was generated during a page split that
             * occurred with a version of Postgres before v11.
             */
            return tupnatts == 0
                || ItemPointerGetOffsetNumber(&((*itup).t_tid)) == P_HIKEY;
        } else {
            /*
             * !heapkeyspace downlink tuple with separator key contains only
             * key attributes.
             */
            if !heapkeyspace {
                return tupnatts == nkeyatts as c_int;
            }

            /* Use generic heapkeyspace pivot tuple handling */
        }
    }

    /* Handle heapkeyspace pivot tuples (excluding minus infinity items) */
    Assert!(heapkeyspace);

    /*
     * Explicit representation of the number of attributes is mandatory with
     * heapkeyspace index pivot tuples.
     */
    if !BTreeTupleIsPivot(itup) {
        return false;
    }

    /* Pivot tuple should not use posting list representation (redundant) */
    if BTreeTupleIsPosting(itup) {
        return false;
    }

    /*
     * Heap TID is a tiebreaker key attribute, so it cannot be untruncated
     * when any other key attribute is truncated
     */
    if !BTreeTupleGetHeapTID(itup).is_null() && tupnatts != nkeyatts as c_int {
        return false;
    }

    /*
     * Pivot tuple must have at least one untruncated key attribute.
     */
    tupnatts > 0 && tupnatts <= nkeyatts as c_int
}

/*
 *
 *  _bt_check_third_page() -- check whether tuple fits on a btree page at all.
 *
 * We actually need to be able to fit three items on every page, so restrict
 * any one item to 1/3 the per-page available space.
 */
pub unsafe fn _bt_check_third_page(
    rel: Relation,
    heap: Relation,
    needheaptidspace: bool,
    page: Page,
    newtup: IndexTuple,
) {
    let itemsz: Size;
    let opaque: BTPageOpaque;

    itemsz = MAXALIGN(IndexTupleSize(newtup));

    /* Double check item size against limit */
    if itemsz <= BTMaxItemSize {
        return;
    }

    /*
     * Tuple is probably too large to fit on page, but it's possible that the
     * index uses version 2 or version 3, or that page is an internal page.
     */
    if !needheaptidspace && itemsz <= BTMaxItemSizeNoHeapTid {
        return;
    }

    /*
     * Internal page insertions cannot fail here.
     */
    opaque = BTPageGetOpaque(page);
    if !P_ISLEAF(opaque) {
        elog!(
            ERROR,
            "cannot insert oversized tuple of size {} on internal page of index \"{}\"",
            itemsz,
            core::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
        );
    }

    ereport!(
        ERROR,
        errmsg!(
            "index row size {} exceeds btree version {} maximum {} for index \"{}\"",
            itemsz,
            if needheaptidspace { BTREE_VERSION } else { BTREE_NOVAC_VERSION },
            if needheaptidspace { BTMaxItemSize } else { BTMaxItemSizeNoHeapTid },
            std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
        )
    );
    /* C also: errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
     * errdetail("Index row references tuple (%u,%u) in relation \"%s\".",
     *           ItemPointerGetBlockNumber/OffsetNumber(BTreeTupleGetHeapTID(newtup)),
     *           RelationGetRelationName(heap)),
     * errhint("Values larger than 1/3 of a buffer page cannot be indexed..."),
     * errtableconstraint(heap, RelationGetRelationName(rel)) */
}

// P_HIKEY constant from access/nbtree.h -- TODO(pg-port).
pub const P_HIKEY: OffsetNumber = 1;
// ERRCODE_PROGRAM_LIMIT_EXCEEDED -- TODO(pg-port): utils/errcodes.h.
pub const ERRCODE_PROGRAM_LIMIT_EXCEEDED: c_int = 0x54000; // placeholder

/*
 * Are all attributes in rel "equality is image equality" attributes?
 *
 * We use each attribute's BTEQUALIMAGE_PROC opclass procedure.
 */
pub unsafe fn _bt_allequalimage(rel: Relation, debugmessage: bool) -> bool {
    let mut allequalimage: bool = true;

    /* INCLUDE indexes can never support deduplication */
    if IndexRelationGetNumberOfAttributes(rel) != IndexRelationGetNumberOfKeyAttributes(rel) {
        return false;
    }

    let mut i: c_int = 0;
    while i < IndexRelationGetNumberOfKeyAttributes(rel) {
        let opfamily:  Oid = *(*rel).rd_opfamily.add(i as usize);
        let opcintype: Oid = *(*rel).rd_opcintype.add(i as usize);
        let collation: Oid = *(*rel).rd_indcollation.add(i as usize);
        let equalimageproc: Oid;

        equalimageproc = get_opfamily_proc(opfamily, opcintype, opcintype, BTEQUALIMAGE_PROC);

        /*
         * If there is no BTEQUALIMAGE_PROC then deduplication is assumed to
         * be unsafe.  Otherwise, actually call proc and see what it says.
         */
        if !OidIsValid(equalimageproc)
            || !DatumGetBool(OidFunctionCall1Coll(
                equalimageproc,
                collation,
                ObjectIdGetDatum(opcintype),
            ))
        {
            allequalimage = false;
            break;
        }
        i += 1;
    }

    if debugmessage {
        if allequalimage {
            elog!(
                DEBUG1,
                "index \"{}\" can safely use deduplication",
                core::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            );
        } else {
            elog!(
                DEBUG1,
                "index \"{}\" cannot use deduplication",
                core::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            );
        }
    }

    allequalimage
}

// End of Part 6 / end of nbtutils.rs.
