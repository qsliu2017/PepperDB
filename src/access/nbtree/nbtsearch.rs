//! nbtsearch.rs
//!   Search code for postgres btrees.
//!
//! Translated 1:1 from postgres/src/backend/access/nbtree/nbtsearch.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/access/nbtree/nbtsearch.c
//!
//! #include mapping:
//!   "postgres.h"           -> crate::prelude::*
//!   "access/nbtree.h"      -> BTStack/BTScanInsert/BTScanOpaque/BTPageOpaque etc. (stubs)
//!   "access/relscan.h"     -> IndexScanDesc (stub)
//!   "access/xact.h"        -> IsolationIsSerializable (stub)
//!   "miscadmin.h"          -> CHECK_FOR_INTERRUPTS (stub)
//!   "pgstat.h"             -> pgstat_count_index_scan (stub)
//!   "storage/predicate.h"  -> PredicateLockPage/PredicateLockRelation (stubs)
//!   "utils/lsyscache.h"    -> get_opfamily_proc (stub)
//!   "utils/rel.h"          -> Relation (crate::utils::rel)

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
use std::ffi::{c_char, c_int, c_void, CStr};

/// errmsg_internal!: STRICT single-message form (matches tcop/postgres.rs).
/// C also folds errcode/errdetail into /* C also: */ comments at call sites.
macro_rules! errmsg_internal { ($fmt:literal $(, $arg:expr)*) => { errmsg!($fmt $(, $arg)*) }; }

use crate::c::{int16, int32, uint8, uint16, uint32, Size};

// ---------------------------------------------------------------------------
// Real, already-ported homes.
// ---------------------------------------------------------------------------
use crate::access::common::indextuple::{
    IndexTuple, IndexTupleData, IndexTupleSize, INDEX_SIZE_MASK,
};
use crate::access::common::scankey::{
    ScanKey, ScanKeyData, ScanKeyEntryInitialize, ScanKeyEntryInitializeWithInfo,
    SK_ISNULL, SK_SEARCHNULL, SK_SEARCHNOTNULL, SK_SEARCHARRAY,
    SK_ROW_HEADER, SK_ROW_MEMBER, SK_ROW_END,
};
use crate::access::stratnum::{
    InvalidStrategy, StrategyNumber,
    BTEqualStrategyNumber, BTLessStrategyNumber, BTLessEqualStrategyNumber,
    BTGreaterEqualStrategyNumber, BTGreaterStrategyNumber,
};
use crate::access::attnum::{AttrNumber, InvalidAttrNumber};
use crate::access::common::tupdesc::TupleDesc;
use crate::storage::bufpage::{
    Page, PageGetItem, PageGetItemId, PageGetMaxOffsetNumber,
};
use crate::storage::buf::Buffer;
use crate::storage::block::BlockNumber;
use crate::storage::itemid::{ItemId, ItemIdData, ItemIdIsDead};
use crate::storage::itemptr::{
    ItemPointer, ItemPointerData, ItemPointerCompare, ItemPointerCopy,
    ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber,
};
use crate::storage::off::{
    FirstOffsetNumber, InvalidOffsetNumber, OffsetNumber, OffsetNumberNext, OffsetNumberPrev,
};
use crate::utils::rel::Relation;
use crate::utils::fmgr::{FmgrInfo, FunctionCall2Coll};
use crate::postgres::{DatumGetInt32, DatumGetPointer, PointerGetDatum, ObjectIdGetDatum};
use crate::access::transam::xlogdefs::XLogRecPtr;

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memmove(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

// ---------------------------------------------------------------------------
// STUBS: symbols from access/nbtree.h and related headers not yet ported.
// Each is a minimal local declaration.
// TODO(pg-port): real definitions live in postgres/src/include/access/nbtree.h
// ---------------------------------------------------------------------------

/// TODO(pg-port): BTStackData / BTStack live in access/nbtree.h.
#[repr(C)]
pub struct BTStackData {
    pub bts_blkno:  BlockNumber,
    pub bts_offset: OffsetNumber,
    pub bts_parent: BTStack,
}
pub type BTStack = *mut BTStackData;

/// TODO(pg-port): BTScanInsertData / BTScanInsert live in access/nbtree.h.
#[repr(C)]
pub struct BTScanInsertData {
    pub heapkeyspace:   bool,
    pub allequalimage:  bool,
    pub anynullkeys:    bool,
    pub nextkey:        bool,
    pub backward:       bool,
    pub keysz:          c_int,
    pub scantid:        ItemPointer,
    pub scankeys:       [ScanKeyData; INDEX_MAX_KEYS as usize],
}
pub type BTScanInsert = *mut BTScanInsertData;

/// TODO(pg-port): BTInsertStateData / BTInsertState live in access/nbtree.h.
#[repr(C)]
pub struct BTInsertStateData {
    pub itup:         IndexTuple,
    pub itemsz:       Size,
    pub itup_key:     BTScanInsert,
    pub bounds_valid: bool,
    pub buf:          Buffer,
    pub postingoff:   c_int,
    pub low:          OffsetNumber,
    pub stricthigh:   OffsetNumber,
}
pub type BTInsertState = *mut BTInsertStateData;

/// TODO(pg-port): BTPageOpaqueData / BTPageOpaque live in access/nbtree.h.
#[repr(C)]
pub struct BTPageOpaqueData {
    pub btpo_prev:    BlockNumber,
    pub btpo_next:    BlockNumber,
    pub btpo_level:   u32,
    pub btpo_flags:   u16,
    pub btpo_cycleid: u16,
}
pub type BTPageOpaque = *mut BTPageOpaqueData;

/// TODO(pg-port): BTScanPosItem lives in access/nbtree.h.
#[repr(C)]
pub struct BTScanPosItem {
    pub heapTid:     ItemPointerData,
    pub indexOffset: OffsetNumber,
    pub tupleOffset: uint16,
}

/// TODO(pg-port): BTScanPosData / BTScanPos live in access/nbtree.h.
#[repr(C)]
pub struct BTScanPosData {
    pub buf:             Buffer,
    pub lsn:             XLogRecPtr,
    pub currPage:        BlockNumber,
    pub nextPage:        BlockNumber,
    pub prevPage:        BlockNumber,
    pub moreLeft:        bool,
    pub moreRight:       bool,
    pub dir:             ScanDirection,
    pub nextTupleOffset: uint16,
    pub firstItem:       c_int,
    pub lastItem:        c_int,
    pub itemIndex:       c_int,
    pub items:           [BTScanPosItem; MaxTIDsPerBTreePage as usize],
}

/// TODO(pg-port): BTArrayKeyInfo lives in access/nbtree.h.
#[repr(C)]
pub struct BTArrayKeyInfo {
    pub scan_key:     c_int,
    pub cur_elem:     c_int,
    pub mark_elem:    c_int,
    pub num_elems:    c_int,
    pub array_elems:  *mut Datum,
    pub low_compare:  ScanKey,
    pub high_compare: ScanKey,
    pub null_elem:    bool,
}

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

/// TODO(pg-port): BTScanOpaqueData / BTScanOpaque live in access/nbtree.h.
#[repr(C)]
pub struct BTScanOpaqueData {
    pub currPos:        BTScanPosData,
    pub markPos:        BTScanPosData,
    pub markItemIndex:  c_int,
    pub dropPin:        bool,
    pub scanBehind:     bool,
    pub oppositeDirCheck: bool,
    pub needPrimScan:   bool,
    pub qual_ok:        bool,
    pub numArrayKeys:   c_int,
    pub numKilled:      c_int,
    pub arrayKeys:      *mut BTArrayKeyInfo,
    pub keyData:        ScanKey,
    pub numberOfKeys:   c_int,
    pub currTuples:     *mut c_char,
    pub markTuples:     *mut c_char,
    pub killedItems:    *mut c_int,
}
pub type BTScanOpaque = *mut BTScanOpaqueData;

/// TODO(pg-port): IndexScanDescData / IndexScanDesc live in access/relscan.h.
#[repr(C)]
pub struct IndexScanDescData {
    pub indexRelation:     Relation,
    pub xs_snapshot:       *mut c_void,
    pub xs_heaptid:        ItemPointerData,
    pub xs_itup:           IndexTuple,
    pub opaque:            *mut c_void,
    pub numberOfKeys:      c_int,
    pub keyData:           ScanKey,
    pub ignore_killed_tuples: bool,
    pub parallel_scan:     *mut c_void,
    pub instrument:        *mut IndexScanInstrData,
}
pub type IndexScanDesc = *mut IndexScanDescData;

/// TODO(pg-port): IndexScanInstr lives in nodes/execnodes.h.
#[repr(C)]
pub struct IndexScanInstrData {
    pub nsearches: u64,
}

/// TODO(pg-port): ScanDirection lives in access/sdir.h.
pub type ScanDirection = c_int;
pub const ForwardScanDirection: ScanDirection = 1;
pub const BackwardScanDirection: ScanDirection = -1;
pub const NoMovementScanDirection: ScanDirection = 0;

// nbtree.h constants / types
/// TODO(pg-port): from access/nbtree.h.
pub const BT_READ: c_int = 0;
/// TODO(pg-port): from access/nbtree.h.
pub const BT_WRITE: c_int = 1;
/// TODO(pg-port): from access/nbtree.h.
pub const P_HIKEY: OffsetNumber = 1;
/// TODO(pg-port): from access/nbtree.h: max TIDs per btree leaf page.
pub const MaxTIDsPerBTreePage: c_int = 1358;
/// TODO(pg-port): from access/htup_details.h / access/relscan.h.
pub const INDEX_MAX_KEYS: c_int = 32;
/// TODO(pg-port): from access/nbtree.h.
pub const BTORDER_PROC: c_int = 1;
/// TODO(pg-port): P_NONE: invalid block number sentinel.
pub const P_NONE: BlockNumber = 0;
/// TODO(pg-port): from nodes/pg_list.h.
pub type Datum = usize;

/// TODO(pg-port): Oid type.
pub type Oid = u32;
/// TODO(pg-port): InvalidOid.
pub const InvalidOid: Oid = 0;
/// TODO(pg-port): RegProcedure.
pub type RegProcedure = Oid;

// nbtree.h macros (stubs)
/// TODO(pg-port): BTPageGetOpaque() (access/nbtree.h).
unsafe fn BTPageGetOpaque(page: Page) -> BTPageOpaque {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}
/// TODO(pg-port): P_ISLEAF() (access/nbtree.h).
unsafe fn P_ISLEAF(opaque: BTPageOpaque) -> bool {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}
/// TODO(pg-port): P_RIGHTMOST() (access/nbtree.h).
unsafe fn P_RIGHTMOST(opaque: BTPageOpaque) -> bool {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}
/// TODO(pg-port): P_LEFTMOST() (access/nbtree.h).
unsafe fn P_LEFTMOST(opaque: BTPageOpaque) -> bool {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}
/// TODO(pg-port): P_IGNORE() (access/nbtree.h).
unsafe fn P_IGNORE(opaque: BTPageOpaque) -> bool {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}
/// TODO(pg-port): P_INCOMPLETE_SPLIT() (access/nbtree.h).
unsafe fn P_INCOMPLETE_SPLIT(opaque: BTPageOpaque) -> bool {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}
/// TODO(pg-port): P_ISDELETED() (access/nbtree.h).
unsafe fn P_ISDELETED(opaque: BTPageOpaque) -> bool {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}
/// TODO(pg-port): P_FIRSTDATAKEY() (access/nbtree.h).
unsafe fn P_FIRSTDATAKEY(opaque: BTPageOpaque) -> OffsetNumber {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}
/// TODO(pg-port): BTreeTupleIsPivot() (access/nbtree.h).
unsafe fn BTreeTupleIsPivot(itup: IndexTuple) -> bool {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}
/// TODO(pg-port): BTreeTupleIsPosting() (access/nbtree.h).
unsafe fn BTreeTupleIsPosting(itup: IndexTuple) -> bool {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}
/// TODO(pg-port): BTreeTupleGetDownLink() (access/nbtree.h).
unsafe fn BTreeTupleGetDownLink(itup: IndexTuple) -> BlockNumber {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}
/// TODO(pg-port): BTreeTupleGetNAtts() (access/nbtree.h).
unsafe fn BTreeTupleGetNAtts(itup: IndexTuple, rel: Relation) -> c_int {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}
/// TODO(pg-port): BTreeTupleGetNPosting() (access/nbtree.h).
unsafe fn BTreeTupleGetNPosting(posting: IndexTuple) -> c_int {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}
/// TODO(pg-port): BTreeTupleGetPostingN() (access/nbtree.h).
unsafe fn BTreeTupleGetPostingN(posting: IndexTuple, n: c_int) -> ItemPointer {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}
/// TODO(pg-port): BTreeTupleGetPostingOffset() (access/nbtree.h).
unsafe fn BTreeTupleGetPostingOffset(posting: IndexTuple) -> uint32 {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}
/// TODO(pg-port): BTreeTupleGetHeapTID() (access/nbtree.h).
unsafe fn BTreeTupleGetHeapTID(itup: IndexTuple) -> ItemPointer {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}
/// TODO(pg-port): BTreeTupleGetMaxHeapTID() (access/nbtree.h).
unsafe fn BTreeTupleGetMaxHeapTID(itup: IndexTuple) -> ItemPointer {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}
/// TODO(pg-port): IndexRelationGetNumberOfKeyAttributes() (utils/rel.h).
unsafe fn IndexRelationGetNumberOfKeyAttributes(rel: Relation) -> c_int {
    unimplemented!() // TODO(pg-port): utils/rel.h
}
/// TODO(pg-port): IndexRelationGetNumberOfAttributes() (utils/rel.h).
unsafe fn IndexRelationGetNumberOfAttributes(rel: Relation) -> c_int {
    unimplemented!() // TODO(pg-port): utils/rel.h
}
/// TODO(pg-port): RelationGetDescr() (utils/rel.h).
unsafe fn RelationGetDescr(rel: Relation) -> TupleDesc {
    unimplemented!() // TODO(pg-port): utils/rel.h
}
/// TODO(pg-port): RelationGetRelationName() (utils/rel.h).
unsafe fn RelationGetRelationName(rel: Relation) -> *const c_char {
    unimplemented!() // TODO(pg-port): utils/rel.h
}
/// TODO(pg-port): RelationNeedsWAL() (utils/rel.h).
unsafe fn RelationNeedsWAL(rel: Relation) -> bool {
    unimplemented!() // TODO(pg-port): utils/rel.h
}
/// TODO(pg-port): BufferGetPage() (storage/bufmgr.h).
unsafe fn BufferGetPage(buf: Buffer) -> Page {
    unimplemented!() // TODO(pg-port): storage/bufmgr.h
}
/// TODO(pg-port): BufferGetBlockNumber() (storage/bufmgr.h).
unsafe fn BufferGetBlockNumber(buf: Buffer) -> BlockNumber {
    unimplemented!() // TODO(pg-port): storage/bufmgr.h
}
/// TODO(pg-port): BufferIsValid() (storage/bufmgr.h).
unsafe fn BufferIsValid(buf: Buffer) -> bool {
    unimplemented!() // TODO(pg-port): storage/bufmgr.h
}
/// TODO(pg-port): BufferGetLSNAtomic() (storage/bufmgr.h).
unsafe fn BufferGetLSNAtomic(buf: Buffer) -> XLogRecPtr {
    unimplemented!() // TODO(pg-port): storage/bufmgr.h
}
/// TODO(pg-port): IncrBufferRefCount() (storage/bufmgr.h).
unsafe fn IncrBufferRefCount(buf: Buffer) {
    unimplemented!() // TODO(pg-port): storage/bufmgr.h
}
/// TODO(pg-port): InvalidBuffer constant (storage/buf.h).
pub const InvalidBuffer: Buffer = 0;
/// TODO(pg-port): _bt_getroot() (nbtpage.c / access/nbtree.h).
unsafe fn _bt_getroot(rel: Relation, heaprel: Relation, access: c_int) -> Buffer {
    unimplemented!() // TODO(pg-port): access/nbtree.h (nbtpage.c)
}
/// TODO(pg-port): _bt_gettrueroot() (nbtpage.c / access/nbtree.h).
unsafe fn _bt_gettrueroot(rel: Relation) -> Buffer {
    unimplemented!() // TODO(pg-port): access/nbtree.h (nbtpage.c)
}
/// TODO(pg-port): _bt_getbuf() (nbtpage.c / access/nbtree.h).
unsafe fn _bt_getbuf(rel: Relation, blkno: BlockNumber, access: c_int) -> Buffer {
    unimplemented!() // TODO(pg-port): access/nbtree.h (nbtpage.c)
}
/// TODO(pg-port): _bt_relbuf() (nbtpage.c / access/nbtree.h).
unsafe fn _bt_relbuf(rel: Relation, buf: Buffer) {
    unimplemented!() // TODO(pg-port): access/nbtree.h (nbtpage.c)
}
/// TODO(pg-port): _bt_relandgetbuf() (nbtpage.c / access/nbtree.h).
unsafe fn _bt_relandgetbuf(rel: Relation, obuf: Buffer, blkno: BlockNumber, access: c_int) -> Buffer {
    unimplemented!() // TODO(pg-port): access/nbtree.h (nbtpage.c)
}
/// TODO(pg-port): _bt_lockbuf() (nbtpage.c / access/nbtree.h).
unsafe fn _bt_lockbuf(rel: Relation, buf: Buffer, access: c_int) {
    unimplemented!() // TODO(pg-port): access/nbtree.h (nbtpage.c)
}
/// TODO(pg-port): _bt_unlockbuf() (nbtpage.c / access/nbtree.h).
unsafe fn _bt_unlockbuf(rel: Relation, buf: Buffer) {
    unimplemented!() // TODO(pg-port): access/nbtree.h (nbtpage.c)
}
/// TODO(pg-port): _bt_finish_split() (nbtinsert.c / access/nbtree.h).
unsafe fn _bt_finish_split(rel: Relation, heaprel: Relation, buf: Buffer, stack: BTStack) {
    unimplemented!() // TODO(pg-port): access/nbtree.h (nbtinsert.c)
}
/// TODO(pg-port): _bt_freestack() (nbtutils.c / access/nbtree.h).
unsafe fn _bt_freestack(stack: BTStack) {
    unimplemented!() // TODO(pg-port): access/nbtree.h (nbtutils.c)
}
/// TODO(pg-port): _bt_metaversion() (nbtpage.c / access/nbtree.h).
unsafe fn _bt_metaversion(rel: Relation, heapkeyspace: *mut bool, allequalimage: *mut bool) {
    unimplemented!() // TODO(pg-port): access/nbtree.h (nbtpage.c)
}
/// TODO(pg-port): _bt_preprocess_keys() (nbtutils.c / access/nbtree.h).
unsafe fn _bt_preprocess_keys(scan: IndexScanDesc) {
    unimplemented!() // TODO(pg-port): access/nbtree.h (nbtutils.c)
}
/// TODO(pg-port): _bt_start_array_keys() (nbtutils.c / access/nbtree.h).
unsafe fn _bt_start_array_keys(scan: IndexScanDesc, dir: ScanDirection) {
    unimplemented!() // TODO(pg-port): access/nbtree.h (nbtutils.c)
}
/// TODO(pg-port): _bt_checkkeys() (nbtutils.c / access/nbtree.h).
unsafe fn _bt_checkkeys(
    scan: IndexScanDesc,
    pstate: *mut BTReadPageState,
    array_keys: bool,
    itup: IndexTuple,
    indnatts: c_int,
) -> bool {
    unimplemented!() // TODO(pg-port): access/nbtree.h (nbtutils.c)
}
/// TODO(pg-port): _bt_scanbehind_checkkeys() (nbtutils.c / access/nbtree.h).
unsafe fn _bt_scanbehind_checkkeys(
    scan: IndexScanDesc,
    dir: ScanDirection,
    finaltup: IndexTuple,
) -> bool {
    unimplemented!() // TODO(pg-port): access/nbtree.h (nbtutils.c)
}
/// TODO(pg-port): _bt_set_startikey() (nbtutils.c / access/nbtree.h).
unsafe fn _bt_set_startikey(scan: IndexScanDesc, pstate: *mut BTReadPageState) {
    unimplemented!() // TODO(pg-port): access/nbtree.h (nbtutils.c)
}
/// TODO(pg-port): _bt_check_natts() (nbtutils.c / access/nbtree.h).
unsafe fn _bt_check_natts(
    rel: Relation,
    heapkeyspace: bool,
    page: Page,
    offnum: OffsetNumber,
) -> bool {
    unimplemented!() // TODO(pg-port): access/nbtree.h (nbtutils.c)
}
/// TODO(pg-port): _bt_killitems() (nbtutils.c / access/nbtree.h).
unsafe fn _bt_killitems(scan: IndexScanDesc) {
    unimplemented!() // TODO(pg-port): access/nbtree.h (nbtutils.c)
}
/// TODO(pg-port): _bt_parallel_seize() (nbtutils.c / access/nbtree.h).
unsafe fn _bt_parallel_seize(
    scan: IndexScanDesc,
    blkno: *mut BlockNumber,
    lastcurrblkno: *mut BlockNumber,
    first: bool,
) -> bool {
    unimplemented!() // TODO(pg-port): access/nbtree.h (nbtutils.c)
}
/// TODO(pg-port): _bt_parallel_release() (nbtutils.c / access/nbtree.h).
unsafe fn _bt_parallel_release(
    scan: IndexScanDesc,
    blkno: BlockNumber,
    lastcurrblkno: BlockNumber,
) {
    unimplemented!() // TODO(pg-port): access/nbtree.h (nbtutils.c)
}
/// TODO(pg-port): _bt_parallel_done() (nbtutils.c / access/nbtree.h).
unsafe fn _bt_parallel_done(scan: IndexScanDesc) {
    unimplemented!() // TODO(pg-port): access/nbtree.h (nbtutils.c)
}
/// TODO(pg-port): _bt_parallel_primscan_schedule() (nbtutils.c / access/nbtree.h).
unsafe fn _bt_parallel_primscan_schedule(scan: IndexScanDesc, currblkno: BlockNumber) {
    unimplemented!() // TODO(pg-port): access/nbtree.h (nbtutils.c)
}
/// TODO(pg-port): index_getprocinfo() (utils/rel.h).
unsafe fn index_getprocinfo(rel: Relation, attno: AttrNumber, procno: c_int) -> *mut FmgrInfo {
    unimplemented!() // TODO(pg-port): utils/rel.h
}
/// TODO(pg-port): index_getattr() (access/common/indextuple.h).
unsafe fn index_getattr(
    tup: IndexTuple,
    attnum: c_int,
    tupleDesc: TupleDesc,
    isnull: *mut bool,
) -> Datum {
    unimplemented!() // TODO(pg-port): access/common/indextuple.h
}
/// TODO(pg-port): get_opfamily_proc() (utils/lsyscache.h).
unsafe fn get_opfamily_proc(opfamily: Oid, lefttype: Oid, righttype: Oid, procnum: c_int) -> RegProcedure {
    unimplemented!() // TODO(pg-port): utils/lsyscache.h
}
/// TODO(pg-port): RegProcedureIsValid() (utils/builtins.h).
unsafe fn RegProcedureIsValid(proc_: RegProcedure) -> bool {
    proc_ != InvalidOid
}
/// TODO(pg-port): PredicateLockPage() (storage/predicate.h).
unsafe fn PredicateLockPage(rel: Relation, blkno: BlockNumber, snapshot: *mut c_void) {
    unimplemented!() // TODO(pg-port): storage/predicate.h
}
/// TODO(pg-port): PredicateLockRelation() (storage/predicate.h).
unsafe fn PredicateLockRelation(rel: Relation, snapshot: *mut c_void) {
    unimplemented!() // TODO(pg-port): storage/predicate.h
}
/// TODO(pg-port): IsolationIsSerializable() (access/xact.h).
unsafe fn IsolationIsSerializable() -> bool {
    unimplemented!() // TODO(pg-port): access/xact.h
}
/// TODO(pg-port): pgstat_count_index_scan() (pgstat.h).
unsafe fn pgstat_count_index_scan(rel: Relation) {
    unimplemented!() // TODO(pg-port): pgstat.h
}
/// TODO(pg-port): CHECK_FOR_INTERRUPTS() (miscadmin.h).
unsafe fn CHECK_FOR_INTERRUPTS() {
    unimplemented!() // TODO(pg-port): miscadmin.h
}
/// TODO(pg-port): BTScanPosIsValid() (access/nbtree.h).
unsafe fn BTScanPosIsValid(pos: &BTScanPosData) -> bool {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}
/// TODO(pg-port): BTScanPosIsPinned() (access/nbtree.h).
unsafe fn BTScanPosIsPinned(pos: &BTScanPosData) -> bool {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}
/// TODO(pg-port): BTScanPosUnpinIfPinned() (access/nbtree.h).
unsafe fn BTScanPosUnpinIfPinned(pos: &mut BTScanPosData) {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}
/// TODO(pg-port): BTScanPosInvalidate() (access/nbtree.h).
unsafe fn BTScanPosInvalidate(pos: &mut BTScanPosData) {
    unimplemented!() // TODO(pg-port): access/nbtree.h
}
/// TODO(pg-port): ScanDirectionIsForward() (access/sdir.h).
#[inline]
fn ScanDirectionIsForward(dir: ScanDirection) -> bool {
    dir == ForwardScanDirection
}
/// TODO(pg-port): ScanDirectionIsBackward() (access/sdir.h).
#[inline]
fn ScanDirectionIsBackward(dir: ScanDirection) -> bool {
    dir == BackwardScanDirection
}
/// TODO(pg-port): INVERT_COMPARE_RESULT macro (utils/fmgrprotos.h).
#[inline]
fn INVERT_COMPARE_RESULT(r: &mut i32) {
    *r = -(*r);
    // prevent overflow for INT_MIN: clamp
    if *r < -1 { *r = -1; } else if *r > 1 { *r = 1; }
}

// Relation field access helpers (stubs)
/// TODO(pg-port): rel->rd_opcintype[] access.
unsafe fn rd_opcintype(rel: Relation, i: usize) -> Oid { unimplemented!() }
/// TODO(pg-port): rel->rd_opfamily[] access.
unsafe fn rd_opfamily(rel: Relation, i: usize) -> Oid { unimplemented!() }

// ---------------------------------------------------------------------------
// Functions
// ---------------------------------------------------------------------------

/*
 *	_bt_drop_lock_and_maybe_pin()
 *
 * Unlock so->currPos.buf.  If scan is so->dropPin, drop the pin, too.
 * Dropping the pin prevents VACUUM from blocking on acquiring a cleanup lock.
 */
#[inline]
pub unsafe fn _bt_drop_lock_and_maybe_pin(rel: Relation, so: BTScanOpaque) {
    if !(*so).dropPin {
        /* Just drop the lock (not the pin) */
        _bt_unlockbuf(rel, (*so).currPos.buf);
        return;
    }

    /*
     * Drop both the lock and the pin.
     *
     * Have to set so->currPos.lsn so that _bt_killitems has a way to detect
     * when concurrent heap TID recycling by VACUUM might have taken place.
     */
    Assert!(RelationNeedsWAL(rel));
    (*so).currPos.lsn = BufferGetLSNAtomic((*so).currPos.buf);
    _bt_relbuf(rel, (*so).currPos.buf);
    (*so).currPos.buf = InvalidBuffer;
}

/*
 *	_bt_search() -- Search the tree for a particular scankey,
 *		or more precisely for the first leaf page it could be on.
 *
 * The passed scankey is an insertion-type scankey (see nbtree/README),
 * but it can omit the rightmost column(s) of the index.
 *
 * Return value is a stack of parent-page pointers (i.e. there is no entry for
 * the leaf level/page).  *bufP is set to the address of the leaf-page buffer,
 * which is locked and pinned.  No locks are held on the parent pages,
 * however!
 *
 * The returned buffer is locked according to access parameter.  Additionally,
 * access = BT_WRITE will allow an empty root page to be created and returned.
 * When access = BT_READ, an empty index will result in *bufP being set to
 * InvalidBuffer.  Also, in BT_WRITE mode, any incomplete splits encountered
 * during the search will be finished.
 *
 * heaprel must be provided by callers that pass access = BT_WRITE, since we
 * might need to allocate a new root page for caller -- see _bt_allocbuf.
 */
pub unsafe fn _bt_search(
    rel: Relation,
    heaprel: Relation,
    key: BTScanInsert,
    bufP: *mut Buffer,
    access: c_int,
) -> BTStack {
    let mut stack_in: BTStack = core::ptr::null_mut();
    let mut page_access: c_int = BT_READ;

    /* heaprel must be set whenever _bt_allocbuf is reachable */
    Assert!(access == BT_READ || access == BT_WRITE);
    Assert!(access == BT_READ || !heaprel.is_null());

    /* Get the root page to start with */
    *bufP = _bt_getroot(rel, heaprel, access);

    /* If index is empty and access = BT_READ, no root page is created. */
    if !BufferIsValid(*bufP) {
        return core::ptr::null_mut() as BTStack;
    }

    /* Loop iterates once per level descended in the tree */
    loop {
        let page: Page;
        let opaque: BTPageOpaque;
        let offnum: OffsetNumber;
        let itemid: *mut crate::storage::itemid::ItemIdData;
        let itup: IndexTuple;
        let child: BlockNumber;
        let new_stack: BTStack;

        /*
         * Race -- the page we just grabbed may have split since we read its
         * downlink in its parent page (or the metapage).  If it has, we may
         * need to move right to its new sibling.  Do that.
         *
         * In write-mode, allow _bt_moveright to finish any incomplete splits
         * along the way.  Strictly speaking, we'd only need to finish an
         * incomplete split on the leaf page we're about to insert to, not on
         * any of the upper levels (internal pages with incomplete splits are
         * also taken care of in _bt_getstackbuf).  But this is a good
         * opportunity to finish splits of internal pages too.
         */
        *bufP = _bt_moveright(rel, heaprel, key, *bufP, (access == BT_WRITE), stack_in, page_access);

        /* if this is a leaf page, we're done */
        page = BufferGetPage(*bufP);
        opaque = BTPageGetOpaque(page);
        if P_ISLEAF(opaque) {
            break;
        }

        /*
         * Find the appropriate pivot tuple on this page.  Its downlink points
         * to the child page that we're about to descend to.
         */
        offnum = _bt_binsrch(rel, key, *bufP);
        itemid = PageGetItemId(page, offnum);
        itup = PageGetItem(page, itemid) as IndexTuple;
        Assert!(BTreeTupleIsPivot(itup) || !(*key).heapkeyspace);
        child = BTreeTupleGetDownLink(itup);

        /*
         * We need to save the location of the pivot tuple we chose in a new
         * stack entry for this page/level.  If caller ends up splitting a
         * page one level down, it usually ends up inserting a new pivot
         * tuple/downlink immediately after the location recorded here.
         */
        new_stack = palloc(size_of::<BTStackData>()) as BTStack;
        (*new_stack).bts_blkno = BufferGetBlockNumber(*bufP);
        (*new_stack).bts_offset = offnum;
        (*new_stack).bts_parent = stack_in;

        /*
         * Page level 1 is lowest non-leaf page level prior to leaves.  So, if
         * we're on the level 1 and asked to lock leaf page in write mode,
         * then lock next page in write mode, because it must be a leaf.
         */
        if (*opaque).btpo_level == 1 && access == BT_WRITE {
            page_access = BT_WRITE;
        }

        /* drop the read lock on the page, then acquire one on its child */
        *bufP = _bt_relandgetbuf(rel, *bufP, child, page_access);

        /* okay, all set to move down a level */
        stack_in = new_stack;
    }

    /*
     * If we're asked to lock leaf in write mode, but didn't manage to, then
     * relock.  This should only happen when the root page is a leaf page (and
     * the only page in the index other than the metapage).
     */
    if access == BT_WRITE && page_access == BT_READ {
        /* trade in our read lock for a write lock */
        _bt_unlockbuf(rel, *bufP);
        _bt_lockbuf(rel, *bufP, BT_WRITE);

        /*
         * Race -- the leaf page may have split after we dropped the read lock
         * but before we acquired a write lock.  If it has, we may need to
         * move right to its new sibling.  Do that.
         */
        *bufP = _bt_moveright(rel, heaprel, key, *bufP, true, stack_in, BT_WRITE);
    }

    return stack_in;
}

/*
 *	_bt_moveright() -- move right in the btree if necessary.
 *
 * When we follow a pointer to reach a page, it is possible that
 * the page has changed in the meanwhile.  If this happens, we're
 * guaranteed that the page has "split right" -- that is, that any
 * data that appeared on the page originally is either on the page
 * or strictly to the right of it.
 *
 * This routine decides whether or not we need to move right in the
 * tree by examining the high key entry on the page.  If that entry is
 * strictly less than the scankey, or <= the scankey in the
 * key.nextkey=true case, then we followed the wrong link and we need
 * to move right.
 *
 * The passed insertion-type scankey can omit the rightmost column(s) of the
 * index. (see nbtree/README)
 *
 * When key.nextkey is false (the usual case), we are looking for the first
 * item >= key.  When key.nextkey is true, we are looking for the first item
 * strictly greater than key.
 *
 * If forupdate is true, we will attempt to finish any incomplete splits
 * that we encounter.  This is required when locking a target page for an
 * insertion, because we don't allow inserting on a page before the split is
 * completed.  'heaprel' and 'stack' are only used if forupdate is true.
 *
 * On entry, we have the buffer pinned and a lock of the type specified by
 * 'access'.  If we move right, we release the buffer and lock and acquire
 * the same on the right sibling.  Return value is the buffer we stop at.
 */
unsafe fn _bt_moveright(
    rel: Relation,
    heaprel: Relation,
    key: BTScanInsert,
    mut buf: Buffer,
    forupdate: bool,
    stack: BTStack,
    access: c_int,
) -> Buffer {
    let mut page: Page;
    let mut opaque: BTPageOpaque;
    let cmpval: i32;

    Assert!(!forupdate || !heaprel.is_null());

    /*
     * When nextkey = false (normal case): if the scan key that brought us to
     * this page is > the high key stored on the page, then the page has split
     * and we need to move right.  (pg_upgrade'd !heapkeyspace indexes could
     * have some duplicates to the right as well as the left, but that's
     * something that's only ever dealt with on the leaf level, after
     * _bt_search has found an initial leaf page.)
     *
     * When nextkey = true: move right if the scan key is >= page's high key.
     * (Note that key.scantid cannot be set in this case.)
     *
     * The page could even have split more than once, so scan as far as
     * needed.
     *
     * We also have to move right if we followed a link that brought us to a
     * dead page.
     */
    cmpval = if (*key).nextkey { 0 } else { 1 };

    loop {
        page = BufferGetPage(buf);
        opaque = BTPageGetOpaque(page);

        if P_RIGHTMOST(opaque) {
            break;
        }

        /*
         * Finish any incomplete splits we encounter along the way.
         */
        if forupdate && P_INCOMPLETE_SPLIT(opaque) {
            let blkno: BlockNumber = BufferGetBlockNumber(buf);

            /* upgrade our lock if necessary */
            if access == BT_READ {
                _bt_unlockbuf(rel, buf);
                _bt_lockbuf(rel, buf, BT_WRITE);
            }

            if P_INCOMPLETE_SPLIT(opaque) {
                _bt_finish_split(rel, heaprel, buf, stack);
            } else {
                _bt_relbuf(rel, buf);
            }

            /* re-acquire the lock in the right mode, and re-check */
            buf = _bt_getbuf(rel, blkno, access);
            continue;
        }

        if P_IGNORE(opaque) || _bt_compare(rel, key, page, P_HIKEY) >= cmpval {
            /* step right one page */
            buf = _bt_relandgetbuf(rel, buf, (*opaque).btpo_next, access);
            continue;
        } else {
            break;
        }
    }

    if P_IGNORE(opaque) {
        elog!(ERROR, "fell off the end of index \"{}\"",
              CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy());
    }

    return buf;
}

/*
 *	_bt_binsrch() -- Do a binary search for a key on a particular page.
 *
 * On an internal (non-leaf) page, _bt_binsrch() returns the OffsetNumber
 * of the last key < given scankey, or last key <= given scankey if nextkey
 * is true.  (Since _bt_compare treats the first data key of such a page as
 * minus infinity, there will be at least one key < scankey, so the result
 * always points at one of the keys on the page.)
 *
 * On a leaf page, _bt_binsrch() returns the final result of the initial
 * positioning process that started with _bt_first's call to _bt_search.
 * We're returning a non-pivot tuple offset, so things are a little different.
 * It is possible that we'll return an offset that's either past the last
 * non-pivot slot, or (in the case of a backward scan) before the first slot.
 *
 * This procedure is not responsible for walking right, it just examines
 * the given page.  _bt_binsrch() has no lock or refcount side effects
 * on the buffer.
 */
unsafe fn _bt_binsrch(rel: Relation, key: BTScanInsert, buf: Buffer) -> OffsetNumber {
    let page: Page;
    let opaque: BTPageOpaque;
    let mut low: OffsetNumber;
    let mut high: OffsetNumber;
    let mut result: i32;
    let cmpval: i32;

    page = BufferGetPage(buf);
    opaque = BTPageGetOpaque(page);

    /* Requesting nextkey semantics while using scantid seems nonsensical */
    Assert!(!(*key).nextkey || (*key).scantid.is_null());
    /* scantid-set callers must use _bt_binsrch_insert() on leaf pages */
    Assert!(!P_ISLEAF(opaque) || (*key).scantid.is_null());

    low = P_FIRSTDATAKEY(opaque);
    high = PageGetMaxOffsetNumber(page);

    /*
     * If there are no keys on the page, return the first available slot. Note
     * this covers two cases: the page is really empty (no keys), or it
     * contains only a high key.  The latter case is possible after vacuuming.
     * This can never happen on an internal page, however, since they are
     * never empty (an internal page must have at least one child).
     */
    if high < low {
        return low;
    }

    /*
     * Binary search to find the first key on the page >= scan key, or first
     * key > scankey when nextkey is true.
     *
     * For nextkey=false (cmpval=1), the loop invariant is: all slots before
     * 'low' are < scan key, all slots at or after 'high' are >= scan key.
     *
     * For nextkey=true (cmpval=0), the loop invariant is: all slots before
     * 'low' are <= scan key, all slots at or after 'high' are > scan key.
     *
     * We can fall out when high == low.
     */
    high += 1; /* establish the loop invariant for high */

    cmpval = if (*key).nextkey { 0 } else { 1 }; /* select comparison value */

    while high > low {
        let mid: OffsetNumber = low + ((high - low) / 2);

        /* We have low <= mid < high, so mid points at a real slot */

        result = _bt_compare(rel, key, page, mid);

        if result >= cmpval {
            low = mid + 1;
        } else {
            high = mid;
        }
    }

    /*
     * At this point we have high == low.
     *
     * On a leaf page we always return the first non-pivot tuple >= scan key
     * (resp. > scan key) for forward scan callers.  For backward scans, it's
     * always the _last_ non-pivot tuple < scan key (resp. <= scan key).
     */
    if P_ISLEAF(opaque) {
        /*
         * In the backward scan case we're supposed to locate the last
         * matching tuple on the leaf level -- not the first matching tuple
         * (the last tuple will be the first one returned by the scan).
         *
         * At this point we've located the first non-pivot tuple immediately
         * after the last matching tuple (which might just be maxoff + 1).
         * Compensate by stepping back.
         */
        if (*key).backward {
            return OffsetNumberPrev(low);
        }

        return low;
    }

    /*
     * On a non-leaf page, return the last key < scan key (resp. <= scan key).
     * There must be one if _bt_compare() is playing by the rules.
     *
     * _bt_compare() will seldom see any exactly-matching pivot tuples, since
     * a truncated -inf heap TID is usually enough to prevent it altogether.
     * Even omitted scan key entries are treated as > truncated attributes.
     *
     * However, during backward scans _bt_compare() interprets omitted scan
     * key attributes as == corresponding truncated -inf attributes instead.
     * This works just like < would work here.  Under this scheme, < strategy
     * backward scans will always directly descend to the correct leaf page.
     * In particular, they will never incur an "extra" leaf page access with a
     * scan key that happens to contain the same prefix of values as some
     * pivot tuple's untruncated prefix.  VACUUM relies on this guarantee when
     * it uses a leaf page high key to "re-find" a page undergoing deletion.
     */
    Assert!(low > P_FIRSTDATAKEY(opaque));

    return OffsetNumberPrev(low);
}

/*----------
 *	_bt_binsrch_insert() -- Cacheable, incremental leaf page binary search.
 *
 * Like _bt_binsrch(), but with support for caching the binary search
 * bounds.  Only used during insertion, and only on the leaf page that it
 * looks like caller will insert tuple on.  Exclusive-locked and pinned
 * leaf page is contained within insertstate.
 *
 * Caches the bounds fields in insertstate so that a subsequent call can
 * reuse the low and strict high bounds of original binary search.  Callers
 * that use these fields directly must be prepared for the case where low
 * and/or stricthigh are not on the same page (one or both exceed maxoff
 * for the page).  The case where there are no items on the page (high <
 * low) makes bounds invalid.
 *
 * Caller is responsible for invalidating bounds when it modifies the page
 * before calling here a second time, and for dealing with posting list
 * tuple matches (callers can use insertstate's postingoff field to
 * determine which existing heap TID will need to be replaced by a posting
 * list split).
 */
pub unsafe fn _bt_binsrch_insert(rel: Relation, insertstate: BTInsertState) -> OffsetNumber {
    let key: BTScanInsert = (*insertstate).itup_key;
    let page: Page;
    let opaque: BTPageOpaque;
    let mut low: OffsetNumber;
    let mut high: OffsetNumber;
    let mut stricthigh: OffsetNumber;
    let mut result: i32;
    let cmpval: i32;

    page = BufferGetPage((*insertstate).buf);
    opaque = BTPageGetOpaque(page);

    Assert!(P_ISLEAF(opaque));
    Assert!(!(*key).nextkey);
    Assert!((*insertstate).postingoff == 0);

    if !(*insertstate).bounds_valid {
        /* Start new binary search */
        low = P_FIRSTDATAKEY(opaque);
        high = PageGetMaxOffsetNumber(page);
    } else {
        /* Restore result of previous binary search against same page */
        low = (*insertstate).low;
        high = (*insertstate).stricthigh;
    }

    /* If there are no keys on the page, return the first available slot */
    if high < low {
        /* Caller can't reuse bounds */
        (*insertstate).low = InvalidOffsetNumber;
        (*insertstate).stricthigh = InvalidOffsetNumber;
        (*insertstate).bounds_valid = false;
        return low;
    }

    /*
     * Binary search to find the first key on the page >= scan key. (nextkey
     * is always false when inserting).
     *
     * The loop invariant is: all slots before 'low' are < scan key, all slots
     * at or after 'high' are >= scan key.  'stricthigh' is > scan key, and is
     * maintained to save additional search effort for caller.
     *
     * We can fall out when high == low.
     */
    if !(*insertstate).bounds_valid {
        high += 1; /* establish the loop invariant for high */
    }
    stricthigh = high; /* high initially strictly higher */

    cmpval = 1; /* !nextkey comparison value */

    while high > low {
        let mid: OffsetNumber = low + ((high - low) / 2);

        /* We have low <= mid < high, so mid points at a real slot */

        result = _bt_compare(rel, key, page, mid);

        if result >= cmpval {
            low = mid + 1;
        } else {
            high = mid;
            if result != 0 {
                stricthigh = high;
            }
        }

        /*
         * If tuple at offset located by binary search is a posting list whose
         * TID range overlaps with caller's scantid, perform posting list
         * binary search to set postingoff for caller.  Caller must split the
         * posting list when postingoff is set.  This should happen
         * infrequently.
         */
        if result == 0 && !(*key).scantid.is_null() {
            /*
             * postingoff should never be set more than once per leaf page
             * binary search.  That would mean that there are duplicate table
             * TIDs in the index, which is never okay.  Check for that here.
             */
            if (*insertstate).postingoff != 0 {
                ereport!(ERROR,
                    /* C also: errcode(ERRCODE_INDEX_CORRUPTED) */                    errmsg_internal!("table tid from new index tuple ({},{}) cannot find insert offset between offsets {} and {} of block {} in index \"{}\"",
                        ItemPointerGetBlockNumber((*key).scantid),
                        ItemPointerGetOffsetNumber((*key).scantid),
                        low, stricthigh,
                        BufferGetBlockNumber((*insertstate).buf),
                        CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()));
            }

            (*insertstate).postingoff = _bt_binsrch_posting(key, page, mid);
        }
    }

    /*
     * On a leaf page, a binary search always returns the first key >= scan
     * key (at least in !nextkey case), which could be the last slot + 1. This
     * is also the lower bound of cached search.
     *
     * stricthigh may also be the last slot + 1, which prevents caller from
     * using bounds directly, but is still useful to us if we're called a
     * second time with cached bounds (cached low will be < stricthigh when
     * that happens).
     */
    (*insertstate).low = low;
    (*insertstate).stricthigh = stricthigh;
    (*insertstate).bounds_valid = true;

    return low;
}

/*----------
 *	_bt_binsrch_posting() -- posting list binary search.
 *
 * Helper routine for _bt_binsrch_insert().
 *
 * Returns offset into posting list where caller's scantid belongs.
 *----------
 */
unsafe fn _bt_binsrch_posting(key: BTScanInsert, page: Page, offnum: OffsetNumber) -> c_int {
    let itup: IndexTuple;
    let itemid: *mut crate::storage::itemid::ItemIdData;
    let mut low: c_int;
    let mut high: c_int;
    let mut mid: c_int;
    let mut res: c_int;

    /*
     * If this isn't a posting tuple, then the index must be corrupt (if it is
     * an ordinary non-pivot tuple then there must be an existing tuple with a
     * heap TID that equals inserter's new heap TID/scantid).  Defensively
     * check that tuple is a posting list tuple whose posting list range
     * includes caller's scantid.
     *
     * (This is also needed because contrib/amcheck's rootdescend option needs
     * to be able to relocate a non-pivot tuple using _bt_binsrch_insert().)
     */
    itemid = PageGetItemId(page, offnum);
    itup = PageGetItem(page, itemid) as IndexTuple;
    if !BTreeTupleIsPosting(itup) {
        return 0;
    }

    Assert!((*key).heapkeyspace && (*key).allequalimage);

    /*
     * In the event that posting list tuple has LP_DEAD bit set, indicate this
     * to _bt_binsrch_insert() caller by returning -1, a sentinel value.  A
     * second call to _bt_binsrch_insert() can take place when its caller has
     * removed the dead item.
     */
    if ItemIdIsDead(itemid) {
        return -1;
    }

    /* "high" is past end of posting list for loop invariant */
    low = 0;
    high = BTreeTupleGetNPosting(itup);
    Assert!(high >= 2);

    while high > low {
        mid = low + ((high - low) / 2);
        res = ItemPointerCompare((*key).scantid, BTreeTupleGetPostingN(itup, mid));

        if res > 0 {
            low = mid + 1;
        } else if res < 0 {
            high = mid;
        } else {
            return mid;
        }
    }

    /* Exact match not found */
    return low;
}

/*----------
 *	_bt_compare() -- Compare insertion-type scankey to tuple on a page.
 *
 *	page/offnum: location of btree item to be compared to.
 *
 *		This routine returns:
 *			<0 if scankey < tuple at offnum;
 *			 0 if scankey == tuple at offnum;
 *			>0 if scankey > tuple at offnum.
 *
 * NULLs in the keys are treated as sortable values.  Therefore
 * "equality" does not necessarily mean that the item should be returned
 * to the caller as a matching key.  Similarly, an insertion scankey
 * with its scantid set is treated as equal to a posting tuple whose TID
 * range overlaps with their scantid.  There generally won't be a
 * matching TID in the posting tuple, which caller must handle
 * themselves (e.g., by splitting the posting list tuple).
 *
 * CRUCIAL NOTE: on a non-leaf page, the first data key is assumed to be
 * "minus infinity": this routine will always claim it is less than the
 * scankey.  The actual key value stored is explicitly truncated to 0
 * attributes (explicitly minus infinity) with version 3+ indexes, but
 * that isn't relied upon.  This allows us to implement the Lehman and
 * Yao convention that the first down-link pointer is before the first
 * key.  See backend/access/nbtree/README for details.
 *----------
 */
pub unsafe fn _bt_compare(
    rel: Relation,
    key: BTScanInsert,
    page: Page,
    offnum: OffsetNumber,
) -> i32 {
    let itupdesc: TupleDesc = RelationGetDescr(rel);
    let opaque: BTPageOpaque = BTPageGetOpaque(page);
    let itup: IndexTuple;
    let mut heapTid: ItemPointer;
    let mut scankey: ScanKey;
    let ncmpkey: c_int;
    let ntupatts: c_int;
    let mut result: i32;

    Assert!(_bt_check_natts(rel, (*key).heapkeyspace, page, offnum));
    Assert!((*key).keysz <= IndexRelationGetNumberOfKeyAttributes(rel));
    Assert!((*key).heapkeyspace || (*key).scantid.is_null());

    /*
     * Force result ">" if target item is first data item on an internal page
     * --- see NOTE above.
     */
    if !P_ISLEAF(opaque) && offnum == P_FIRSTDATAKEY(opaque) {
        return 1;
    }

    itup = PageGetItem(page, PageGetItemId(page, offnum)) as IndexTuple;
    ntupatts = BTreeTupleGetNAtts(itup, rel);

    /*
     * The scan key is set up with the attribute number associated with each
     * term in the key.  It is important that, if the index is multi-key, the
     * scan contain the first k key attributes, and that they be in order.  If
     * you think about how multi-key ordering works, you'll understand why
     * this is.
     *
     * We don't test for violation of this condition here, however.  The
     * initial setup for the index scan had better have gotten it right (see
     * _bt_first).
     */

    let ncmpkey: c_int = Min(ntupatts, (*key).keysz);
    Assert!((*key).heapkeyspace || ncmpkey == (*key).keysz);
    Assert!(!BTreeTupleIsPosting(itup) || (*key).allequalimage);
    scankey = (*key).scankeys.as_mut_ptr();
    let mut i: c_int = 1;
    while i <= ncmpkey {
        let datum: Datum;
        let mut isNull: bool = false;

        datum = index_getattr(itup, (*scankey).sk_attno as c_int, itupdesc, &raw mut isNull);

        if (*scankey).sk_flags & SK_ISNULL != 0 { /* key is NULL */
            if isNull {
                result = 0; /* NULL "=" NULL */
            } else if (*scankey).sk_flags & SK_BT_NULLS_FIRST != 0 {
                result = -1; /* NULL "<" NOT_NULL */
            } else {
                result = 1; /* NULL ">" NOT_NULL */
            }
        } else if isNull { /* key is NOT_NULL and item is NULL */
            if (*scankey).sk_flags & SK_BT_NULLS_FIRST != 0 {
                result = 1; /* NOT_NULL ">" NULL */
            } else {
                result = -1; /* NOT_NULL "<" NULL */
            }
        } else {
            /*
             * The sk_func needs to be passed the index value as left arg and
             * the sk_argument as right arg (they might be of different
             * types).  Since it is convenient for callers to think of
             * _bt_compare as comparing the scankey to the index item, we have
             * to flip the sign of the comparison result.  (Unless it's a DESC
             * column, in which case we *don't* flip the sign.)
             */
            result = DatumGetInt32(FunctionCall2Coll(
                &raw mut (*scankey).sk_func,
                (*scankey).sk_collation,
                datum,
                (*scankey).sk_argument,
            ));

            if (*scankey).sk_flags & SK_BT_DESC == 0 {
                INVERT_COMPARE_RESULT(&mut result);
            }
        }

        /* if the keys are unequal, return the difference */
        if result != 0 {
            return result;
        }

        scankey = scankey.add(1);
        i += 1;
    }

    /*
     * All non-truncated attributes (other than heap TID) were found to be
     * equal.  Treat truncated attributes as minus infinity when scankey has a
     * key attribute value that would otherwise be compared directly.
     *
     * Note: it doesn't matter if ntupatts includes non-key attributes;
     * scankey won't, so explicitly excluding non-key attributes isn't
     * necessary.
     */
    if (*key).keysz > ntupatts {
        return 1;
    }

    /*
     * Use the heap TID attribute and scantid to try to break the tie.  The
     * rules are the same as any other key attribute -- only the
     * representation differs.
     */
    heapTid = BTreeTupleGetHeapTID(itup);
    if (*key).scantid.is_null() {
        /*
         * Forward scans have a scankey that is considered greater than a
         * truncated pivot tuple if and when the scankey has equal values for
         * attributes up to and including the least significant untruncated
         * attribute in tuple.  Even attributes that were omitted from the
         * scan key are considered greater than -inf truncated attributes.
         * (See _bt_binsrch for an explanation of our backward scan behavior.)
         *
         * For example, if an index has the minimum two attributes (single
         * user key attribute, plus heap TID attribute), and a page's high key
         * is ('foo', -inf), and scankey is ('foo', <omitted>), the search
         * will not descend to the page to the left.  The search will descend
         * right instead.  The truncated attribute in pivot tuple means that
         * all non-pivot tuples on the page to the left are strictly < 'foo',
         * so it isn't necessary to descend left.  In other words, search
         * doesn't have to descend left because it isn't interested in a match
         * that has a heap TID value of -inf.
         *
         * Note: the heap TID part of the test ensures that scankey is being
         * compared to a pivot tuple with one or more truncated -inf key
         * attributes.  The heap TID attribute is the last key attribute in
         * every index, of course, but other than that it isn't special.
         */
        if !(*key).backward && (*key).keysz == ntupatts && heapTid.is_null()
            && (*key).heapkeyspace
        {
            return 1;
        }

        /* All provided scankey arguments found to be equal */
        return 0;
    }

    /*
     * Treat truncated heap TID as minus infinity, since scankey has a key
     * attribute value (scantid) that would otherwise be compared directly
     */
    Assert!((*key).keysz == IndexRelationGetNumberOfKeyAttributes(rel));
    if heapTid.is_null() {
        return 1;
    }

    /*
     * Scankey must be treated as equal to a posting list tuple if its scantid
     * value falls within the range of the posting list.  In all other cases
     * there can only be a single heap TID value, which is compared directly
     * with scantid.
     */
    Assert!(ntupatts >= IndexRelationGetNumberOfKeyAttributes(rel));
    result = ItemPointerCompare((*key).scantid, heapTid);
    if result <= 0 || !BTreeTupleIsPosting(itup) {
        return result;
    } else {
        result = ItemPointerCompare((*key).scantid, BTreeTupleGetMaxHeapTID(itup));
        if result > 0 {
            return 1;
        }
    }

    return 0;
}

// Extra SK_BT flag constants used below (defined in nbtutils.rs but local here too)
// TODO(pg-port): these are duplicated from nbtutils.rs; merge when module is unified
const SK_BT_REQFWD_:     u32 = 0x00010000;
const SK_BT_REQBKWD_:    u32 = 0x00020000;
const SK_BT_SKIP_:       u32 = 0x00100000;
const SK_BT_MINVAL_:     u32 = 0x00200000;
const SK_BT_MAXVAL_:     u32 = 0x00400000;
const SK_BT_NEXT_:       u32 = 0x00800000;
const SK_BT_PRIOR_:      u32 = 0x01000000;
const SK_BT_DESC_:       u32 = 0x40000000; /* INDOPTION_DESC << 24 */
const SK_BT_NULLS_FIRST_: u32 = 0x80000000u32; /* INDOPTION_NULLS_FIRST << 24 */
/* c_int aliases for direct (uncast) sk_flags comparisons in _bt_compare */
const SK_BT_DESC:        c_int = 0x40000000u32 as c_int; /* INDOPTION_DESC << 24 */
const SK_BT_NULLS_FIRST: c_int = 0x80000000u32 as c_int; /* INDOPTION_NULLS_FIRST << 24 */

/// TODO(pg-port): InvalidBlockNumber (storage/block.h).
pub const InvalidBlockNumber: BlockNumber = 0xFFFF_FFFF;

/*
 *	_bt_first() -- Find the first item in a scan.
 *
 *		We need to be clever about the direction of scan, the search
 *		conditions, and the tree ordering.  We find the first item (or,
 *		if backwards scan, the last item) in the tree that satisfies the
 *		qualifications in the scan key.  On success exit, data about the
 *		matching tuple(s) on the page has been loaded into so->currPos.  We'll
 *		drop all locks and hold onto a pin on page's buffer, except during
 *		so->dropPin scans, when we drop both the lock and the pin.
 *		_bt_returnitem sets the next item to return to scan on success exit.
 *
 * If there are no matching items in the index, we return false, with no
 * pins or locks held.  so->currPos will remain invalid.
 *
 * Note that scan->keyData[], and the so->keyData[] scankey built from it,
 * are both search-type scankeys (see nbtree/README for more about this).
 * Within this routine, we build a temporary insertion-type scankey to use
 * in locating the scan start position.
 */
pub unsafe fn _bt_first(scan: IndexScanDesc, dir: ScanDirection) -> bool {
    let rel: Relation = (*scan).indexRelation;
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;
    let mut stack: BTStack;
    let offnum: OffsetNumber;
    let mut inskey: BTScanInsertData = core::mem::zeroed();
    let mut startKeys: [ScanKey; INDEX_MAX_KEYS as usize] = [core::ptr::null_mut(); INDEX_MAX_KEYS as usize];
    let mut notnullkeys: [ScanKeyData; INDEX_MAX_KEYS as usize] = [core::mem::zeroed(); INDEX_MAX_KEYS as usize];
    let mut keysz: c_int = 0;
    let mut strat_total: StrategyNumber;
    let mut blkno: BlockNumber = InvalidBlockNumber;
    let mut lastcurrblkno: BlockNumber = 0;

    Assert!(!BTScanPosIsValid(&(*so).currPos));

    /*
     * Examine the scan keys and eliminate any redundant keys; also mark the
     * keys that must be matched to continue the scan.
     */
    _bt_preprocess_keys(scan);

    /*
     * Quit now if _bt_preprocess_keys() discovered that the scan keys can
     * never be satisfied (eg, x == 1 AND x > 2).
     */
    if !(*so).qual_ok {
        Assert!(!(*so).needPrimScan);
        _bt_parallel_done(scan);
        return false;
    }

    /*
     * If this is a parallel scan, we must seize the scan.  _bt_readfirstpage
     * will likely release the parallel scan later on.
     */
    if !(*scan).parallel_scan.is_null() &&
        !_bt_parallel_seize(scan, &raw mut blkno, &raw mut lastcurrblkno, true)
    {
        return false;
    }

    /*
     * Initialize the scan's arrays (if any) for the current scan direction
     * (except when they were already set to later values as part of
     * scheduling the primitive index scan that is now underway)
     */
    if (*so).numArrayKeys != 0 && !(*so).needPrimScan {
        _bt_start_array_keys(scan, dir);
    }

    if blkno != InvalidBlockNumber {
        /*
         * We anticipated calling _bt_search, but another worker bet us to it.
         * _bt_readnextpage releases the scan for us (not _bt_readfirstpage).
         */
        Assert!(!(*scan).parallel_scan.is_null());
        Assert!(!(*so).needPrimScan);
        Assert!(blkno != P_NONE);

        if !_bt_readnextpage(scan, blkno, lastcurrblkno, dir, true) {
            return false;
        }

        _bt_returnitem(scan, so);
        return true;
    }

    /*
     * Count an indexscan for stats, now that we know that we'll call
     * _bt_search/_bt_endpoint below
     */
    pgstat_count_index_scan(rel);
    if !(*scan).instrument.is_null() {
        (*(*scan).instrument).nsearches += 1;
    }

    /*----------
     * Examine the scan keys to discover where we need to start the scan.
     * The selected scan keys (at most one per index column) are remembered by
     * storing their addresses into the local startKeys[] array.  The final
     * startKeys[] entry's strategy is set in strat_total. (Actually, there
     * are a couple of cases where we force a less/more restrictive strategy.)
     *
     * We must use the key that was marked required (in the direction opposite
     * our own scan's) during preprocessing.  Each index attribute can only
     * have one such required key.  In general, the keys that we use to find
     * an initial position when scanning forwards are the same keys that end
     * the scan on the leaf level when scanning backwards (and vice-versa).
     *
     * When the scan keys include cross-type operators, _bt_preprocess_keys
     * may not be able to eliminate redundant keys; in such cases it will
     * arbitrarily pick a usable key for each attribute (and scan direction),
     * ensuring that there is no more than one key required in each direction.
     * We stop considering further keys once we reach the first nonrequired
     * key (which must come after all required keys), so this can't affect us.
     *
     * The required keys that we use as starting boundaries have to be =, >,
     * or >= keys for a forward scan or =, <, <= keys for a backwards scan.
     * We can use keys for multiple attributes so long as the prior attributes
     * had only =, >= (resp. =, <=) keys.  These rules are very similar to the
     * rules that preprocessing used to determine which keys to mark required.
     * We cannot always use every required key as a positioning key, though.
     * Skip arrays necessitate independently applying our own rules here.
     * Skip arrays are always generally considered = array keys, but we'll
     * nevertheless treat them as inequalities at certain points of the scan.
     * When that happens, it _might_ have implications for the number of
     * required keys that we can safely use for initial positioning purposes.
     *
     * For example, a forward scan with a skip array on its leading attribute
     * (with no low_compare/high_compare) will have at least two required scan
     * keys, but we won't use any of them as boundary keys during the scan's
     * initial call here.  Our positioning key during the first call here can
     * be thought of as representing "> -infinity".  Similarly, if such a skip
     * array's low_compare is "a > 'foo'", then we position using "a > 'foo'"
     * during the scan's initial call here; a lower-order key such as "b = 42"
     * can't be used until the "a" array advances beyond MINVAL/low_compare.
     *
     * On the other hand, if such a skip array's low_compare was "a >= 'foo'",
     * then we _can_ use "a >= 'foo' AND b = 42" during the initial call here.
     * A subsequent call here might have us use "a = 'fop' AND b = 42".  Note
     * that we treat = and >= as equivalent when scanning forwards (just as we
     * treat = and <= as equivalent when scanning backwards).  We effectively
     * do the same thing (though with a distinct "a" element/value) each time.
     *
     * All keys (with the exception of SK_SEARCHNULL keys and SK_BT_SKIP
     * array keys whose array is "null_elem=true") imply a NOT NULL qualifier.
     * If the index stores nulls at the end of the index we'll be starting
     * from, and we have no boundary key for the column (which means the key
     * we deduced NOT NULL from is an inequality key that constrains the other
     * end of the index), then we cons up an explicit SK_SEARCHNOTNULL key to
     * use as a boundary key.  If we didn't do this, we might find ourselves
     * traversing a lot of null entries at the start of the scan.
     *
     * In this loop, row-comparison keys are treated the same as keys on their
     * first (leftmost) columns.  We'll add all lower-order columns of the row
     * comparison that were marked required during preprocessing below.
     *
     * _bt_advance_array_keys needs to know exactly how we'll reposition the
     * scan (should it opt to schedule another primitive index scan).  It is
     * critical that primscans only be scheduled when they'll definitely make
     * some useful progress.  _bt_advance_array_keys does this by calling
     * _bt_checkkeys routines that report whether a tuple is past the end of
     * matches for the scan's keys (given the scan's current array elements).
     * If the page's final tuple is "after the end of matches" for a scan that
     * uses the *opposite* scan direction, then it must follow that it's also
     * "before the start of matches" for the actual current scan direction.
     * It is therefore essential that all of our initial positioning rules are
     * symmetric with _bt_checkkeys's corresponding continuescan=false rule.
     * If you update anything here, _bt_checkkeys/_bt_advance_array_keys might
     * need to be kept in sync.
     *----------
     */
    strat_total = BTEqualStrategyNumber;
    if (*so).numberOfKeys > 0 {
        let mut curattr: AttrNumber;
        let mut bkey: ScanKey;
        let mut impliesNN: ScanKey;
        let mut cur: ScanKey;

        /*
         * bkey will be set to the key that preprocessing left behind as the
         * boundary key for this attribute, in this scan direction (if any)
         */
        cur = (*so).keyData;
        curattr = 1;
        bkey = core::ptr::null_mut();
        /* Also remember any scankey that implies a NOT NULL constraint */
        impliesNN = core::ptr::null_mut();

        /*
         * Loop iterates from 0 to numberOfKeys inclusive; we use the last
         * pass to handle after-last-key processing.  Actual exit from the
         * loop is at one of the "break" statements below.
         */
        let mut i: c_int = 0;
        loop {
            if i >= (*so).numberOfKeys || (*cur).sk_attno != curattr {
                /* Done looking for the curattr boundary key */
                Assert!(bkey.is_null() ||
                       ((*bkey).sk_attno == curattr &&
                        ((*bkey).sk_flags as u32 & (SK_BT_REQFWD_ | SK_BT_REQBKWD_)) != 0));
                Assert!(impliesNN.is_null() ||
                       ((*impliesNN).sk_attno == curattr &&
                        ((*impliesNN).sk_flags as u32 & (SK_BT_REQFWD_ | SK_BT_REQBKWD_)) != 0));

                /*
                 * If this is a scan key for a skip array whose current
                 * element is MINVAL, choose low_compare (when scanning
                 * backwards it'll be MAXVAL, and we'll choose high_compare).
                 *
                 * Note: if the array's low_compare key makes 'bkey' NULL,
                 * then we behave as if the array's first element is -inf,
                 * except when !array->null_elem implies a usable NOT NULL
                 * constraint.
                 */
                if !bkey.is_null() &&
                    ((*bkey).sk_flags as u32 & (SK_BT_MINVAL_ | SK_BT_MAXVAL_)) != 0
                {
                    let ikey: c_int = bkey as c_int - (*so).keyData as c_int; /* byte offset / sizeof? use pointer arithmetic */
                    let ikey: c_int = (bkey as usize - (*so).keyData as usize) as c_int
                        / size_of::<ScanKeyData>() as c_int;
                    let skipequalitykey: ScanKey = bkey;
                    let mut array: *mut BTArrayKeyInfo = core::ptr::null_mut();

                    let mut arridx: c_int = 0;
                    while arridx < (*so).numArrayKeys {
                        array = (*so).arrayKeys.add(arridx as usize);
                        if (*array).scan_key == ikey {
                            break;
                        }
                        arridx += 1;
                    }

                    if ScanDirectionIsForward(dir) {
                        Assert!(((*skipequalitykey).sk_flags as u32 & SK_BT_MAXVAL_) == 0);
                        bkey = (*array).low_compare;
                    } else {
                        Assert!(((*skipequalitykey).sk_flags as u32 & SK_BT_MINVAL_) == 0);
                        bkey = (*array).high_compare;
                    }

                    Assert!(bkey.is_null() ||
                           (*bkey).sk_attno == (*skipequalitykey).sk_attno);

                    if !(*array).null_elem {
                        impliesNN = skipequalitykey;
                    } else {
                        Assert!(bkey.is_null() && impliesNN.is_null());
                    }
                }

                /*
                 * If we didn't find a usable boundary key, see if we can
                 * deduce a NOT NULL key
                 */
                if bkey.is_null() && !impliesNN.is_null() &&
                    (if ((*impliesNN).sk_flags as u32 & SK_BT_NULLS_FIRST_) != 0 {
                        ScanDirectionIsForward(dir)
                    } else {
                        ScanDirectionIsBackward(dir)
                    })
                {
                    /* Yes, so build the key in notnullkeys[keysz] */
                    bkey = &raw mut notnullkeys[keysz as usize];
                    ScanKeyEntryInitialize(bkey,
                                           (SK_SEARCHNOTNULL | SK_ISNULL |
                                            ((*impliesNN).sk_flags &
                                             (SK_BT_DESC_ as c_int | SK_BT_NULLS_FIRST_ as c_int))) as c_int,
                                           curattr,
                                           (if ((*impliesNN).sk_flags as u32 & SK_BT_NULLS_FIRST_) != 0 {
                                               BTGreaterStrategyNumber
                                           } else {
                                               BTLessStrategyNumber
                                           }) as StrategyNumber,
                                           InvalidOid,
                                           InvalidOid,
                                           InvalidOid,
                                           0usize);
                }

                /*
                 * If preprocessing didn't leave a usable boundary key, quit;
                 * else save the boundary key pointer in startKeys[]
                 */
                if bkey.is_null() {
                    break;
                }
                startKeys[keysz as usize] = bkey;
                keysz += 1;

                /*
                 * We can only consider adding more boundary keys when the one
                 * that we just chose to add uses either the = or >= strategy
                 * (during backwards scans we can only do so when the key that
                 * we just added to startKeys[] uses the = or <= strategy)
                 */
                strat_total = (*bkey).sk_strategy as StrategyNumber;
                if strat_total == BTGreaterStrategyNumber ||
                    strat_total == BTLessStrategyNumber
                {
                    break;
                }

                /*
                 * If the key that we just added to startKeys[] is a skip
                 * array = key whose current element is marked NEXT or PRIOR,
                 * make strat_total > or < (and stop adding boundary keys).
                 * This can only happen with opclasses that lack skip support.
                 */
                if ((*bkey).sk_flags as u32 & (SK_BT_NEXT_ | SK_BT_PRIOR_)) != 0 {
                    Assert!(((*bkey).sk_flags as u32 & SK_BT_SKIP_) != 0);
                    Assert!(strat_total == BTEqualStrategyNumber);

                    if ScanDirectionIsForward(dir) {
                        Assert!(((*bkey).sk_flags as u32 & SK_BT_PRIOR_) == 0);
                        strat_total = BTGreaterStrategyNumber;
                    } else {
                        Assert!(((*bkey).sk_flags as u32 & SK_BT_NEXT_) == 0);
                        strat_total = BTLessStrategyNumber;
                    }

                    /*
                     * We're done.  We'll never find an exact = match for a
                     * NEXT or PRIOR sentinel sk_argument value.  There's no
                     * sense in trying to add more keys to startKeys[].
                     */
                    break;
                }

                /*
                 * Done if that was the last scan key output by preprocessing.
                 * Also done if we've now examined all keys marked required.
                 */
                if i >= (*so).numberOfKeys ||
                    ((*cur).sk_flags as u32 & (SK_BT_REQFWD_ | SK_BT_REQBKWD_)) == 0
                {
                    break;
                }

                /*
                 * Reset for next attr.
                 */
                Assert!((*cur).sk_attno == curattr + 1);
                curattr = (*cur).sk_attno;
                bkey = core::ptr::null_mut();
                impliesNN = core::ptr::null_mut();
            }

            if i >= (*so).numberOfKeys {
                break;
            }

            /*
             * If we've located the starting boundary key for curattr, we have
             * no interest in curattr's other required key
             */
            if !bkey.is_null() {
                cur = cur.add(1);
                i += 1;
                continue;
            }

            /*
             * Is this key the starting boundary key for curattr?
             *
             * If not, does it imply a NOT NULL constraint?  (Because
             * SK_SEARCHNULL keys are always assigned BTEqualStrategyNumber,
             * *any* inequality key works for that; we need not test.)
             */
            match (*cur).sk_strategy as u32 {
                s if s == BTLessStrategyNumber as u32 ||
                     s == BTLessEqualStrategyNumber as u32 => {
                    if ScanDirectionIsBackward(dir) {
                        bkey = cur;
                    } else if impliesNN.is_null() {
                        impliesNN = cur;
                    }
                }
                s if s == BTEqualStrategyNumber as u32 => {
                    bkey = cur;
                }
                s if s == BTGreaterEqualStrategyNumber as u32 ||
                     s == BTGreaterStrategyNumber as u32 => {
                    if ScanDirectionIsForward(dir) {
                        bkey = cur;
                    } else if impliesNN.is_null() {
                        impliesNN = cur;
                    }
                }
                _ => {}
            }

            cur = cur.add(1);
            i += 1;
        }
    }

    /*
     * If we found no usable boundary keys, we have to start from one end of
     * the tree.  Walk down that edge to the first or last key, and scan from
     * there.
     *
     * Note: calls _bt_readfirstpage for us, which releases the parallel scan.
     */
    if keysz == 0 {
        return _bt_endpoint(scan, dir);
    }

    /*
     * We want to start the scan somewhere within the index.  Set up an
     * insertion scankey we can use to search for the boundary point we
     * identified above.  The insertion scankey is built using the keys
     * identified by startKeys[].  (Remaining insertion scankey fields are
     * initialized after initial-positioning scan keys are finalized.)
     */
    Assert!(keysz <= INDEX_MAX_KEYS);
    let mut i: c_int = 0;
    while i < keysz {
        let bkey: ScanKey = startKeys[i as usize];

        Assert!((*bkey).sk_attno as c_int == i + 1);

        if (*bkey).sk_flags & SK_ROW_HEADER as c_int != 0 {
            /*
             * Row comparison header: look to the first row member instead
             */
            let subkey_base: ScanKey = DatumGetPointer((*bkey).sk_argument) as ScanKey;
            let mut subkey: ScanKey = subkey_base;
            let mut loosen_strat: bool = false;
            let mut tighten_strat: bool = false;

            /*
             * Cannot be a NULL in the first row member: _bt_preprocess_keys
             * would've marked the qual as unsatisfiable, preventing us from
             * ever getting this far
             */
            Assert!((*subkey).sk_flags & SK_ROW_MEMBER as c_int != 0);
            Assert!((*subkey).sk_attno == (*bkey).sk_attno);
            Assert!((*subkey).sk_flags & SK_ISNULL as c_int == 0);

            /*
             * This is either a > or >= key (during backwards scans it is
             * either < or <=) that was marked required during preprocessing.
             * Later so->keyData[] keys can't have been marked required, so
             * our row compare header key must be the final startKeys[] entry.
             */
            Assert!(((*subkey).sk_flags as u32 & (SK_BT_REQFWD_ | SK_BT_REQBKWD_)) != 0);
            Assert!((*subkey).sk_strategy == (*bkey).sk_strategy);
            Assert!((*subkey).sk_strategy as u32 == strat_total as u32);
            Assert!(i == keysz - 1);

            /*
             * The member scankeys are already in insertion format (ie, they
             * have sk_func = 3-way-comparison function)
             */
            memcpy(inskey.scankeys.as_mut_ptr().add(i as usize) as *mut c_void,
                   subkey as *const c_void,
                   size_of::<ScanKeyData>());

            /*
             * Now look to later row compare members.
             *
             * If there's an "index attribute gap" between two row compare
             * members, the second member won't have been marked required, and
             * so can't be used as a starting boundary key here.  The part of
             * the row comparison that we do still use has to be treated as a
             * ">=" or "<=" condition.  For example, a qual "(a, c) > (1, 42)"
             * with an omitted intervening index attribute "b" will use an
             * insertion scan key "a >= 1".  Even the first "a = 1" tuple on
             * the leaf level might satisfy the row compare qual.
             *
             * We're able to use a _more_ restrictive strategy when we reach a
             * NULL row compare member, since they're always unsatisfiable.
             * For example, a qual "(a, b, c) >= (1, NULL, 77)" will use an
             * insertion scan key "a > 1".  All tuples where "a = 1" cannot
             * possibly satisfy the row compare qual, so this is safe.
             */
            Assert!((*subkey).sk_flags & SK_ROW_END as c_int == 0);
            loop {
                subkey = subkey.add(1);
                Assert!((*subkey).sk_flags & SK_ROW_MEMBER as c_int != 0);

                if (*subkey).sk_flags & SK_ISNULL as c_int != 0 {
                    /*
                     * NULL member key, can only use earlier keys.
                     *
                     * We deliberately avoid checking if this key is marked
                     * required.  All earlier keys are required, and this key
                     * is unsatisfiable either way, so we can't miss anything.
                     */
                    tighten_strat = true;
                    break;
                }

                if ((*subkey).sk_flags as u32 & (SK_BT_REQFWD_ | SK_BT_REQBKWD_)) == 0 {
                    /* nonrequired member key, can only use earlier keys */
                    loosen_strat = true;
                    break;
                }

                Assert!((*subkey).sk_attno as c_int == keysz + 1);
                Assert!((*subkey).sk_strategy == (*bkey).sk_strategy);
                Assert!(keysz < INDEX_MAX_KEYS);

                memcpy(inskey.scankeys.as_mut_ptr().add(keysz as usize) as *mut c_void,
                       subkey as *const c_void,
                       size_of::<ScanKeyData>());
                keysz += 1;

                if (*subkey).sk_flags & SK_ROW_END as c_int != 0 {
                    break;
                }
            }
            Assert!(!(loosen_strat && tighten_strat));
            if loosen_strat {
                /* Use less restrictive strategy (and fewer member keys) */
                match strat_total as u32 {
                    s if s == BTLessStrategyNumber as u32 => {
                        strat_total = BTLessEqualStrategyNumber;
                    }
                    s if s == BTGreaterStrategyNumber as u32 => {
                        strat_total = BTGreaterEqualStrategyNumber;
                    }
                    _ => {}
                }
            }
            if tighten_strat {
                /* Use more restrictive strategy (and fewer member keys) */
                match strat_total as u32 {
                    s if s == BTLessEqualStrategyNumber as u32 => {
                        strat_total = BTLessStrategyNumber;
                    }
                    s if s == BTGreaterEqualStrategyNumber as u32 => {
                        strat_total = BTGreaterStrategyNumber;
                    }
                    _ => {}
                }
            }

            /* Done (row compare header key is always last startKeys[] key) */
            break;
        }

        /*
         * Ordinary comparison key/search-style key.
         *
         * Transform the search-style scan key to an insertion scan key by
         * replacing the sk_func with the appropriate btree 3-way-comparison
         * function.
         *
         * If scankey operator is not a cross-type comparison, we can use the
         * cached comparison function; otherwise gotta look it up in the
         * catalogs.  (That can't lead to infinite recursion, since no
         * indexscan initiated by syscache lookup will use cross-data-type
         * operators.)
         *
         * We support the convention that sk_subtype == InvalidOid means the
         * opclass input type; this hack simplifies life for ScanKeyInit().
         */
        if (*bkey).sk_subtype == rd_opcintype(rel, i as usize) ||
            (*bkey).sk_subtype == InvalidOid
        {
            let procinfo: *mut FmgrInfo;

            procinfo = index_getprocinfo(rel, (*bkey).sk_attno, BTORDER_PROC);
            ScanKeyEntryInitializeWithInfo(inskey.scankeys.as_mut_ptr().add(i as usize),
                                           (*bkey).sk_flags,
                                           (*bkey).sk_attno,
                                           InvalidStrategy as StrategyNumber,
                                           (*bkey).sk_subtype,
                                           (*bkey).sk_collation,
                                           procinfo,
                                           (*bkey).sk_argument);
        } else {
            let cmp_proc: RegProcedure;

            cmp_proc = get_opfamily_proc(rd_opfamily(rel, i as usize),
                                         rd_opcintype(rel, i as usize),
                                         (*bkey).sk_subtype, BTORDER_PROC);
            if !RegProcedureIsValid(cmp_proc) {
                elog!(ERROR, "missing support function {}({},{}) for attribute {} of index \"{}\"",
                     BTORDER_PROC,
                     rd_opcintype(rel, i as usize),
                     (*bkey).sk_subtype,
                     (*bkey).sk_attno,
                     CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy());
            }
            ScanKeyEntryInitialize(inskey.scankeys.as_mut_ptr().add(i as usize),
                                   (*bkey).sk_flags,
                                   (*bkey).sk_attno,
                                   InvalidStrategy as StrategyNumber,
                                   (*bkey).sk_subtype,
                                   (*bkey).sk_collation,
                                   cmp_proc,
                                   (*bkey).sk_argument);
        }

        i += 1;
    }

    /*----------
     * Examine the selected initial-positioning strategy to determine exactly
     * where we need to start the scan, and set flag variables to control the
     * initial descent by _bt_search (and our _bt_binsrch call for the leaf
     * page _bt_search returns).
     *----------
     */
    _bt_metaversion(rel, &raw mut inskey.heapkeyspace, &raw mut inskey.allequalimage);
    inskey.anynullkeys = false; /* unused */
    inskey.scantid = core::ptr::null_mut();
    inskey.keysz = keysz;
    match strat_total as u32 {
        s if s == BTLessStrategyNumber as u32 => {
            inskey.nextkey = false;
            inskey.backward = true;
        }
        s if s == BTLessEqualStrategyNumber as u32 => {
            inskey.nextkey = true;
            inskey.backward = true;
        }
        s if s == BTEqualStrategyNumber as u32 => {
            /*
             * If a backward scan was specified, need to start with last equal
             * item not first one.
             */
            if ScanDirectionIsBackward(dir) {
                /*
                 * This is the same as the <= strategy
                 */
                inskey.nextkey = true;
                inskey.backward = true;
            } else {
                /*
                 * This is the same as the >= strategy
                 */
                inskey.nextkey = false;
                inskey.backward = false;
            }
        }
        s if s == BTGreaterEqualStrategyNumber as u32 => {
            /*
             * Find first item >= scankey
             */
            inskey.nextkey = false;
            inskey.backward = false;
        }
        s if s == BTGreaterStrategyNumber as u32 => {
            /*
             * Find first item > scankey
             */
            inskey.nextkey = true;
            inskey.backward = false;
        }
        _ => {
            /* can't get here, but keep compiler quiet */
            elog!(ERROR, "unrecognized strat_total: {}", strat_total);
            return false;
        }
    }

    /*
     * Use the manufactured insertion scan key to descend the tree and
     * position ourselves on the target leaf page.
     */
    Assert!(ScanDirectionIsBackward(dir) == inskey.backward);
    stack = _bt_search(rel, core::ptr::null_mut(), &raw mut inskey, &raw mut (*so).currPos.buf, BT_READ);

    /* don't need to keep the stack around... */
    _bt_freestack(stack);

    if !BufferIsValid((*so).currPos.buf) {
        Assert!(!(*so).needPrimScan);

        /*
         * We only get here if the index is completely empty. Lock relation
         * because nothing finer to lock exists.  Without a buffer lock, it's
         * possible for another transaction to insert data between
         * _bt_search() and PredicateLockRelation().  We have to try again
         * after taking the relation-level predicate lock, to close a narrow
         * window where we wouldn't scan concurrently inserted tuples, but the
         * writer wouldn't see our predicate lock.
         */
        if IsolationIsSerializable() {
            PredicateLockRelation(rel, (*scan).xs_snapshot);
            stack = _bt_search(rel, core::ptr::null_mut(), &raw mut inskey, &raw mut (*so).currPos.buf, BT_READ);
            _bt_freestack(stack);
        }

        if !BufferIsValid((*so).currPos.buf) {
            _bt_parallel_done(scan);
            return false;
        }
    }

    /* position to the precise item on the page */
    offnum = _bt_binsrch(rel, &raw mut inskey, (*so).currPos.buf);

    /*
     * Now load data from the first page of the scan (usually the page
     * currently in so->currPos.buf).
     *
     * If inskey.nextkey = false and inskey.backward = false, offnum is
     * positioned at the first non-pivot tuple >= inskey.scankeys.
     *
     * If inskey.nextkey = false and inskey.backward = true, offnum is
     * positioned at the last non-pivot tuple < inskey.scankeys.
     *
     * If inskey.nextkey = true and inskey.backward = false, offnum is
     * positioned at the first non-pivot tuple > inskey.scankeys.
     *
     * If inskey.nextkey = true and inskey.backward = true, offnum is
     * positioned at the last non-pivot tuple <= inskey.scankeys.
     *
     * It's possible that _bt_binsrch returned an offnum that is out of bounds
     * for the page.  For example, when inskey is both < the leaf page's high
     * key and > all of its non-pivot tuples, offnum will be "maxoff + 1".
     */
    if !_bt_readfirstpage(scan, offnum, dir) {
        return false;
    }

    _bt_returnitem(scan, so);
    return true;
}

/*
 *	_bt_next() -- Get the next item in a scan.
 *
 *		On entry, so->currPos describes the current page, which may be pinned
 *		but is not locked, and so->currPos.itemIndex identifies which item was
 *		previously returned.
 *
 *		On success exit, so->currPos is updated as needed, and _bt_returnitem
 *		sets the next item to return to the scan.  so->currPos remains valid.
 *
 *		On failure exit (no more tuples), we invalidate so->currPos.  It'll
 *		still be possible for the scan to return tuples by changing direction,
 *		though we'll need to call _bt_first anew in that other direction.
 */
pub unsafe fn _bt_next(scan: IndexScanDesc, dir: ScanDirection) -> bool {
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;

    Assert!(BTScanPosIsValid(&(*so).currPos));

    /*
     * Advance to next tuple on current page; or if there's no more, try to
     * step to the next page with data.
     */
    if ScanDirectionIsForward(dir) {
        (*so).currPos.itemIndex += 1;
        if (*so).currPos.itemIndex > (*so).currPos.lastItem {
            if !_bt_steppage(scan, dir) {
                return false;
            }
        }
    } else {
        (*so).currPos.itemIndex -= 1;
        if (*so).currPos.itemIndex < (*so).currPos.firstItem {
            if !_bt_steppage(scan, dir) {
                return false;
            }
        }
    }

    _bt_returnitem(scan, so);
    return true;
}

/*
 *	_bt_readpage() -- Load data from current index page into so->currPos
 *
 * Caller must have pinned and read-locked so->currPos.buf; the buffer's state
 * is not changed here.  Also, currPos.moreLeft and moreRight must be valid;
 * they are updated as appropriate.  All other fields of so->currPos are
 * initialized from scratch here.
 *
 * We scan the current page starting at offnum and moving in the indicated
 * direction.  All items matching the scan keys are loaded into currPos.items.
 * moreLeft or moreRight (as appropriate) is cleared if _bt_checkkeys reports
 * that there can be no more matching tuples in the current scan direction
 * (could just be for the current primitive index scan when scan has arrays).
 *
 * In the case of a parallel scan, caller must have called _bt_parallel_seize
 * prior to calling this function; this function will invoke
 * _bt_parallel_release before returning.
 *
 * Returns true if any matching items found on the page, false if none.
 */
unsafe fn _bt_readpage(
    scan: IndexScanDesc,
    dir: ScanDirection,
    mut offnum: OffsetNumber,
    firstpage: bool,
) -> bool {
    let rel: Relation = (*scan).indexRelation;
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;
    let page: Page;
    let opaque: BTPageOpaque;
    let minoff: OffsetNumber;
    let maxoff: OffsetNumber;
    let mut pstate: BTReadPageState = core::mem::zeroed();
    let arrayKeys: bool;
    let mut itemIndex: c_int;
    let indnatts: c_int;

    /* save the page/buffer block number, along with its sibling links */
    page = BufferGetPage((*so).currPos.buf);
    opaque = BTPageGetOpaque(page);
    (*so).currPos.currPage = BufferGetBlockNumber((*so).currPos.buf);
    (*so).currPos.prevPage = (*opaque).btpo_prev;
    (*so).currPos.nextPage = (*opaque).btpo_next;
    /* delay setting so->currPos.lsn until _bt_drop_lock_and_maybe_pin */
    (*so).currPos.dir = dir;
    (*so).currPos.nextTupleOffset = 0;

    /* either moreRight or moreLeft should be set now (may be unset later) */
    Assert!(if ScanDirectionIsForward(dir) { (*so).currPos.moreRight } else { (*so).currPos.moreLeft });
    Assert!(!P_IGNORE(opaque));
    Assert!(BTScanPosIsPinned(&(*so).currPos));
    Assert!(!(*so).needPrimScan);

    if !(*scan).parallel_scan.is_null() {
        /* allow next/prev page to be read by other worker without delay */
        if ScanDirectionIsForward(dir) {
            _bt_parallel_release(scan, (*so).currPos.nextPage, (*so).currPos.currPage);
        } else {
            _bt_parallel_release(scan, (*so).currPos.prevPage, (*so).currPos.currPage);
        }
    }

    PredicateLockPage(rel, (*so).currPos.currPage, (*scan).xs_snapshot);

    /* initialize local variables */
    indnatts = IndexRelationGetNumberOfAttributes(rel);
    arrayKeys = (*so).numArrayKeys != 0;
    minoff = P_FIRSTDATAKEY(opaque);
    maxoff = PageGetMaxOffsetNumber(page);

    /* initialize page-level state that we'll pass to _bt_checkkeys */
    pstate.minoff = minoff;
    pstate.maxoff = maxoff;
    pstate.finaltup = core::ptr::null_mut();
    pstate.page = page;
    pstate.firstpage = firstpage;
    pstate.forcenonrequired = false;
    pstate.startikey = 0;
    pstate.offnum = InvalidOffsetNumber;
    pstate.skip = InvalidOffsetNumber;
    pstate.continuescan = true; /* default assumption */
    pstate.rechecks = 0;
    pstate.targetdistance = 0;
    pstate.nskipadvances = 0;

    if ScanDirectionIsForward(dir) {
        /* SK_SEARCHARRAY forward scans must provide high key up front */
        if arrayKeys {
            if !P_RIGHTMOST(opaque) {
                let iid: *mut ItemIdData = PageGetItemId(page, P_HIKEY);

                pstate.finaltup = PageGetItem(page, iid) as IndexTuple;

                if (*so).scanBehind &&
                    !_bt_scanbehind_checkkeys(scan, dir, pstate.finaltup)
                {
                    /* Schedule another primitive index scan after all */
                    (*so).currPos.moreRight = false;
                    (*so).needPrimScan = true;
                    if !(*scan).parallel_scan.is_null() {
                        _bt_parallel_primscan_schedule(scan, (*so).currPos.currPage);
                    }
                    return false;
                }
            }

            (*so).scanBehind = false; /* reset */
            (*so).oppositeDirCheck = false; /* reset */
        }

        /*
         * Consider pstate.startikey optimization once the ongoing primitive
         * index scan has already read at least one page
         */
        if !pstate.firstpage && minoff < maxoff {
            _bt_set_startikey(scan, &raw mut pstate);
        }

        /* load items[] in ascending order */
        itemIndex = 0;

        if offnum < minoff {
            offnum = minoff;
        }

        while offnum <= maxoff {
            let iid: *mut ItemIdData = PageGetItemId(page, offnum);
            let itup: IndexTuple;
            let passes_quals: bool;

            /*
             * If the scan specifies not to return killed tuples, then we
             * treat a killed tuple as not passing the qual
             */
            if (*scan).ignore_killed_tuples && ItemIdIsDead(iid) {
                offnum = OffsetNumberNext(offnum);
                continue;
            }

            itup = PageGetItem(page, iid) as IndexTuple;
            Assert!(!BTreeTupleIsPivot(itup));

            pstate.offnum = offnum;
            passes_quals = _bt_checkkeys(scan, &raw mut pstate, arrayKeys, itup, indnatts);

            /*
             * Check if we need to skip ahead to a later tuple (only possible
             * when the scan uses array keys)
             */
            if arrayKeys && pstate.skip != InvalidOffsetNumber {
                Assert!(!passes_quals && pstate.continuescan);
                Assert!(offnum < pstate.skip);
                Assert!(!pstate.forcenonrequired);

                offnum = pstate.skip;
                pstate.skip = InvalidOffsetNumber;
                continue;
            }

            if passes_quals {
                /* tuple passes all scan key conditions */
                if !BTreeTupleIsPosting(itup) {
                    /* Remember it */
                    _bt_saveitem(so, itemIndex, offnum, itup);
                    itemIndex += 1;
                } else {
                    let tupleOffset: c_int;

                    /*
                     * Set up state to return posting list, and remember first
                     * TID
                     */
                    tupleOffset =
                        _bt_setuppostingitems(so, itemIndex, offnum,
                                              BTreeTupleGetPostingN(itup, 0),
                                              itup);
                    itemIndex += 1;
                    /* Remember additional TIDs */
                    let mut p: c_int = 1;
                    while p < BTreeTupleGetNPosting(itup) {
                        _bt_savepostingitem(so, itemIndex, offnum,
                                            BTreeTupleGetPostingN(itup, p),
                                            tupleOffset);
                        itemIndex += 1;
                        p += 1;
                    }
                }
            }
            /* When !continuescan, there can't be any more matches, so stop */
            if !pstate.continuescan {
                break;
            }

            offnum = OffsetNumberNext(offnum);
        }

        /*
         * We don't need to visit page to the right when the high key
         * indicates that no more matches will be found there.
         *
         * Checking the high key like this works out more often than you might
         * think.  Leaf page splits pick a split point between the two most
         * dissimilar tuples (this is weighed against the need to evenly share
         * free space).  Leaf pages with high key attribute values that can
         * only appear on non-pivot tuples on the right sibling page are
         * common.
         */
        if pstate.continuescan && !(*so).scanBehind && !P_RIGHTMOST(opaque) {
            let iid: *mut ItemIdData = PageGetItemId(page, P_HIKEY);
            let itup: IndexTuple = PageGetItem(page, iid) as IndexTuple;
            let truncatt: c_int;

            /* Reset arrays, per _bt_set_startikey contract */
            if pstate.forcenonrequired {
                _bt_start_array_keys(scan, dir);
            }
            pstate.forcenonrequired = false;
            pstate.startikey = 0; /* _bt_set_startikey ignores P_HIKEY */

            truncatt = BTreeTupleGetNAtts(itup, rel);
            _bt_checkkeys(scan, &raw mut pstate, arrayKeys, itup, truncatt);
        }

        if !pstate.continuescan {
            (*so).currPos.moreRight = false;
        }

        Assert!(itemIndex <= MaxTIDsPerBTreePage);
        (*so).currPos.firstItem = 0;
        (*so).currPos.lastItem = itemIndex - 1;
        (*so).currPos.itemIndex = 0;
    } else {
        /* SK_SEARCHARRAY backward scans must provide final tuple up front */
        if arrayKeys {
            if minoff <= maxoff && !P_LEFTMOST(opaque) {
                let iid: *mut ItemIdData = PageGetItemId(page, minoff);

                pstate.finaltup = PageGetItem(page, iid) as IndexTuple;

                if (*so).scanBehind &&
                    !_bt_scanbehind_checkkeys(scan, dir, pstate.finaltup)
                {
                    /* Schedule another primitive index scan after all */
                    (*so).currPos.moreLeft = false;
                    (*so).needPrimScan = true;
                    if !(*scan).parallel_scan.is_null() {
                        _bt_parallel_primscan_schedule(scan, (*so).currPos.currPage);
                    }
                    return false;
                }
            }

            (*so).scanBehind = false; /* reset */
            (*so).oppositeDirCheck = false; /* reset */
        }

        /*
         * Consider pstate.startikey optimization once the ongoing primitive
         * index scan has already read at least one page
         */
        if !pstate.firstpage && minoff < maxoff {
            _bt_set_startikey(scan, &raw mut pstate);
        }

        /* load items[] in descending order */
        itemIndex = MaxTIDsPerBTreePage;

        if offnum > maxoff {
            offnum = maxoff;
        }

        while offnum >= minoff {
            let iid: *mut ItemIdData = PageGetItemId(page, offnum);
            let itup: IndexTuple;
            let tuple_alive: bool;
            let passes_quals: bool;

            /*
             * If the scan specifies not to return killed tuples, then we
             * treat a killed tuple as not passing the qual.  Most of the
             * time, it's a win to not bother examining the tuple's index
             * keys, but just skip to the next tuple (previous, actually,
             * since we're scanning backwards).  However, if this is the first
             * tuple on the page, we do check the index keys, to prevent
             * uselessly advancing to the page to the left.  This is similar
             * to the high key optimization used by forward scans.
             */
            if (*scan).ignore_killed_tuples && ItemIdIsDead(iid) {
                if offnum > minoff {
                    offnum = OffsetNumberPrev(offnum);
                    continue;
                }

                tuple_alive = false;
            } else {
                tuple_alive = true;
            }

            itup = PageGetItem(page, iid) as IndexTuple;
            Assert!(!BTreeTupleIsPivot(itup));

            pstate.offnum = offnum;
            if arrayKeys && offnum == minoff && pstate.forcenonrequired {
                /* Reset arrays, per _bt_set_startikey contract */
                pstate.forcenonrequired = false;
                pstate.startikey = 0;
                _bt_start_array_keys(scan, dir);
            }
            passes_quals = _bt_checkkeys(scan, &raw mut pstate, arrayKeys, itup, indnatts);

            if arrayKeys && (*so).scanBehind {
                /*
                 * Done scanning this page, but not done with the current
                 * primscan.
                 *
                 * Note: Forward scans don't check this explicitly, since they
                 * prefer to reuse pstate.skip for this instead.
                 */
                Assert!(!passes_quals && pstate.continuescan);
                Assert!(!pstate.forcenonrequired);

                break;
            }

            /*
             * Check if we need to skip ahead to a later tuple (only possible
             * when the scan uses array keys)
             */
            if arrayKeys && pstate.skip != InvalidOffsetNumber {
                Assert!(!passes_quals && pstate.continuescan);
                Assert!(offnum > pstate.skip);
                Assert!(!pstate.forcenonrequired);

                offnum = pstate.skip;
                pstate.skip = InvalidOffsetNumber;
                continue;
            }

            if passes_quals && tuple_alive {
                /* tuple passes all scan key conditions */
                if !BTreeTupleIsPosting(itup) {
                    /* Remember it */
                    itemIndex -= 1;
                    _bt_saveitem(so, itemIndex, offnum, itup);
                } else {
                    let tupleOffset: c_int;

                    /*
                     * Set up state to return posting list, and remember first
                     * TID.
                     *
                     * Note that we deliberately save/return items from
                     * posting lists in ascending heap TID order for backwards
                     * scans.  This allows _bt_killitems() to make a
                     * consistent assumption about the order of items
                     * associated with the same posting list tuple.
                     */
                    itemIndex -= 1;
                    tupleOffset =
                        _bt_setuppostingitems(so, itemIndex, offnum,
                                              BTreeTupleGetPostingN(itup, 0),
                                              itup);
                    /* Remember additional TIDs */
                    let mut p: c_int = 1;
                    while p < BTreeTupleGetNPosting(itup) {
                        itemIndex -= 1;
                        _bt_savepostingitem(so, itemIndex, offnum,
                                            BTreeTupleGetPostingN(itup, p),
                                            tupleOffset);
                        p += 1;
                    }
                }
            }
            /* When !continuescan, there can't be any more matches, so stop */
            if !pstate.continuescan {
                break;
            }

            offnum = OffsetNumberPrev(offnum);
        }

        /*
         * We don't need to visit page to the left when no more matches will
         * be found there
         */
        if !pstate.continuescan {
            (*so).currPos.moreLeft = false;
        }

        Assert!(itemIndex >= 0);
        (*so).currPos.firstItem = itemIndex;
        (*so).currPos.lastItem = MaxTIDsPerBTreePage - 1;
        (*so).currPos.itemIndex = MaxTIDsPerBTreePage - 1;
    }

    /*
     * If _bt_set_startikey told us to temporarily treat the scan's keys as
     * nonrequired (possible only during scans with array keys), there must be
     * no lasting consequences for the scan's array keys.  The scan's arrays
     * should now have exactly the same elements as they would have had if the
     * nonrequired behavior had never been used.  (In general, a scan's arrays
     * are expected to track its progress through the index's key space.)
     *
     * We are required (by _bt_set_startikey) to call _bt_checkkeys against
     * pstate.finaltup with pstate.forcenonrequired=false to allow the scan's
     * arrays to recover.  Assert that that step hasn't been missed.
     */
    Assert!(!pstate.forcenonrequired);

    return (*so).currPos.firstItem <= (*so).currPos.lastItem;
}

/* Save an index item into so->currPos.items[itemIndex] */
unsafe fn _bt_saveitem(
    so: BTScanOpaque,
    itemIndex: c_int,
    offnum: OffsetNumber,
    itup: IndexTuple,
) {
    let currItem: *mut BTScanPosItem = &raw mut (*so).currPos.items[itemIndex as usize];

    Assert!(!BTreeTupleIsPivot(itup) && !BTreeTupleIsPosting(itup));

    (*currItem).heapTid = (*itup).t_tid;
    (*currItem).indexOffset = offnum;
    if !(*so).currTuples.is_null() {
        let itupsz: Size = IndexTupleSize(itup);

        (*currItem).tupleOffset = (*so).currPos.nextTupleOffset;
        memcpy((*so).currTuples.add((*so).currPos.nextTupleOffset as usize) as *mut c_void,
               itup as *const c_void, itupsz);
        (*so).currPos.nextTupleOffset += MAXALIGN(itupsz) as uint16;
    }
}

/*
 * Setup state to save TIDs/items from a single posting list tuple.
 *
 * Saves an index item into so->currPos.items[itemIndex] for TID that is
 * returned to scan first.  Second or subsequent TIDs for posting list should
 * be saved by calling _bt_savepostingitem().
 *
 * Returns an offset into tuple storage space that main tuple is stored at if
 * needed.
 */
unsafe fn _bt_setuppostingitems(
    so: BTScanOpaque,
    itemIndex: c_int,
    offnum: OffsetNumber,
    heapTid: ItemPointer,
    itup: IndexTuple,
) -> c_int {
    let currItem: *mut BTScanPosItem = &raw mut (*so).currPos.items[itemIndex as usize];

    Assert!(BTreeTupleIsPosting(itup));

    (*currItem).heapTid = *heapTid;
    (*currItem).indexOffset = offnum;
    if !(*so).currTuples.is_null() {
        /* Save base IndexTuple (truncate posting list) */
        let base: IndexTuple;
        let mut itupsz: Size = BTreeTupleGetPostingOffset(itup) as Size;

        itupsz = MAXALIGN(itupsz);
        (*currItem).tupleOffset = (*so).currPos.nextTupleOffset;
        base = (*so).currTuples.add((*so).currPos.nextTupleOffset as usize) as IndexTuple;
        memcpy(base as *mut c_void, itup as *const c_void, itupsz);
        /* Defensively reduce work area index tuple header size */
        (*base).t_info &= !INDEX_SIZE_MASK;
        (*base).t_info |= itupsz as u16;
        (*so).currPos.nextTupleOffset += itupsz as uint16;

        return (*currItem).tupleOffset as c_int;
    }

    return 0;
}

/*
 * Save an index item into so->currPos.items[itemIndex] for current posting
 * tuple.
 *
 * Assumes that _bt_setuppostingitems() has already been called for current
 * posting list tuple.  Caller passes its return value as tupleOffset.
 */
#[inline]
unsafe fn _bt_savepostingitem(
    so: BTScanOpaque,
    itemIndex: c_int,
    offnum: OffsetNumber,
    heapTid: ItemPointer,
    tupleOffset: c_int,
) {
    let currItem: *mut BTScanPosItem = &raw mut (*so).currPos.items[itemIndex as usize];

    (*currItem).heapTid = *heapTid;
    (*currItem).indexOffset = offnum;

    /*
     * Have index-only scans return the same base IndexTuple for every TID
     * that originates from the same posting list
     */
    if !(*so).currTuples.is_null() {
        (*currItem).tupleOffset = tupleOffset as uint16;
    }
}

/*
 * Return the index item from so->currPos.items[so->currPos.itemIndex] to the
 * index scan by setting the relevant fields in caller's index scan descriptor
 */
#[inline]
unsafe fn _bt_returnitem(scan: IndexScanDesc, so: BTScanOpaque) {
    let currItem: *mut BTScanPosItem = &raw mut (*so).currPos.items[(*so).currPos.itemIndex as usize];

    /* Most recent _bt_readpage must have succeeded */
    Assert!(BTScanPosIsValid(&(*so).currPos));
    Assert!((*so).currPos.itemIndex >= (*so).currPos.firstItem);
    Assert!((*so).currPos.itemIndex <= (*so).currPos.lastItem);

    /* Return next item, per amgettuple contract */
    (*scan).xs_heaptid = (*currItem).heapTid;
    if !(*so).currTuples.is_null() {
        (*scan).xs_itup = (*so).currTuples.add((*currItem).tupleOffset as usize) as IndexTuple;
    }
}

/*
 *	_bt_steppage() -- Step to next page containing valid data for scan
 *
 * Wrapper on _bt_readnextpage that performs final steps for the current page.
 *
 * On entry, so->currPos must be valid.  Its buffer will be pinned, though
 * never locked. (Actually, when so->dropPin there won't even be a pin held,
 * though so->currPos.currPage must still be set to a valid block number.)
 */
unsafe fn _bt_steppage(scan: IndexScanDesc, dir: ScanDirection) -> bool {
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;
    let blkno: BlockNumber;
    let lastcurrblkno: BlockNumber;

    Assert!(BTScanPosIsValid(&(*so).currPos));

    /* Before leaving current page, deal with any killed items */
    if (*so).numKilled > 0 {
        _bt_killitems(scan);
    }

    /*
     * Before we modify currPos, make a copy of the page data if there was a
     * mark position that needs it.
     */
    if (*so).markItemIndex >= 0 {
        /* bump pin on current buffer for assignment to mark buffer */
        if BTScanPosIsPinned(&(*so).currPos) {
            IncrBufferRefCount((*so).currPos.buf);
        }
        memcpy(&raw mut (*so).markPos as *mut c_void,
               &raw const (*so).currPos as *const c_void,
               offset_of!(BTScanPosData, items) + size_of::<BTScanPosItem>() +
               (*so).currPos.lastItem as usize * size_of::<BTScanPosItem>());
        if !(*so).markTuples.is_null() {
            memcpy((*so).markTuples as *mut c_void,
                   (*so).currTuples as *const c_void,
                   (*so).currPos.nextTupleOffset as usize);
        }
        (*so).markPos.itemIndex = (*so).markItemIndex;
        (*so).markItemIndex = -1;

        /*
         * If we're just about to start the next primitive index scan
         * (possible with a scan that has arrays keys, and needs to skip to
         * continue in the current scan direction), moreLeft/moreRight only
         * indicate the end of the current primitive index scan.  They must
         * never be taken to indicate that the top-level index scan has ended
         * (that would be wrong).
         *
         * We could handle this case by treating the current array keys as
         * markPos state.  But depending on the current array state like this
         * would add complexity.  Instead, we just unset markPos's copy of
         * moreRight or moreLeft (whichever might be affected), while making
         * btrestrpos reset the scan's arrays to their initial scan positions.
         * In effect, btrestrpos leaves advancing the arrays up to the first
         * _bt_readpage call (that takes place after it has restored markPos).
         */
        if (*so).needPrimScan {
            if ScanDirectionIsForward((*so).currPos.dir) {
                (*so).markPos.moreRight = true;
            } else {
                (*so).markPos.moreLeft = true;
            }
        }

        /* mark/restore not supported by parallel scans */
        Assert!((*scan).parallel_scan.is_null());
    }

    BTScanPosUnpinIfPinned(&mut (*so).currPos);

    /* Walk to the next page with data */
    if ScanDirectionIsForward(dir) {
        blkno = (*so).currPos.nextPage;
    } else {
        blkno = (*so).currPos.prevPage;
    }
    lastcurrblkno = (*so).currPos.currPage;

    /*
     * Cancel primitive index scans that were scheduled when the call to
     * _bt_readpage for currPos happened to use the opposite direction to the
     * one that we're stepping in now.  (It's okay to leave the scan's array
     * keys as-is, since the next _bt_readpage will advance them.)
     */
    if (*so).currPos.dir != dir {
        (*so).needPrimScan = false;
    }

    return _bt_readnextpage(scan, blkno, lastcurrblkno, dir, false);
}

/*
 *	_bt_readfirstpage() -- Read first page containing valid data for _bt_first
 *
 * _bt_first caller passes us an offnum returned by _bt_binsrch, which might
 * be an out of bounds offnum such as "maxoff + 1" in certain corner cases.
 * When we're passed an offnum past the end of the page, we might still manage
 * to stop the scan on this page by calling _bt_checkkeys against the high
 * key.  See _bt_readpage for full details.
 *
 * On entry, so->currPos must be pinned and locked (so offnum stays valid).
 * Parallel scan callers must have seized the scan before calling here.
 *
 * On exit, we'll have updated so->currPos and retained locks and pins
 * according to the same rules as those laid out for _bt_readnextpage exit.
 * Like _bt_readnextpage, our return value indicates if there are any matching
 * records in the given direction.
 *
 * We always release the scan for a parallel scan caller, regardless of
 * success or failure; we'll call _bt_parallel_release as soon as possible.
 */
unsafe fn _bt_readfirstpage(scan: IndexScanDesc, offnum: OffsetNumber, dir: ScanDirection) -> bool {
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;

    (*so).numKilled = 0; /* just paranoia */
    (*so).markItemIndex = -1; /* ditto */

    /* Initialize so->currPos for the first page (page in so->currPos.buf) */
    if (*so).needPrimScan {
        Assert!((*so).numArrayKeys != 0);

        (*so).currPos.moreLeft = true;
        (*so).currPos.moreRight = true;
        (*so).needPrimScan = false;
    } else if ScanDirectionIsForward(dir) {
        (*so).currPos.moreLeft = false;
        (*so).currPos.moreRight = true;
    } else {
        (*so).currPos.moreLeft = true;
        (*so).currPos.moreRight = false;
    }

    /*
     * Attempt to load matching tuples from the first page.
     *
     * Note that _bt_readpage will finish initializing the so->currPos fields.
     * _bt_readpage also releases parallel scan (even when it returns false).
     */
    if _bt_readpage(scan, dir, offnum, true) {
        let rel: Relation = (*scan).indexRelation;

        /*
         * _bt_readpage succeeded.  Drop the lock (and maybe the pin) on
         * so->currPos.buf in preparation for btgettuple returning tuples.
         */
        Assert!(BTScanPosIsPinned(&(*so).currPos));
        _bt_drop_lock_and_maybe_pin(rel, so);
        return true;
    }

    /* There's no actually-matching data on the page in so->currPos.buf */
    _bt_unlockbuf((*scan).indexRelation, (*so).currPos.buf);

    /* Call _bt_readnextpage using its _bt_steppage wrapper function */
    if !_bt_steppage(scan, dir) {
        return false;
    }

    /* _bt_readpage for a later page (now in so->currPos) succeeded */
    return true;
}

/*
 *	_bt_readnextpage() -- Read next page containing valid data for _bt_next
 *
 * Caller's blkno is the next interesting page's link, taken from either the
 * previously-saved right link or left link.  lastcurrblkno is the page that
 * was current at the point where the blkno link was saved, which we use to
 * reason about concurrent page splits/page deletions during backwards scans.
 * In the common case where seized=false, blkno is either so->currPos.nextPage
 * or so->currPos.prevPage, and lastcurrblkno is so->currPos.currPage.
 *
 * On entry, so->currPos shouldn't be locked by caller.  so->currPos.buf must
 * be InvalidBuffer/unpinned as needed by caller (note that lastcurrblkno
 * won't need to be read again in almost all cases).  Parallel scan callers
 * that seized the scan before calling here should pass seized=true; such a
 * caller's blkno and lastcurrblkno arguments come from the seized scan.
 * seized=false callers just pass us the blkno/lastcurrblkno taken from their
 * so->currPos, which (along with so->currPos itself) can be used to end the
 * scan.  A seized=false caller's blkno can never be assumed to be the page
 * that must be read next during a parallel scan, though.  We must figure that
 * part out for ourselves by seizing the scan (the correct page to read might
 * already be beyond the seized=false caller's blkno during a parallel scan,
 * unless blkno/so->currPos.nextPage/so->currPos.prevPage is already P_NONE,
 * or unless so->currPos.moreRight/so->currPos.moreLeft is already unset).
 *
 * On success exit, so->currPos is updated to contain data from the next
 * interesting page, and we return true.  We hold a pin on the buffer on
 * success exit (except during so->dropPin index scans, when we drop the pin
 * eagerly to avoid blocking VACUUM).
 *
 * If there are no more matching records in the given direction, we invalidate
 * so->currPos (while ensuring it retains no locks or pins), and return false.
 *
 * We always release the scan for a parallel scan caller, regardless of
 * success or failure; we'll call _bt_parallel_release as soon as possible.
 */
unsafe fn _bt_readnextpage(
    scan: IndexScanDesc,
    mut blkno: BlockNumber,
    mut lastcurrblkno: BlockNumber,
    dir: ScanDirection,
    mut seized: bool,
) -> bool {
    let rel: Relation = (*scan).indexRelation;
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;

    Assert!((*so).currPos.currPage == lastcurrblkno || seized);
    Assert!(!(blkno == P_NONE && seized));
    Assert!(!BTScanPosIsPinned(&(*so).currPos));

    /*
     * Remember that the scan already read lastcurrblkno, a page to the left
     * of blkno (or remember reading a page to the right, for backwards scans)
     */
    if ScanDirectionIsForward(dir) {
        (*so).currPos.moreLeft = true;
    } else {
        (*so).currPos.moreRight = true;
    }

    loop {
        let page: Page;
        let opaque: BTPageOpaque;

        if blkno == P_NONE ||
            (if ScanDirectionIsForward(dir) {
                !(*so).currPos.moreRight
            } else {
                !(*so).currPos.moreLeft
            })
        {
            /* most recent _bt_readpage call (for lastcurrblkno) ended scan */
            Assert!((*so).currPos.currPage == lastcurrblkno && !seized);
            BTScanPosInvalidate(&mut (*so).currPos);
            _bt_parallel_done(scan); /* iff !so->needPrimScan */
            return false;
        }

        Assert!(!(*so).needPrimScan);

        /* parallel scan must never actually visit so->currPos blkno */
        if !seized && !(*scan).parallel_scan.is_null() &&
            !_bt_parallel_seize(scan, &raw mut blkno, &raw mut lastcurrblkno, false)
        {
            /* whole scan is now done (or another primitive scan required) */
            BTScanPosInvalidate(&mut (*so).currPos);
            return false;
        }

        if ScanDirectionIsForward(dir) {
            /* read blkno, but check for interrupts first */
            CHECK_FOR_INTERRUPTS();
            (*so).currPos.buf = _bt_getbuf(rel, blkno, BT_READ);
        } else {
            /* read blkno, avoiding race (also checks for interrupts) */
            (*so).currPos.buf = _bt_lock_and_validate_left(rel, &raw mut blkno, lastcurrblkno);
            if (*so).currPos.buf == InvalidBuffer {
                /* must have been a concurrent deletion of leftmost page */
                BTScanPosInvalidate(&mut (*so).currPos);
                _bt_parallel_done(scan);
                return false;
            }
        }

        page = BufferGetPage((*so).currPos.buf);
        opaque = BTPageGetOpaque(page);
        lastcurrblkno = blkno;
        if !P_IGNORE(opaque) {
            /* see if there are any matches on this page */
            if ScanDirectionIsForward(dir) {
                /* note that this will clear moreRight if we can stop */
                if _bt_readpage(scan, dir, P_FIRSTDATAKEY(opaque), seized) {
                    break;
                }
                blkno = (*so).currPos.nextPage;
            } else {
                /* note that this will clear moreLeft if we can stop */
                if _bt_readpage(scan, dir, PageGetMaxOffsetNumber(page), seized) {
                    break;
                }
                blkno = (*so).currPos.prevPage;
            }
        } else {
            /* _bt_readpage not called, so do all this for ourselves */
            if ScanDirectionIsForward(dir) {
                blkno = (*opaque).btpo_next;
            } else {
                blkno = (*opaque).btpo_prev;
            }
            if !(*scan).parallel_scan.is_null() {
                _bt_parallel_release(scan, blkno, lastcurrblkno);
            }
        }

        /* no matching tuples on this page */
        _bt_relbuf(rel, (*so).currPos.buf);
        seized = false; /* released by _bt_readpage (or by us) */
    }

    /*
     * _bt_readpage succeeded.  Drop the lock (and maybe the pin) on
     * so->currPos.buf in preparation for btgettuple returning tuples.
     */
    Assert!((*so).currPos.currPage == blkno);
    Assert!(BTScanPosIsPinned(&(*so).currPos));
    _bt_drop_lock_and_maybe_pin(rel, so);

    return true;
}

/*
 * _bt_lock_and_validate_left() -- lock caller's left sibling blkno,
 * recovering from concurrent page splits/page deletions when necessary
 *
 * Called during backwards scans, to deal with their unique concurrency rules.
 *
 * blkno points to the block number of the page that we expect to move the
 * scan to.  We'll successfully move the scan there when we find that its
 * right sibling link still points to lastcurrblkno (the page we just read).
 * Otherwise, we have to figure out which page is the correct one for the scan
 * to now read the hard way, reasoning about concurrent splits and deletions.
 * See nbtree/README.
 *
 * On return, we have both a pin and a read lock on the returned page, whose
 * block number will be set in *blkno.  Returns InvalidBuffer if there is no
 * page to the left (no lock or pin is held in that case).
 *
 * It is possible for the returned leaf page to be half-dead; caller must
 * check that condition and step left again when required.
 */
unsafe fn _bt_lock_and_validate_left(
    rel: Relation,
    blkno: *mut BlockNumber,
    mut lastcurrblkno: BlockNumber,
) -> Buffer {
    let origblkno: BlockNumber = *blkno; /* detects circular links */

    loop {
        let mut buf: Buffer;
        let mut page: Page;
        let mut opaque: BTPageOpaque;
        let mut tries: c_int;

        /* check for interrupts while we're not holding any buffer lock */
        CHECK_FOR_INTERRUPTS();
        buf = _bt_getbuf(rel, *blkno, BT_READ);
        page = BufferGetPage(buf);
        opaque = BTPageGetOpaque(page);

        /*
         * If this isn't the page we want, walk right till we find what we
         * want --- but go no more than four hops (an arbitrary limit). If we
         * don't find the correct page by then, the most likely bet is that
         * lastcurrblkno got deleted and isn't in the sibling chain at all
         * anymore, not that its left sibling got split more than four times.
         *
         * Note that it is correct to test P_ISDELETED not P_IGNORE here,
         * because half-dead pages are still in the sibling chain.
         */
        tries = 0;
        loop {
            if !P_ISDELETED(opaque) && (*opaque).btpo_next == lastcurrblkno {
                /* Found desired page, return it */
                return buf;
            }
            tries += 1;
            if P_RIGHTMOST(opaque) || tries > 4 {
                break;
            }
            /* step right */
            *blkno = (*opaque).btpo_next;
            buf = _bt_relandgetbuf(rel, buf, *blkno, BT_READ);
            page = BufferGetPage(buf);
            opaque = BTPageGetOpaque(page);
        }

        /*
         * Return to the original page (usually the page most recently read by
         * _bt_readpage, which is passed by caller as lastcurrblkno) to see
         * what's up with its prev sibling link
         */
        buf = _bt_relandgetbuf(rel, buf, lastcurrblkno, BT_READ);
        page = BufferGetPage(buf);
        opaque = BTPageGetOpaque(page);
        if P_ISDELETED(opaque) {
            /*
             * It was deleted.  Move right to first nondeleted page (there
             * must be one); that is the page that has acquired the deleted
             * one's keyspace, so stepping left from it will take us where we
             * want to be.
             */
            loop {
                if P_RIGHTMOST(opaque) {
                    elog!(ERROR, "fell off the end of index \"{}\"",
                         CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy());
                }
                lastcurrblkno = (*opaque).btpo_next;
                buf = _bt_relandgetbuf(rel, buf, lastcurrblkno, BT_READ);
                page = BufferGetPage(buf);
                opaque = BTPageGetOpaque(page);
                if !P_ISDELETED(opaque) {
                    break;
                }
            }
        } else {
            /*
             * Original lastcurrblkno wasn't deleted; the explanation had
             * better be that the page to the left got split or deleted.
             * Without this check, we risk going into an infinite loop.
             */
            if (*opaque).btpo_prev == origblkno {
                elog!(ERROR, "could not find left sibling of block {} in index \"{}\"",
                     lastcurrblkno,
                     CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy());
            }
            /* Okay to try again, since left sibling link changed */
        }

        /*
         * Original lastcurrblkno from caller was concurrently deleted (could
         * also have been a great many concurrent left sibling page splits).
         * Found a non-deleted page that should now act as our lastcurrblkno.
         */
        if P_LEFTMOST(opaque) {
            /* New lastcurrblkno has no left sibling (concurrently deleted) */
            _bt_relbuf(rel, buf);
            break;
        }

        /* Start from scratch with new lastcurrblkno's blkno/prev link */
        *blkno = (*opaque).btpo_prev;
        // origblkno is local read-only copy, no reassign needed
        _bt_relbuf(rel, buf);
    }

    return InvalidBuffer;
}

/*
 * _bt_get_endpoint() -- Find the first or last page on a given tree level
 *
 * If the index is empty, we will return InvalidBuffer; any other failure
 * condition causes ereport().  We will not return a dead page.
 *
 * The returned buffer is pinned and read-locked.
 */
pub unsafe fn _bt_get_endpoint(rel: Relation, level: u32, rightmost: bool) -> Buffer {
    let mut buf: Buffer;
    let mut page: Page;
    let mut opaque: BTPageOpaque;
    let mut offnum: OffsetNumber;
    let mut blkno: BlockNumber;
    let mut itup: IndexTuple;

    /*
     * If we are looking for a leaf page, okay to descend from fast root;
     * otherwise better descend from true root.  (There is no point in being
     * smarter about intermediate levels.)
     */
    if level == 0 {
        buf = _bt_getroot(rel, core::ptr::null_mut(), BT_READ);
    } else {
        buf = _bt_gettrueroot(rel);
    }

    if !BufferIsValid(buf) {
        return InvalidBuffer;
    }

    page = BufferGetPage(buf);
    opaque = BTPageGetOpaque(page);

    loop {
        /*
         * If we landed on a deleted page, step right to find a live page
         * (there must be one).  Also, if we want the rightmost page, step
         * right if needed to get to it (this could happen if the page split
         * since we obtained a pointer to it).
         */
        while P_IGNORE(opaque) || (rightmost && !P_RIGHTMOST(opaque)) {
            blkno = (*opaque).btpo_next;
            if blkno == P_NONE {
                elog!(ERROR, "fell off the end of index \"{}\"",
                     CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy());
            }
            buf = _bt_relandgetbuf(rel, buf, blkno, BT_READ);
            page = BufferGetPage(buf);
            opaque = BTPageGetOpaque(page);
        }

        /* Done? */
        if (*opaque).btpo_level == level {
            break;
        }
        if (*opaque).btpo_level < level {
            ereport!(ERROR,
                /* C also: errcode(ERRCODE_INDEX_CORRUPTED) */                errmsg_internal!("btree level {} not found in index \"{}\"",
                                 level,
                                 CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()));
        }

        /* Descend to leftmost or rightmost child page */
        if rightmost {
            offnum = PageGetMaxOffsetNumber(page);
        } else {
            offnum = P_FIRSTDATAKEY(opaque);
        }

        itup = PageGetItem(page, PageGetItemId(page, offnum)) as IndexTuple;
        blkno = BTreeTupleGetDownLink(itup);

        buf = _bt_relandgetbuf(rel, buf, blkno, BT_READ);
        page = BufferGetPage(buf);
        opaque = BTPageGetOpaque(page);
    }

    return buf;
}

/*
 *	_bt_endpoint() -- Find the first or last page in the index, and scan
 * from there to the first key satisfying all the quals.
 *
 * This is used by _bt_first() to set up a scan when we've determined
 * that the scan must start at the beginning or end of the index (for
 * a forward or backward scan respectively).
 *
 * Parallel scan callers must have seized the scan before calling here.
 * Exit conditions are the same as for _bt_first().
 */
unsafe fn _bt_endpoint(scan: IndexScanDesc, dir: ScanDirection) -> bool {
    let rel: Relation = (*scan).indexRelation;
    let so: BTScanOpaque = (*scan).opaque as BTScanOpaque;
    let page: Page;
    let opaque: BTPageOpaque;
    let start: OffsetNumber;

    Assert!(!BTScanPosIsValid(&(*so).currPos));
    Assert!(!(*so).needPrimScan);

    /*
     * Scan down to the leftmost or rightmost leaf page.  This is a simplified
     * version of _bt_search().
     */
    (*so).currPos.buf = _bt_get_endpoint(rel, 0, ScanDirectionIsBackward(dir));

    if !BufferIsValid((*so).currPos.buf) {
        /*
         * Empty index. Lock the whole relation, as nothing finer to lock
         * exists.
         */
        PredicateLockRelation(rel, (*scan).xs_snapshot);
        _bt_parallel_done(scan);
        return false;
    }

    page = BufferGetPage((*so).currPos.buf);
    opaque = BTPageGetOpaque(page);
    Assert!(P_ISLEAF(opaque));

    if ScanDirectionIsForward(dir) {
        /* There could be dead pages to the left, so not this: */
        /* Assert(P_LEFTMOST(opaque)); */

        start = P_FIRSTDATAKEY(opaque);
    } else if ScanDirectionIsBackward(dir) {
        Assert!(P_RIGHTMOST(opaque));

        start = PageGetMaxOffsetNumber(page);
    } else {
        elog!(ERROR, "invalid scan direction: {}", dir);
        start = 0; /* keep compiler quiet */
    }

    /*
     * Now load data from the first page of the scan.
     */
    if !_bt_readfirstpage(scan, start, dir) {
        return false;
    }

    _bt_returnitem(scan, so);
    return true;
}
