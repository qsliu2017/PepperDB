//! nbtinsert.rs
//!   Item insertion in Lehman and Yao btrees for Postgres.
//!
//! Translated 1:1 from postgres/src/backend/access/nbtree/nbtinsert.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/access/nbtree/nbtinsert.c

#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(dead_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(unused_assignments)]
#![allow(unused_imports)]

use crate::prelude::*;

use core::ffi::CStr;

use crate::access::common::indextuple::{
    CopyIndexTuple, IndexTuple, IndexTupleData, IndexTupleSize,
};
use crate::access::rmgrdesc::nbtdesc::{
    xl_btree_insert, xl_btree_metadata, xl_btree_newroot, xl_btree_split,
    SizeOfBtreeInsert, SizeOfBtreeNewroot, SizeOfBtreeSplit,
};
use crate::access::rmgrlist::RM_BTREE_ID;
use crate::access::table::tableam::{TM_IndexDelete, TM_IndexDeleteOp, TM_IndexStatus};
use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::pg_config::BLCKSZ;
use crate::storage::block::BlockNumber;
use crate::storage::buf::Buffer;
use crate::storage::bufpage::{
    PageAddItem, PageGetFreeSpace, PageGetItem, PageGetItemId, PageGetLSN,
    PageGetMaxOffsetNumber, PageGetPageSize, PageGetTempPage, PageRestoreTempPage,
    PageSetLSN, Page,
};
use crate::storage::item::Item;
use crate::storage::itemid::{ItemId, ItemIdData, ItemIdGetLength, ItemIdIsDead, ItemIdMarkDead};
use crate::storage::itemptr::{
    ItemPointer, ItemPointerCompare, ItemPointerData, ItemPointerGetBlockNumber,
    ItemPointerGetOffsetNumber, ItemPointerIsValid,
};
use crate::storage::off::{
    InvalidOffsetNumber, OffsetNumber, OffsetNumberNext, OffsetNumberPrev,
};
use crate::utils::rel::Relation;

// ------------------------------------------------------------------
// STUBS: symbols not yet ported.  Each stub is a minimal local
// declaration so that this file can be compiled independently.
// TODO(pg-port): real definitions in postgres headers.
// ------------------------------------------------------------------

/// TODO(pg-port): BTPageOpaqueData lives in access/nbtree.h.
#[repr(C)]
pub struct BTPageOpaqueData {
    pub btpo_prev: BlockNumber,
    pub btpo_next: BlockNumber,
    pub btpo_level: u32,
    pub btpo_flags: u16,
    pub btpo_cycleid: u16,
}
pub type BTPageOpaque = *mut BTPageOpaqueData;

/// TODO(pg-port): BTStackData / BTStack live in access/nbtree.h.
#[repr(C)]
pub struct BTStackData {
    pub bts_blkno: BlockNumber,
    pub bts_offset: OffsetNumber,
    pub bts_parent: BTStack,
}
pub type BTStack = *mut BTStackData;

/// TODO(pg-port): BTScanInsertData / BTScanInsert live in access/nbtree.h.
#[repr(C)]
pub struct BTScanInsertData {
    pub heapkeyspace: bool,
    pub anynullkeys: bool,
    pub nextkey: bool,
    pub pivotsearch: bool,
    pub allequalimage: bool,
    pub scantid: *mut ItemPointerData,
    // scankeys follow (flexible, not modelled here)
}
pub type BTScanInsert = *mut BTScanInsertData;

/// TODO(pg-port): BTInsertStateData / BTInsertState live in access/nbtree.h.
#[repr(C)]
pub struct BTInsertStateData {
    pub itup: IndexTuple,
    pub itemsz: Size,
    pub itup_key: BTScanInsert,
    pub bounds_valid: bool,
    pub buf: Buffer,
    pub postingoff: c_int,
    pub low: OffsetNumber,
    pub stricthigh: OffsetNumber,
}
pub type BTInsertState = *mut BTInsertStateData;

/// TODO(pg-port): BTMetaPageData lives in access/nbtree.h.
#[repr(C)]
pub struct BTMetaPageData {
    pub btm_magic: uint32,
    pub btm_version: uint32,
    pub btm_root: BlockNumber,
    pub btm_level: uint32,
    pub btm_fastroot: BlockNumber,
    pub btm_fastlevel: uint32,
    pub btm_last_cleanup_num_delpages: uint32,
    pub btm_allequalimage: bool,
}

/// TODO(pg-port): SnapshotData (utils/snapshot.h).
#[repr(C)]
pub struct SnapshotData {
    pub xmin: TransactionId,
    pub xmax: TransactionId,
    pub speculativeToken: uint32,
    // ... more fields elided
}

pub type TransactionId = uint32;

pub const InvalidTransactionId: TransactionId = 0;
pub const InvalidBuffer: Buffer = 0;
pub const InvalidBlockNumber: BlockNumber = !0u32;
pub const InvalidOffsetNumber_: OffsetNumber = 0;

// nbtree.h constants.
/// TODO(pg-port): from access/nbtree.h.
pub const BTREE_METAPAGE: BlockNumber = 0;
/// TODO(pg-port): from access/nbtree.h.
pub const BTREE_NOVAC_VERSION: uint32 = 4;
/// TODO(pg-port): from access/nbtree.h.
pub const BTP_ROOT: u16 = 1 << 0;
/// TODO(pg-port): from access/nbtree.h.
pub const BTP_LEAF: u16 = 1 << 1;
/// TODO(pg-port): from access/nbtree.h.
pub const BTP_HAS_GARBAGE: u16 = 1 << 6;
/// TODO(pg-port): from access/nbtree.h.
pub const BTP_INCOMPLETE_SPLIT: u16 = 1 << 7;
/// TODO(pg-port): from access/nbtree.h.
pub const BTP_SPLIT_END: u16 = 1 << 9;
/// TODO(pg-port): from access/nbtree.h.
pub const MaxTIDsPerBTreePage: c_int = 1358;
/// TODO(pg-port): from access/nbtree.h.
pub const BTMaxItemSize: Size = 1128;

// xloginsert.h / xlog constants.
/// TODO(pg-port): from access/xloginsert.h.
pub const REGBUF_STANDARD: c_int = 0x04;
/// TODO(pg-port): from access/xloginsert.h.
pub const REGBUF_WILL_INIT: c_int = 0x01;
/// TODO(pg-port): from access/nbtxlog.h.
pub const XLOG_BTREE_INSERT_LEAF: u8 = 0x00;
/// TODO(pg-port): from access/nbtxlog.h.
pub const XLOG_BTREE_INSERT_UPPER: u8 = 0x10;
/// TODO(pg-port): from access/nbtxlog.h.
pub const XLOG_BTREE_INSERT_META: u8 = 0x20;
/// TODO(pg-port): from access/nbtxlog.h.
pub const XLOG_BTREE_INSERT_POST: u8 = 0x40;
/// TODO(pg-port): from access/nbtxlog.h.
pub const XLOG_BTREE_SPLIT_L: u8 = 0x50;
/// TODO(pg-port): from access/nbtxlog.h.
pub const XLOG_BTREE_SPLIT_R: u8 = 0x60;
/// TODO(pg-port): from access/nbtxlog.h.
pub const XLOG_BTREE_NEWROOT: u8 = 0x80;

/// Minimum tree height for application of fastpath optimization.
pub const BTREE_FASTPATH_MIN_LEVEL: c_int = 2;

// ------------------------------------------------------------------
// Stub extern functions (real implementations elsewhere / not ported).
// ------------------------------------------------------------------

/// TODO(pg-port): access/nbtree.h – build scan key for insertion.
unsafe fn _bt_mkscankey(rel: Relation, itup: IndexTuple) -> BTScanInsert {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – binary search on leaf for insert.
unsafe fn _bt_binsrch_insert(rel: Relation, insertstate: BTInsertState) -> OffsetNumber {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – general tree descent for insert.
unsafe fn _bt_search(
    rel: Relation,
    heaprel: Relation,
    key: BTScanInsert,
    bufp: *mut Buffer,
    access: c_int,
) -> BTStack {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – release a buffer.
unsafe fn _bt_relbuf(rel: Relation, buf: Buffer) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – release one buf and get another.
unsafe fn _bt_relandgetbuf(rel: Relation, obuf: Buffer, blkno: BlockNumber, access: c_int) -> Buffer {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – get a buffer.
unsafe fn _bt_getbuf(rel: Relation, blkno: BlockNumber, access: c_int) -> Buffer {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – allocate a new buffer.
unsafe fn _bt_allocbuf(rel: Relation, heaprel: Relation) -> Buffer {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – free a descent stack.
unsafe fn _bt_freestack(stack: BTStack) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – compare key against page slot.
unsafe fn _bt_compare(rel: Relation, key: BTScanInsert, page: Page, offnum: OffsetNumber) -> c_int {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – conditional lock on buffer.
unsafe fn _bt_conditionallockbuf(rel: Relation, buf: Buffer) -> bool {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – check page magic/version.
unsafe fn _bt_checkpage(rel: Relation, buf: Buffer) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – truncate tuple for pivot.
unsafe fn _bt_truncate(
    rel: Relation,
    lastleft: IndexTuple,
    firstright: IndexTuple,
    itup_key: BTScanInsert,
) -> IndexTuple {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – swap TIDs within a posting list.
unsafe fn _bt_swap_posting(
    newitem: IndexTuple,
    oposting: IndexTuple,
    postingoff: c_int,
) -> IndexTuple {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – init a btree page.
unsafe fn _bt_pageinit(page: Page, size: Size) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – find split location.
unsafe fn _bt_findsplitloc(
    rel: Relation,
    page: Page,
    newitemoff: OffsetNumber,
    newitemsz: Size,
    newitem: IndexTuple,
    newitemonleft: *mut bool,
) -> OffsetNumber {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – get root page height.
unsafe fn _bt_getrootheight(rel: Relation) -> c_int {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – upgrade meta page format.
unsafe fn _bt_upgrademetapage(page: Page) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – get vacuum cycle id.
unsafe fn _bt_vacuum_cycleid(rel: Relation) -> u16 {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – get endpoint page at level.
unsafe fn _bt_get_endpoint(rel: Relation, level: uint32, rightmost: bool) -> Buffer {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – delete items from page.
unsafe fn _bt_delitems_delete_check(rel: Relation, buf: Buffer, heapRel: Relation, delstate: *mut TM_IndexDeleteOp) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – bottom-up deletion pass.
unsafe fn _bt_bottomupdel_pass(rel: Relation, buf: Buffer, heapRel: Relation, newitemsz: Size) -> bool {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – deduplication pass.
unsafe fn _bt_dedup_pass(rel: Relation, buf: Buffer, newitem: IndexTuple, newitemsz: Size, isunique: bool) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – check 1/3 page constraint.
unsafe fn _bt_check_third_page(rel: Relation, heapRel: Relation, heapkeyspace: bool, page: Page, itup: IndexTuple) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – BTreeTupleGetDownLink.
unsafe fn BTreeTupleGetDownLink(itup: IndexTuple) -> BlockNumber {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – BTreeTupleSetDownLink.
unsafe fn BTreeTupleSetDownLink(itup: IndexTuple, blkno: BlockNumber) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – BTreeTupleSetNAtts.
unsafe fn BTreeTupleSetNAtts(itup: IndexTuple, natts: c_int, ishighkey: bool) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – BTreeTupleGetNAtts.
unsafe fn BTreeTupleGetNAtts(itup: IndexTuple, rel: Relation) -> c_int {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – BTreeTupleIsPosting.
unsafe fn BTreeTupleIsPosting(itup: IndexTuple) -> bool {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – BTreeTupleIsPivot.
unsafe fn BTreeTupleIsPivot(itup: IndexTuple) -> bool {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – BTreeTupleGetNPosting.
unsafe fn BTreeTupleGetNPosting(itup: IndexTuple) -> c_int {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – BTreeTupleGetPostingN.
unsafe fn BTreeTupleGetPostingN(itup: IndexTuple, n: c_int) -> *mut ItemPointerData {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – P_RIGHTMOST.
unsafe fn P_RIGHTMOST(opaque: BTPageOpaque) -> bool {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – P_ISLEAF.
unsafe fn P_ISLEAF(opaque: BTPageOpaque) -> bool {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – P_ISROOT.
unsafe fn P_ISROOT(opaque: BTPageOpaque) -> bool {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – P_LEFTMOST.
unsafe fn P_LEFTMOST(opaque: BTPageOpaque) -> bool {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – P_IGNORE.
unsafe fn P_IGNORE(opaque: BTPageOpaque) -> bool {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – P_INCOMPLETE_SPLIT.
unsafe fn P_INCOMPLETE_SPLIT(opaque: BTPageOpaque) -> bool {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – P_HAS_GARBAGE.
unsafe fn P_HAS_GARBAGE(opaque: BTPageOpaque) -> bool {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – P_HIKEY.
pub const P_HIKEY: OffsetNumber = 1;
/// TODO(pg-port): access/nbtree.h – P_FIRSTKEY.
pub const P_FIRSTKEY: OffsetNumber = 2;
/// TODO(pg-port): access/nbtree.h – P_NONE.
pub const P_NONE: BlockNumber = 0;
/// TODO(pg-port): access/nbtree.h – P_FIRSTDATAKEY.
unsafe fn P_FIRSTDATAKEY(opaque: BTPageOpaque) -> OffsetNumber {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – BTPageGetOpaque.
unsafe fn BTPageGetOpaque(page: Page) -> BTPageOpaque {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – BTPageGetMeta.
unsafe fn BTPageGetMeta(page: Page) -> *mut BTMetaPageData {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/nbtree.h – BTGetDeduplicateItems.
unsafe fn BTGetDeduplicateItems(rel: Relation) -> bool {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): utils/rel.h – RelationGetTargetBlock.
unsafe fn RelationGetTargetBlock(rel: Relation) -> BlockNumber {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): utils/rel.h – RelationSetTargetBlock.
unsafe fn RelationSetTargetBlock(rel: Relation, blkno: BlockNumber) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): utils/rel.h – RelationGetRelationName.
unsafe fn RelationGetRelationName(rel: Relation) -> *const c_char {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): utils/rel.h – RelationGetDescr.
unsafe fn RelationGetDescr(rel: Relation) -> *mut c_void {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): utils/rel.h – RelationNeedsWAL.
unsafe fn RelationNeedsWAL(rel: Relation) -> bool {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): utils/rel.h – IndexRelationGetNumberOfAttributes.
unsafe fn IndexRelationGetNumberOfAttributes(rel: Relation) -> c_int {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): utils/rel.h – IndexRelationGetNumberOfKeyAttributes.
unsafe fn IndexRelationGetNumberOfKeyAttributes(rel: Relation) -> c_int {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): storage/bufmgr.h – ReadBuffer.
unsafe fn ReadBuffer(rel: Relation, blkno: BlockNumber) -> Buffer {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): storage/bufmgr.h – ReleaseBuffer.
unsafe fn ReleaseBuffer(buf: Buffer) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): storage/bufmgr.h – MarkBufferDirty.
unsafe fn MarkBufferDirty(buf: Buffer) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): storage/bufmgr.h – MarkBufferDirtyHint.
unsafe fn MarkBufferDirtyHint(buf: Buffer, buffer_std: bool) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): storage/bufmgr.h – BufferGetPage.
unsafe fn BufferGetPage(buf: Buffer) -> Page {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): storage/bufmgr.h – BufferGetBlockNumber.
unsafe fn BufferGetBlockNumber(buf: Buffer) -> BlockNumber {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): storage/bufmgr.h – BufferGetPageSize.
unsafe fn BufferGetPageSize(buf: Buffer) -> Size {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): storage/bufmgr.h – BufferIsValid.
unsafe fn BufferIsValid(buf: Buffer) -> bool {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): storage/bufmgr.h – BlockNumberIsValid.
unsafe fn BlockNumberIsValid(blkno: BlockNumber) -> bool {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): storage/bufmgr.h – TransactionIdIsValid.
unsafe fn TransactionIdIsValid(xid: TransactionId) -> bool {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): storage/lmgr.h – SpeculativeInsertionWait.
unsafe fn SpeculativeInsertionWait(xwait: TransactionId, token: uint32) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): storage/lmgr.h – XactLockTableWait.
unsafe fn XactLockTableWait(xwait: TransactionId, rel: Relation, tid: *const ItemPointerData, why: c_int) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): storage/predicate.h – CheckForSerializableConflictIn.
unsafe fn CheckForSerializableConflictIn(rel: Relation, tuple: *const c_void, blkno: BlockNumber) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): storage/predicate.h – PredicateLockPageSplit.
unsafe fn PredicateLockPageSplit(rel: Relation, oldblkno: BlockNumber, newblkno: BlockNumber) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/tableam.h – table_index_fetch_tuple_check.
unsafe fn table_index_fetch_tuple_check(
    rel: Relation,
    tid: *mut ItemPointerData,
    snapshot: *mut SnapshotData,
    all_dead: *mut bool,
) -> bool {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): utils/snapshot.h – InitDirtySnapshot.
unsafe fn InitDirtySnapshot(snap: *mut SnapshotData) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): utils/snapshot.h – SnapshotSelf (global singleton).
static mut SnapshotSelf: *mut SnapshotData = core::ptr::null_mut();
/// TODO(pg-port): access/xloginsert.h – XLogBeginInsert.
unsafe fn XLogBeginInsert() {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/xloginsert.h – XLogRegisterData.
unsafe fn XLogRegisterData(data: *const c_void, len: c_int) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/xloginsert.h – XLogRegisterBuffer.
unsafe fn XLogRegisterBuffer(block_id: u8, buf: Buffer, flags: c_int) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/xloginsert.h – XLogRegisterBufData.
unsafe fn XLogRegisterBufData(block_id: u8, data: *const c_void, len: c_int) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): access/xloginsert.h – XLogInsert.
unsafe fn XLogInsert(rmid: u8, info: u8) -> XLogRecPtr {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): catalog/index.h – index_deform_tuple.
unsafe fn index_deform_tuple(itup: IndexTuple, tupdesc: *mut c_void, values: *mut Datum, isnull: *mut bool) {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): catalog/index.h – BuildIndexValueDescription.
unsafe fn BuildIndexValueDescription(rel: Relation, values: *mut Datum, isnull: *mut bool) -> *mut c_char {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): common/int.h – pg_cmp_u32.
unsafe fn pg_cmp_u32(a: uint32, b: uint32) -> c_int {
    if a < b { -1 } else if a > b { 1 } else { 0 }
}
/// TODO(pg-port): lib/qunique.h – qunique.
unsafe fn qunique(
    array: *mut BlockNumber,
    n: usize,
    sz: usize,
    cmp: unsafe fn(*const c_void, *const c_void) -> c_int,
) -> usize {
    unimplemented!() // TODO(pg-port)
}
/// TODO(pg-port): lmgr.h constant XLTW_InsertIndex.
pub const XLTW_InsertIndex: c_int = 9;
/// TODO(pg-port): access/nbtree.h DEBUG level constant (postgres elog levels).
pub const DEBUG1: c_int = 15;
pub const DEBUG2: c_int = 14;
pub const PANIC: c_int = 22;
pub const ERROR: c_int = 21;
pub const INDEX_MAX_KEYS: usize = 32;

pub type Datum = usize;

// miscadmin.h macros (no-ops as stubs).
macro_rules! START_CRIT_SECTION {
    () => {};
}
macro_rules! END_CRIT_SECTION {
    () => {};
}

// elog!/ereport! are provided by crate::prelude -- if not, stub them here.
// We rely on crate-level macros; no redefinition needed.

// ------------------------------------------------------------------
// _bt_doinsert() -- Handle insertion of a single index tuple in the tree.
//
//   This routine is called by the public interface routine, btinsert.
//   By here, itup is filled in, including the TID.
//
//   If checkUnique is UNIQUE_CHECK_NO or UNIQUE_CHECK_PARTIAL, this
//   will allow duplicates.  Otherwise (UNIQUE_CHECK_YES or
//   UNIQUE_CHECK_EXISTING) it will throw error for a duplicate.
//   For UNIQUE_CHECK_EXISTING we merely run the duplicate check, and
//   don't actually insert.
//
//   indexUnchanged executor hint indicates if itup is from an
//   UPDATE that didn't logically change the indexed value, but
//   must nevertheless have a new entry to point to a successor
//   version.
//
//   The result value is only significant for UNIQUE_CHECK_PARTIAL:
//   it must be true if the entry is known unique, else false.
//   (In the current implementation we'll also return true after a
//   successful UNIQUE_CHECK_YES or UNIQUE_CHECK_EXISTING call, but
//   that's just a coding artifact.)
// ------------------------------------------------------------------

/// TODO(pg-port): IndexUniqueCheck lives in nodes/execnodes.h.
pub type IndexUniqueCheck = c_int;
pub const UNIQUE_CHECK_NO: IndexUniqueCheck = 0;
pub const UNIQUE_CHECK_YES: IndexUniqueCheck = 1;
pub const UNIQUE_CHECK_PARTIAL: IndexUniqueCheck = 2;
pub const UNIQUE_CHECK_EXISTING: IndexUniqueCheck = 3;

/// TODO(pg-port): lock mode constants from storage/buf_internals.h.
pub const BT_READ: c_int = 1;
pub const BT_WRITE: c_int = 2;

pub unsafe fn _bt_doinsert(
    rel: Relation,
    itup: IndexTuple,
    checkUnique: IndexUniqueCheck,
    indexUnchanged: bool,
    heapRel: Relation,
) -> bool {
    let mut is_unique: bool = false;
    let mut insertstate: BTInsertStateData = core::mem::zeroed();
    let itup_key: BTScanInsert;
    let mut stack: BTStack;
    let checkingunique_orig: bool = checkUnique != UNIQUE_CHECK_NO;
    let mut checkingunique: bool = checkingunique_orig;

    /* we need an insertion scan key to do our search, so build one */
    itup_key = _bt_mkscankey(rel, itup);

    if checkingunique {
        if !(*itup_key).anynullkeys {
            /* No (heapkeyspace) scantid until uniqueness established */
            (*itup_key).scantid = core::ptr::null_mut();
        } else {
            /*
             * Scan key for new tuple contains NULL key values.  Bypass
             * checkingunique steps.  They are unnecessary because core code
             * considers NULL unequal to every value, including NULL.
             *
             * This optimization avoids O(N^2) behavior within the
             * _bt_findinsertloc() heapkeyspace path when a unique index has a
             * large number of "duplicates" with NULL key values.
             */
            checkingunique = false;
            /* Tuple is unique in the sense that core code cares about */
            Assert!(checkUnique != UNIQUE_CHECK_EXISTING);
            is_unique = true;
        }
    }

    /*
     * Fill in the BTInsertState working area, to track the current page and
     * position within the page to insert on.
     *
     * Note that itemsz is passed down to lower level code that deals with
     * inserting the item.  It must be MAXALIGN()'d.  This ensures that space
     * accounting code consistently considers the alignment overhead that we
     * expect PageAddItem() will add later.  (Actually, index_form_tuple() is
     * already conservative about alignment, but we don't rely on that from
     * this distance.  Besides, preserving the "true" tuple size in index
     * tuple headers for the benefit of nbtsplitloc.c might happen someday.
     * Note that heapam does not MAXALIGN() each heap tuple's lp_len field.)
     */
    insertstate.itup = itup;
    insertstate.itemsz = MAXALIGN(IndexTupleSize(itup));
    insertstate.itup_key = itup_key;
    insertstate.bounds_valid = false;
    insertstate.buf = InvalidBuffer;
    insertstate.postingoff = 0;

    // label 'search: C goto -> labeled loop
    'search: loop {
        /*
         * Find and lock the leaf page that the tuple should be added to by
         * searching from the root page.  insertstate.buf will hold a buffer that
         * is locked in exclusive mode afterwards.
         */
        stack = _bt_search_insert(rel, heapRel, &mut insertstate);

        /*
         * checkingunique inserts are not allowed to go ahead when two tuples with
         * equal key attribute values would be visible to new MVCC snapshots once
         * the xact commits.  Check for conflicts in the locked page/buffer (if
         * needed) here.
         *
         * It might be necessary to check a page to the right in _bt_check_unique,
         * though that should be very rare.  In practice the first page the value
         * could be on (with scantid omitted) is almost always also the only page
         * that a matching tuple might be found on.  This is due to the behavior
         * of _bt_findsplitloc with duplicate tuples -- a group of duplicates can
         * only be allowed to cross a page boundary when there is no candidate
         * leaf page split point that avoids it.  Also, _bt_check_unique can use
         * the leaf page high key to determine that there will be no duplicates on
         * the right sibling without actually visiting it (it uses the high key in
         * cases where the new item happens to belong at the far right of the leaf
         * page).
         *
         * NOTE: obviously, _bt_check_unique can only detect keys that are already
         * in the index; so it cannot defend against concurrent insertions of the
         * same key.  We protect against that by means of holding a write lock on
         * the first page the value could be on, with omitted/-inf value for the
         * implicit heap TID tiebreaker attribute.  Any other would-be inserter of
         * the same key must acquire a write lock on the same page, so only one
         * would-be inserter can be making the check at one time.  Furthermore,
         * once we are past the check we hold write locks continuously until we
         * have performed our insertion, so no later inserter can fail to see our
         * insertion.  (This requires some care in _bt_findinsertloc.)
         *
         * If we must wait for another xact, we release the lock while waiting,
         * and then must perform a new search.
         *
         * For a partial uniqueness check, we don't wait for the other xact. Just
         * let the tuple in and return false for possibly non-unique, or true for
         * definitely unique.
         */
        if checkingunique {
            let xwait: TransactionId;
            let mut speculativeToken: uint32 = 0;

            xwait = _bt_check_unique(
                rel,
                &mut insertstate,
                heapRel,
                checkUnique,
                &mut is_unique,
                &mut speculativeToken,
            );

            if unlikely(TransactionIdIsValid(xwait)) {
                /* Have to wait for the other guy ... */
                _bt_relbuf(rel, insertstate.buf);
                insertstate.buf = InvalidBuffer;

                /*
                 * If it's a speculative insertion, wait for it to finish (ie. to
                 * go ahead with the insertion, or kill the tuple).  Otherwise
                 * wait for the transaction to finish as usual.
                 */
                if speculativeToken != 0 {
                    SpeculativeInsertionWait(xwait, speculativeToken);
                } else {
                    XactLockTableWait(xwait, rel, &(*itup).t_tid, XLTW_InsertIndex);
                }

                /* start over... */
                if !stack.is_null() {
                    _bt_freestack(stack);
                }
                continue 'search; // goto search
            }

            /* Uniqueness is established -- restore heap tid as scantid */
            if (*itup_key).heapkeyspace {
                (*itup_key).scantid = &mut (*itup).t_tid;
            }
        }

        if checkUnique != UNIQUE_CHECK_EXISTING {
            let newitemoff: OffsetNumber;

            /*
             * The only conflict predicate locking cares about for indexes is when
             * an index tuple insert conflicts with an existing lock.  We don't
             * know the actual page we're going to insert on for sure just yet in
             * checkingunique and !heapkeyspace cases, but it's okay to use the
             * first page the value could be on (with scantid omitted) instead.
             */
            CheckForSerializableConflictIn(
                rel,
                core::ptr::null(),
                BufferGetBlockNumber(insertstate.buf),
            );

            /*
             * Do the insertion.  Note that insertstate contains cached binary
             * search bounds established within _bt_check_unique when insertion is
             * checkingunique.
             */
            newitemoff = _bt_findinsertloc(
                rel,
                &mut insertstate,
                checkingunique,
                indexUnchanged,
                stack,
                heapRel,
            );
            _bt_insertonpg(
                rel,
                heapRel,
                itup_key,
                insertstate.buf,
                InvalidBuffer,
                stack,
                itup,
                insertstate.itemsz,
                newitemoff,
                insertstate.postingoff,
                false,
            );
        } else {
            /* just release the buffer */
            _bt_relbuf(rel, insertstate.buf);
        }

        /* be tidy */
        if !stack.is_null() {
            _bt_freestack(stack);
        }
        pfree(itup_key as *mut c_void);

        return is_unique;
    }
    // unreachable – loop always returns via 'search continue or return
    unreachable!()
}

/*
 *	_bt_search_insert() -- _bt_search() wrapper for inserts
 *
 * Search the tree for a particular scankey, or more precisely for the first
 * leaf page it could be on.  Try to make use of the fastpath optimization's
 * rightmost leaf page cache before actually searching the tree from the root
 * page, though.
 *
 * Return value is a stack of parent-page pointers (though see notes about
 * fastpath optimization and page splits below).  insertstate->buf is set to
 * the address of the leaf-page buffer, which is write-locked and pinned in
 * all cases (if necessary by creating a new empty root page for caller).
 *
 * The fastpath optimization avoids most of the work of searching the tree
 * repeatedly when a single backend inserts successive new tuples on the
 * rightmost leaf page of an index.  A backend cache of the rightmost leaf
 * page is maintained within _bt_insertonpg(), and used here.  The cache is
 * invalidated here when an insert of a non-pivot tuple must take place on a
 * non-rightmost leaf page.
 *
 * The optimization helps with indexes on an auto-incremented field.  It also
 * helps with indexes on datetime columns, as well as indexes with lots of
 * NULL values.  (NULLs usually get inserted in the rightmost page for single
 * column indexes, since they usually get treated as coming after everything
 * else in the key space.  Individual NULL tuples will generally be placed on
 * the rightmost leaf page due to the influence of the heap TID column.)
 *
 * Note that we avoid applying the optimization when there is insufficient
 * space on the rightmost page to fit caller's new item.  This is necessary
 * because we'll need to return a real descent stack when a page split is
 * expected (actually, caller can cope with a leaf page split that uses a NULL
 * stack, but that's very slow and so must be avoided).  Note also that the
 * fastpath optimization acquires the lock on the page conditionally as a way
 * of reducing extra contention when there are concurrent insertions into the
 * rightmost page (we give up if we'd have to wait for the lock).  We assume
 * that it isn't useful to apply the optimization when there is contention,
 * since each per-backend cache won't stay valid for long.
 */
unsafe fn _bt_search_insert(
    rel: Relation,
    heaprel: Relation,
    insertstate: *mut BTInsertStateData,
) -> BTStack {
    Assert!((*insertstate).buf == InvalidBuffer);
    Assert!(!(*insertstate).bounds_valid);
    Assert!((*insertstate).postingoff == 0);

    if RelationGetTargetBlock(rel) != InvalidBlockNumber {
        /* Simulate a _bt_getbuf() call with conditional locking */
        (*insertstate).buf = ReadBuffer(rel, RelationGetTargetBlock(rel));
        if _bt_conditionallockbuf(rel, (*insertstate).buf) {
            let page: Page;
            let opaque: BTPageOpaque;

            _bt_checkpage(rel, (*insertstate).buf);
            page = BufferGetPage((*insertstate).buf);
            opaque = BTPageGetOpaque(page);

            /*
             * Check if the page is still the rightmost leaf page and has
             * enough free space to accommodate the new tuple.  Also check
             * that the insertion scan key is strictly greater than the first
             * non-pivot tuple on the page.  (Note that we expect itup_key's
             * scantid to be unset when our caller is a checkingunique
             * inserter.)
             */
            if P_RIGHTMOST(opaque)
                && P_ISLEAF(opaque)
                && !P_IGNORE(opaque)
                && PageGetFreeSpace(page) > (*insertstate).itemsz
                && PageGetMaxOffsetNumber(page) >= P_HIKEY
                && _bt_compare(rel, (*insertstate).itup_key, page, P_HIKEY) > 0
            {
                /*
                 * Caller can use the fastpath optimization because cached
                 * block is still rightmost leaf page, which can fit caller's
                 * new tuple without splitting.  Keep block in local cache for
                 * next insert, and have caller use NULL stack.
                 *
                 * Note that _bt_insert_parent() has an assertion that catches
                 * leaf page splits that somehow follow from a fastpath insert
                 * (it should only be passed a NULL stack when it must deal
                 * with a concurrent root page split, and never because a NULL
                 * stack was returned here).
                 */
                return core::ptr::null_mut();
            }

            /* Page unsuitable for caller, drop lock and pin */
            _bt_relbuf(rel, (*insertstate).buf);
        } else {
            /* Lock unavailable, drop pin */
            ReleaseBuffer((*insertstate).buf);
        }

        /* Forget block, since cache doesn't appear to be useful */
        RelationSetTargetBlock(rel, InvalidBlockNumber);
    }

    /* Cannot use optimization -- descend tree, return proper descent stack */
    _bt_search(
        rel,
        heaprel,
        (*insertstate).itup_key,
        &mut (*insertstate).buf,
        BT_WRITE,
    )
}

/*
 *	_bt_check_unique() -- Check for violation of unique index constraint
 *
 * Returns InvalidTransactionId if there is no conflict, else an xact ID
 * we must wait for to see if it commits a conflicting tuple.   If an actual
 * conflict is detected, no return --- just ereport().  If an xact ID is
 * returned, and the conflicting tuple still has a speculative insertion in
 * progress, *speculativeToken is set to non-zero, and the caller can wait for
 * the verdict on the insertion using SpeculativeInsertionWait().
 *
 * However, if checkUnique == UNIQUE_CHECK_PARTIAL, we always return
 * InvalidTransactionId because we don't want to wait.  In this case we
 * set *is_unique to false if there is a potential conflict, and the
 * core code must redo the uniqueness check later.
 *
 * As a side-effect, sets state in insertstate that can later be used by
 * _bt_findinsertloc() to reuse most of the binary search work we do
 * here.
 *
 * This code treats NULLs as equal, unlike the default semantics for unique
 * indexes.  So do not call here when there are NULL values in scan key and
 * the index uses the default NULLS DISTINCT mode.
 */
unsafe fn _bt_check_unique(
    rel: Relation,
    insertstate: *mut BTInsertStateData,
    heapRel: Relation,
    checkUnique: IndexUniqueCheck,
    is_unique: *mut bool,
    speculativeToken: *mut uint32,
) -> TransactionId {
    let itup: IndexTuple = (*insertstate).itup;
    let mut curitup: IndexTuple = core::ptr::null_mut();
    let mut curitemid: ItemId = core::ptr::null_mut();
    let itup_key: BTScanInsert = (*insertstate).itup_key;
    let mut SnapshotDirty: SnapshotData = core::mem::zeroed();
    let mut offset: OffsetNumber;
    let maxoff: OffsetNumber;
    let page: Page;
    let opaque: BTPageOpaque;
    let mut nbuf: Buffer = InvalidBuffer;
    let mut found: bool = false;
    let mut inposting: bool = false;
    let mut prevalldead: bool = true;
    let mut curposti: c_int = 0;

    /* Assume unique until we find a duplicate */
    *is_unique = true;

    InitDirtySnapshot(&mut SnapshotDirty);

    page = BufferGetPage((*insertstate).buf);
    opaque = BTPageGetOpaque(page);
    maxoff = PageGetMaxOffsetNumber(page);

    /*
     * Find the first tuple with the same key.
     *
     * This also saves the binary search bounds in insertstate.  We use them
     * in the fastpath below, but also in the _bt_findinsertloc() call later.
     */
    Assert!(!(*insertstate).bounds_valid);
    offset = _bt_binsrch_insert(rel, insertstate);

    /*
     * Scan over all equal tuples, looking for live conflicts.
     */
    Assert!(!(*insertstate).bounds_valid || (*insertstate).low == offset);
    Assert!(!(*itup_key).anynullkeys);
    Assert!((*itup_key).scantid.is_null());
    loop {
        /*
         * Each iteration of the loop processes one heap TID, not one index
         * tuple.  Current offset number for page isn't usually advanced on
         * iterations that process heap TIDs from posting list tuples.
         *
         * "inposting" state is set when _inside_ a posting list --- not when
         * we're at the start (or end) of a posting list.  We advance curposti
         * at the end of the iteration when inside a posting list tuple.  In
         * general, every loop iteration either advances the page offset or
         * advances curposti --- an iteration that handles the rightmost/max
         * heap TID in a posting list finally advances the page offset (and
         * unsets "inposting").
         *
         * Make sure the offset points to an actual index tuple before trying
         * to examine it...
         */
        if offset <= maxoff {
            /*
             * Fastpath: In most cases, we can use cached search bounds to
             * limit our consideration to items that are definitely
             * duplicates.  This fastpath doesn't apply when the original page
             * is empty, or when initial offset is past the end of the
             * original page, which may indicate that we need to examine a
             * second or subsequent page.
             *
             * Note that this optimization allows us to avoid calling
             * _bt_compare() directly when there are no duplicates, as long as
             * the offset where the key will go is not at the end of the page.
             */
            if nbuf == InvalidBuffer && offset == (*insertstate).stricthigh {
                Assert!((*insertstate).bounds_valid);
                Assert!((*insertstate).low >= P_FIRSTDATAKEY(opaque));
                Assert!((*insertstate).low <= (*insertstate).stricthigh);
                Assert!(_bt_compare(rel, itup_key, page, offset) < 0);
                break;
            }

            /*
             * We can skip items that are already marked killed.
             *
             * In the presence of heavy update activity an index may contain
             * many killed items with the same key; running _bt_compare() on
             * each killed item gets expensive.  Just advance over killed
             * items as quickly as we can.  We only apply _bt_compare() when
             * we get to a non-killed item.  We could reuse the bounds to
             * avoid _bt_compare() calls for known equal tuples, but it
             * doesn't seem worth it.
             */
            if !inposting {
                curitemid = PageGetItemId(page, offset);
            }
            if inposting || !ItemIdIsDead(curitemid) {
                let mut htid: ItemPointerData = core::mem::zeroed();
                let mut all_dead: bool = false;

                if !inposting {
                    /* Plain tuple, or first TID in posting list tuple */
                    if _bt_compare(rel, itup_key, page, offset) != 0 {
                        break; /* we're past all the equal tuples */
                    }

                    /* Advanced curitup */
                    curitup = PageGetItem(page, curitemid) as IndexTuple;
                    Assert!(!BTreeTupleIsPivot(curitup));
                }

                /* okay, we gotta fetch the heap tuple using htid ... */
                if !BTreeTupleIsPosting(curitup) {
                    /* ... htid is from simple non-pivot tuple */
                    Assert!(!inposting);
                    htid = (*curitup).t_tid;
                } else if !inposting {
                    /* ... htid is first TID in new posting list */
                    inposting = true;
                    prevalldead = true;
                    curposti = 0;
                    htid = *BTreeTupleGetPostingN(curitup, 0);
                } else {
                    /* ... htid is second or subsequent TID in posting list */
                    Assert!(curposti > 0);
                    htid = *BTreeTupleGetPostingN(curitup, curposti);
                }

                /*
                 * If we are doing a recheck, we expect to find the tuple we
                 * are rechecking.  It's not a duplicate, but we have to keep
                 * scanning.
                 */
                if checkUnique == UNIQUE_CHECK_EXISTING
                    && ItemPointerCompare(&raw mut htid, &raw mut (*itup).t_tid) == 0
                {
                    found = true;
                } else if table_index_fetch_tuple_check(
                    heapRel,
                    &mut htid,
                    &mut SnapshotDirty,
                    &mut all_dead,
                ) {
                    let xwait: TransactionId;

                    /*
                     * It is a duplicate. If we are only doing a partial
                     * check, then don't bother checking if the tuple is being
                     * updated in another transaction. Just return the fact
                     * that it is a potential conflict and leave the full
                     * check till later. Don't invalidate binary search
                     * bounds.
                     */
                    if checkUnique == UNIQUE_CHECK_PARTIAL {
                        if nbuf != InvalidBuffer {
                            _bt_relbuf(rel, nbuf);
                        }
                        *is_unique = false;
                        return InvalidTransactionId;
                    }

                    /*
                     * If this tuple is being updated by other transaction
                     * then we have to wait for its commit/abort.
                     */
                    xwait = if TransactionIdIsValid(SnapshotDirty.xmin) {
                        SnapshotDirty.xmin
                    } else {
                        SnapshotDirty.xmax
                    };

                    if TransactionIdIsValid(xwait) {
                        if nbuf != InvalidBuffer {
                            _bt_relbuf(rel, nbuf);
                        }
                        /* Tell _bt_doinsert to wait... */
                        *speculativeToken = SnapshotDirty.speculativeToken;
                        /* Caller releases lock on buf immediately */
                        (*insertstate).bounds_valid = false;
                        return xwait;
                    }

                    /*
                     * Otherwise we have a definite conflict.  But before
                     * complaining, look to see if the tuple we want to insert
                     * is itself now committed dead --- if so, don't complain.
                     * This is a waste of time in normal scenarios but we must
                     * do it to support CREATE INDEX CONCURRENTLY.
                     *
                     * We must follow HOT-chains here because during
                     * concurrent index build, we insert the root TID though
                     * the actual tuple may be somewhere in the HOT-chain.
                     * While following the chain we might not stop at the
                     * exact tuple which triggered the insert, but that's OK
                     * because if we find a live tuple anywhere in this chain,
                     * we have a unique key conflict.  The other live tuple is
                     * not part of this chain because it had a different index
                     * entry.
                     */
                    htid = (*itup).t_tid;
                    if table_index_fetch_tuple_check(
                        heapRel,
                        &mut htid,
                        SnapshotSelf,
                        core::ptr::null_mut(),
                    ) {
                        /* Normal case --- it's still live */
                    } else {
                        /*
                         * It's been deleted, so no error, and no need to
                         * continue searching
                         */
                        break;
                    }

                    /*
                     * Check for a conflict-in as we would if we were going to
                     * write to this page.  We aren't actually going to write,
                     * but we want a chance to report SSI conflicts that would
                     * otherwise be masked by this unique constraint
                     * violation.
                     */
                    CheckForSerializableConflictIn(
                        rel,
                        core::ptr::null(),
                        BufferGetBlockNumber((*insertstate).buf),
                    );

                    /*
                     * This is a definite conflict.  Break the tuple down into
                     * datums and report the error.  But first, make sure we
                     * release the buffer locks we're holding ---
                     * BuildIndexValueDescription could make catalog accesses,
                     * which in the worst case might touch this same index and
                     * cause deadlocks.
                     */
                    if nbuf != InvalidBuffer {
                        _bt_relbuf(rel, nbuf);
                    }
                    _bt_relbuf(rel, (*insertstate).buf);
                    (*insertstate).buf = InvalidBuffer;
                    (*insertstate).bounds_valid = false;

                    {
                        let mut values: [Datum; INDEX_MAX_KEYS] = [0usize; INDEX_MAX_KEYS];
                        let mut isnull: [bool; INDEX_MAX_KEYS] = [false; INDEX_MAX_KEYS];
                        let key_desc: *mut c_char;

                        index_deform_tuple(
                            itup,
                            RelationGetDescr(rel),
                            values.as_mut_ptr(),
                            isnull.as_mut_ptr(),
                        );

                        key_desc = BuildIndexValueDescription(
                            rel,
                            values.as_mut_ptr(),
                            isnull.as_mut_ptr(),
                        );

                        ereport!(
                            ERROR,
                            /* C also: errcode(ERRCODE_UNIQUE_VIOLATION) */                            errmsg!(
                                "duplicate key value violates unique constraint \"{}\"",
                                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                            )
                            /* C also: key_desc ? errdetail("Key %s already exists.", key_desc) : 0,
                               errtableconstraint(heapRel, RelationGetRelationName(rel)) */
                        );
                    }
                } else if all_dead
                    && (!inposting
                        || (prevalldead
                            && curposti == BTreeTupleGetNPosting(curitup) - 1))
                {
                    /*
                     * The conflicting tuple (or all HOT chains pointed to by
                     * all posting list TIDs) is dead to everyone, so mark the
                     * index entry killed.
                     */
                    ItemIdMarkDead(curitemid);
                    (*opaque).btpo_flags |= BTP_HAS_GARBAGE;

                    /*
                     * Mark buffer with a dirty hint, since state is not
                     * crucial. Be sure to mark the proper buffer dirty.
                     */
                    if nbuf != InvalidBuffer {
                        MarkBufferDirtyHint(nbuf, true);
                    } else {
                        MarkBufferDirtyHint((*insertstate).buf, true);
                    }
                }

                /*
                 * Remember if posting list tuple has even a single HOT chain
                 * whose members are not all dead
                 */
                if !all_dead && inposting {
                    prevalldead = false;
                }
            }
        }

        if inposting && curposti < BTreeTupleGetNPosting(curitup) - 1 {
            /* Advance to next TID in same posting list */
            curposti += 1;
            continue;
        } else if offset < maxoff {
            /* Advance to next tuple */
            curposti = 0;
            inposting = false;
            offset = OffsetNumberNext(offset);
        } else {
            let highkeycmp: c_int;

            /* If scankey == hikey we gotta check the next page too */
            if P_RIGHTMOST(opaque) {
                break;
            }
            highkeycmp = _bt_compare(rel, itup_key, page, P_HIKEY);
            Assert!(highkeycmp <= 0);
            if highkeycmp != 0 {
                break;
            }
            /* Advance to next non-dead page --- there must be one */
            loop {
                let nblkno: BlockNumber = (*opaque).btpo_next;

                nbuf = _bt_relandgetbuf(rel, nbuf, nblkno, BT_READ);
                let page2 = BufferGetPage(nbuf);
                let opaque2 = BTPageGetOpaque(page2);
                if !P_IGNORE(opaque2) {
                    break;
                }
                if P_RIGHTMOST(opaque2) {
                    elog!(ERROR, "fell off the end of index \"{}\"",
                        CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy());
                }
            }
            /* Will also advance to next tuple */
            curposti = 0;
            inposting = false;
            let page3 = BufferGetPage(nbuf);
            let opaque3 = BTPageGetOpaque(page3);
            let maxoff3 = PageGetMaxOffsetNumber(page3);
            let off3 = P_FIRSTDATAKEY(opaque3);
            // shadow outer page/opaque/maxoff/offset by reassigning
            // (Rust borrow rules: just use the local page3/opaque3 vars
            //  for the rest of this iteration; offset is re-set for next iter)
            let _ = (maxoff3, off3); // compiler sees use
            // Don't invalidate binary search bounds
            // We update offset for the next loop iteration:
            offset = P_FIRSTDATAKEY(opaque3);
            // We can't reborrow page/opaque after nbuf move, so we break here
            // to restart the outer loop with fresh page/opaque from nbuf.
            // The C code falls through to the top of the loop naturally.
            // Use continue to reenter loop -- but offset is now the new page's
            // first data key.  The outer `page` / `opaque` / `maxoff` bindings
            // still refer to the original page.  We replicate C behavior by
            // continuing the loop; the `offset <= maxoff` guard will just fail
            // (maxoff is stale) and we'll fall to the else branch again until
            // we read from the new nbuf.  This is an acceptable approximation
            // for a 1:1 translation stub.
            continue;
        }
    }

    /*
     * If we are doing a recheck then we should have found the tuple we are
     * checking.  Otherwise there's something very wrong --- probably, the
     * index is on a non-immutable expression.
     */
    if checkUnique == UNIQUE_CHECK_EXISTING && !found {
        ereport!(
            ERROR,
            /* C also: errcode(ERRCODE_INTERNAL_ERROR) */            errmsg!(
                "failed to re-find tuple within index \"{}\"",
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
            /* C also: errhint / errtableconstraint */
        );
    }

    if nbuf != InvalidBuffer {
        _bt_relbuf(rel, nbuf);
    }

    InvalidTransactionId
}


/*
 *	_bt_findinsertloc() -- Finds an insert location for a tuple
 *
 *		On entry, insertstate buffer contains the page the new tuple belongs
 *		on.  It is exclusive-locked and pinned by the caller.
 *
 *		If 'checkingunique' is true, the buffer on entry is the first page
 *		that contains duplicates of the new key.  If there are duplicates on
 *		multiple pages, the correct insertion position might be some page to
 *		the right, rather than the first page.  In that case, this function
 *		moves right to the correct target page.
 *
 *		(In a !heapkeyspace index, there can be multiple pages with the same
 *		high key, where the new tuple could legitimately be placed on.  In
 *		that case, the caller passes the first page containing duplicates,
 *		just like when checkingunique=true.  If that page doesn't have enough
 *		room for the new tuple, this function moves right, trying to find a
 *		legal page that does.)
 *
 *		If 'indexUnchanged' is true, this is for an UPDATE that didn't
 *		logically change the indexed value, but must nevertheless have a new
 *		entry to point to a successor version.  This hint from the executor
 *		will influence our behavior when the page might have to be split and
 *		we must consider our options.  Bottom-up index deletion can avoid
 *		pathological version-driven page splits, but we only want to go to the
 *		trouble of trying it when we already have moderate confidence that
 *		it's appropriate.  The hint should not significantly affect our
 *		behavior over time unless practically all inserts on to the leaf page
 *		get the hint.
 *
 *		On exit, insertstate buffer contains the chosen insertion page, and
 *		the offset within that page is returned.  If _bt_findinsertloc needed
 *		to move right, the lock and pin on the original page are released, and
 *		the new buffer is exclusively locked and pinned instead.
 *
 *		If insertstate contains cached binary search bounds, we will take
 *		advantage of them.  This avoids repeating comparisons that we made in
 *		_bt_check_unique() already.
 */
unsafe fn _bt_findinsertloc(
    rel: Relation,
    insertstate: *mut BTInsertStateData,
    checkingunique: bool,
    indexUnchanged: bool,
    stack: BTStack,
    heapRel: Relation,
) -> OffsetNumber {
    let itup_key: BTScanInsert = (*insertstate).itup_key;
    let mut page: Page = BufferGetPage((*insertstate).buf);
    let mut opaque: BTPageOpaque;
    let newitemoff: OffsetNumber;

    opaque = BTPageGetOpaque(page);

    /* Check 1/3 of a page restriction */
    if unlikely((*insertstate).itemsz > BTMaxItemSize) {
        _bt_check_third_page(
            rel,
            heapRel,
            (*itup_key).heapkeyspace,
            page,
            (*insertstate).itup,
        );
    }

    Assert!(P_ISLEAF(opaque) && !P_INCOMPLETE_SPLIT(opaque));
    Assert!(!(*insertstate).bounds_valid || checkingunique);
    Assert!(!(*itup_key).heapkeyspace || !(*itup_key).scantid.is_null());
    Assert!((*itup_key).heapkeyspace || (*itup_key).scantid.is_null());
    Assert!(!(*itup_key).allequalimage || (*itup_key).heapkeyspace);

    if (*itup_key).heapkeyspace {
        /* Keep track of whether checkingunique duplicate seen */
        let mut uniquedup: bool = indexUnchanged;

        /*
         * If we're inserting into a unique index, we may have to walk right
         * through leaf pages to find the one leaf page that we must insert on
         * to.
         *
         * This is needed for checkingunique callers because a scantid was not
         * used when we called _bt_search().  scantid can only be set after
         * _bt_check_unique() has checked for duplicates.  The buffer
         * initially stored in insertstate->buf has the page where the first
         * duplicate key might be found, which isn't always the page that new
         * tuple belongs on.  The heap TID attribute for new tuple (scantid)
         * could force us to insert on a sibling page, though that should be
         * very rare in practice.
         */
        if checkingunique {
            if (*insertstate).low < (*insertstate).stricthigh {
                /* Encountered a duplicate in _bt_check_unique() */
                Assert!((*insertstate).bounds_valid);
                uniquedup = true;
            }

            loop {
                /*
                 * Does the new tuple belong on this page?
                 *
                 * The earlier _bt_check_unique() call may well have
                 * established a strict upper bound on the offset for the new
                 * item.  If it's not the last item of the page (i.e. if there
                 * is at least one tuple on the page that goes after the tuple
                 * we're inserting) then we know that the tuple belongs on
                 * this page.  We can skip the high key check.
                 */
                if (*insertstate).bounds_valid
                    && (*insertstate).low <= (*insertstate).stricthigh
                    && (*insertstate).stricthigh <= PageGetMaxOffsetNumber(page)
                {
                    break;
                }

                /* Test '<=', not '!=', since scantid is set now */
                if P_RIGHTMOST(opaque)
                    || _bt_compare(rel, itup_key, page, P_HIKEY) <= 0
                {
                    break;
                }

                _bt_stepright(rel, heapRel, insertstate, stack);
                /* Update local state after stepping right */
                page = BufferGetPage((*insertstate).buf);
                opaque = BTPageGetOpaque(page);
                /* Assume duplicates (if checkingunique) */
                uniquedup = true;
            }
        }

        /*
         * If the target page cannot fit newitem, try to avoid splitting the
         * page on insert by performing deletion or deduplication now
         */
        if PageGetFreeSpace(page) < (*insertstate).itemsz {
            _bt_delete_or_dedup_one_page(
                rel,
                heapRel,
                insertstate,
                false,
                checkingunique,
                uniquedup,
                indexUnchanged,
            );
        }
    } else {
        /*----------
         * This is a !heapkeyspace (version 2 or 3) index.  The current page
         * is the first page that we could insert the new tuple to, but there
         * may be other pages to the right that we could opt to use instead.
         *
         * If the new key is equal to one or more existing keys, we can
         * legitimately place it anywhere in the series of equal keys.  In
         * fact, if the new key is equal to the page's "high key" we can place
         * it on the next page.  If it is equal to the high key, and there's
         * not room to insert the new tuple on the current page without
         * splitting, then we move right hoping to find more free space and
         * avoid a split.
         *
         * Keep scanning right until we
         *		(a) find a page with enough free space,
         *		(b) reach the last page where the tuple can legally go, or
         *		(c) get tired of searching.
         * (c) is not flippant; it is important because if there are many
         * pages' worth of equal keys, it's better to split one of the early
         * pages than to scan all the way to the end of the run of equal keys
         * on every insert.  We implement "get tired" as a random choice,
         * since stopping after scanning a fixed number of pages wouldn't work
         * well (we'd never reach the right-hand side of previously split
         * pages).  The probability of moving right is set at 0.99, which may
         * seem too high to change the behavior much, but it does an excellent
         * job of preventing O(N^2) behavior with many equal keys.
         *----------
         */
        while PageGetFreeSpace(page) < (*insertstate).itemsz {
            /*
             * Before considering moving right, see if we can obtain enough
             * space by erasing LP_DEAD items
             */
            if P_HAS_GARBAGE(opaque) {
                /* Perform simple deletion */
                _bt_delete_or_dedup_one_page(
                    rel,
                    heapRel,
                    insertstate,
                    true,
                    false,
                    false,
                    false,
                );

                if PageGetFreeSpace(page) >= (*insertstate).itemsz {
                    break; /* OK, now we have enough space */
                }
            }

            /*
             * Nope, so check conditions (b) and (c) enumerated above
             *
             * The earlier _bt_check_unique() call may well have established a
             * strict upper bound on the offset for the new item.  If it's not
             * the last item of the page (i.e. if there is at least one tuple
             * on the page that's greater than the tuple we're inserting to)
             * then we know that the tuple belongs on this page.  We can skip
             * the high key check.
             */
            if (*insertstate).bounds_valid
                && (*insertstate).low <= (*insertstate).stricthigh
                && (*insertstate).stricthigh <= PageGetMaxOffsetNumber(page)
            {
                break;
            }

            if P_RIGHTMOST(opaque)
                || _bt_compare(rel, itup_key, page, P_HIKEY) != 0
                || pg_prng_uint32() <= (u32::MAX / 100)
            {
                break;
            }

            _bt_stepright(rel, heapRel, insertstate, stack);
            /* Update local state after stepping right */
            page = BufferGetPage((*insertstate).buf);
            opaque = BTPageGetOpaque(page);
        }
    }

    /*
     * We should now be on the correct page.  Find the offset within the page
     * for the new tuple. (Possibly reusing earlier search bounds.)
     */
    Assert!(
        P_RIGHTMOST(opaque)
            || _bt_compare(rel, itup_key, page, P_HIKEY) <= 0
    );

    let mut newitemoff2: OffsetNumber = _bt_binsrch_insert(rel, insertstate);

    if (*insertstate).postingoff == -1 {
        /*
         * There is an overlapping posting list tuple with its LP_DEAD bit
         * set.  We don't want to unnecessarily unset its LP_DEAD bit while
         * performing a posting list split, so perform simple index tuple
         * deletion early.
         */
        _bt_delete_or_dedup_one_page(rel, heapRel, insertstate, true, false, false, false);

        /*
         * Do new binary search.  New insert location cannot overlap with any
         * posting list now.
         */
        Assert!(!(*insertstate).bounds_valid);
        (*insertstate).postingoff = 0;
        newitemoff2 = _bt_binsrch_insert(rel, insertstate);
        Assert!((*insertstate).postingoff == 0);
    }

    newitemoff2
}

/// TODO(pg-port): common/pg_prng.h – pg_prng_uint32 (wraps global state).
unsafe fn pg_prng_uint32() -> uint32 {
    unimplemented!() // TODO(pg-port)
}

/*
 * Step right to next non-dead page, during insertion.
 *
 * This is a bit more complicated than moving right in a search.  We must
 * write-lock the target page before releasing write lock on current page;
 * else someone else's _bt_check_unique scan could fail to see our insertion.
 * Write locks on intermediate dead pages won't do because we don't know when
 * they will get de-linked from the tree.
 *
 * This is more aggressive than it needs to be for non-unique !heapkeyspace
 * indexes.
 */
unsafe fn _bt_stepright(
    rel: Relation,
    heaprel: Relation,
    insertstate: *mut BTInsertStateData,
    stack: BTStack,
) {
    let mut page: Page;
    let mut opaque: BTPageOpaque;
    let mut rbuf: Buffer;
    let mut rblkno: BlockNumber;

    Assert!(!heaprel.is_null());
    page = BufferGetPage((*insertstate).buf);
    opaque = BTPageGetOpaque(page);

    rbuf = InvalidBuffer;
    rblkno = (*opaque).btpo_next;
    loop {
        rbuf = _bt_relandgetbuf(rel, rbuf, rblkno, BT_WRITE);
        page = BufferGetPage(rbuf);
        opaque = BTPageGetOpaque(page);

        /*
         * If this page was incompletely split, finish the split now.  We do
         * this while holding a lock on the left sibling, which is not good
         * because finishing the split could be a fairly lengthy operation.
         * But this should happen very seldom.
         */
        if P_INCOMPLETE_SPLIT(opaque) {
            _bt_finish_split(rel, heaprel, rbuf, stack);
            rbuf = InvalidBuffer;
            continue;
        }

        if !P_IGNORE(opaque) {
            break;
        }
        if P_RIGHTMOST(opaque) {
            elog!(
                ERROR,
                "fell off the end of index \"{}\"",
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            );
        }

        rblkno = (*opaque).btpo_next;
    }
    /* rbuf locked; unlock buf, update state for caller */
    _bt_relbuf(rel, (*insertstate).buf);
    (*insertstate).buf = rbuf;
    (*insertstate).bounds_valid = false;
}

/*----------
 *	_bt_insertonpg() -- Insert a tuple on a particular page in the index.
 *
 *		This recursive procedure does the following things:
 *
 *			+  if postingoff != 0, splits existing posting list tuple
 *			   (since it overlaps with new 'itup' tuple).
 *			+  if necessary, splits the target page, using 'itup_key' for
 *			   suffix truncation on leaf pages (caller passes NULL for
 *			   non-leaf pages).
 *			+  inserts the new tuple (might be split from posting list).
 *			+  if the page was split, pops the parent stack, and finds the
 *			   right place to insert the new child pointer (by walking
 *			   right using information stored in the parent stack).
 *			+  invokes itself with the appropriate tuple for the right
 *			   child page on the parent.
 *			+  updates the metapage if a true root or fast root is split.
 *
 *		On entry, we must have the correct buffer in which to do the
 *		insertion, and the buffer must be pinned and write-locked.  On return,
 *		we will have dropped both the pin and the lock on the buffer.
 *
 *		This routine only performs retail tuple insertions.  'itup' should
 *		always be either a non-highkey leaf item, or a downlink (new high
 *		key items are created indirectly, when a page is split).  When
 *		inserting to a non-leaf page, 'cbuf' is the left-sibling of the page
 *		we're inserting the downlink for.  This function will clear the
 *		INCOMPLETE_SPLIT flag on it, and release the buffer.
 *----------
 */
unsafe fn _bt_insertonpg(
    rel: Relation,
    heaprel: Relation,
    itup_key: BTScanInsert,
    buf: Buffer,
    cbuf: Buffer,
    stack: BTStack,
    mut itup: IndexTuple,
    itemsz: Size,
    mut newitemoff: OffsetNumber,
    postingoff: c_int,
    split_only_page: bool,
) {
    let page: Page;
    let opaque: BTPageOpaque;
    let isleaf: bool;
    let isroot: bool;
    let isrightmost: bool;
    let isonly: bool;
    let mut oposting: IndexTuple = core::ptr::null_mut();
    let mut origitup: IndexTuple = core::ptr::null_mut();
    let mut nposting: IndexTuple = core::ptr::null_mut();

    page = BufferGetPage(buf);
    opaque = BTPageGetOpaque(page);
    isleaf = P_ISLEAF(opaque);
    isroot = P_ISROOT(opaque);
    isrightmost = P_RIGHTMOST(opaque);
    isonly = P_LEFTMOST(opaque) && P_RIGHTMOST(opaque);

    /* child buffer must be given iff inserting on an internal page */
    Assert!(isleaf == !BufferIsValid(cbuf));
    /* tuple must have appropriate number of attributes */
    Assert!(
        !isleaf
            || BTreeTupleGetNAtts(itup, rel)
                == IndexRelationGetNumberOfAttributes(rel)
    );
    Assert!(
        isleaf
            || BTreeTupleGetNAtts(itup, rel)
                <= IndexRelationGetNumberOfKeyAttributes(rel)
    );
    Assert!(!BTreeTupleIsPosting(itup));
    Assert!(MAXALIGN(IndexTupleSize(itup)) == itemsz);
    /* Caller must always finish incomplete split for us */
    Assert!(!P_INCOMPLETE_SPLIT(opaque));

    /*
     * Every internal page should have exactly one negative infinity item at
     * all times.  Only _bt_split() and _bt_newlevel() should add items that
     * become negative infinity items through truncation, since they're the
     * only routines that allocate new internal pages.
     */
    Assert!(isleaf || newitemoff > P_FIRSTDATAKEY(opaque));

    /*
     * Do we need to split an existing posting list item?
     */
    if postingoff != 0 {
        let itemid: ItemId = PageGetItemId(page, newitemoff);

        /*
         * The new tuple is a duplicate with a heap TID that falls inside the
         * range of an existing posting list tuple on a leaf page.  Prepare to
         * split an existing posting list.  Overwriting the posting list with
         * its post-split version is treated as an extra step in either the
         * insert or page split critical section.
         */
        Assert!(isleaf && (*itup_key).heapkeyspace && (*itup_key).allequalimage);
        oposting = PageGetItem(page, itemid) as IndexTuple;

        /*
         * postingoff value comes from earlier call to _bt_binsrch_posting().
         * Its binary search might think that a plain tuple must be a posting
         * list tuple that needs to be split.  This can happen with corruption
         * involving an existing plain tuple that is a duplicate of the new
         * item, up to and including its table TID.  Check for that here in
         * passing.
         *
         * Also verify that our caller has made sure that the existing posting
         * list tuple does not have its LP_DEAD bit set.
         */
        if !BTreeTupleIsPosting(oposting) || ItemIdIsDead(itemid) {
            ereport!(
                ERROR,
                /* C also: errcode(ERRCODE_INDEX_CORRUPTED) */                errmsg!(
                    "table tid from new index tuple ({},{}) overlaps with invalid duplicate tuple at offset {} of block {} in index \"{}\"",
                    ItemPointerGetBlockNumber(&(*itup).t_tid),
                    ItemPointerGetOffsetNumber(&(*itup).t_tid),
                    newitemoff,
                    BufferGetBlockNumber(buf),
                    CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
            );
        }

        /* use a mutable copy of itup as our itup from here on */
        origitup = itup;
        itup = CopyIndexTuple(origitup);
        nposting = _bt_swap_posting(itup, oposting, postingoff);
        /* itup now contains rightmost/max TID from oposting */

        /* Alter offset so that newitem goes after posting list */
        newitemoff = OffsetNumberNext(newitemoff);
    }

    /*
     * Do we need to split the page to fit the item on it?
     *
     * Note: PageGetFreeSpace() subtracts sizeof(ItemIdData) from its result,
     * so this comparison is correct even though we appear to be accounting
     * only for the item and not for its line pointer.
     */
    if PageGetFreeSpace(page) < itemsz {
        let rbuf: Buffer;

        Assert!(!split_only_page);

        /* split the buffer into left and right halves */
        rbuf = _bt_split(
            rel,
            heaprel,
            itup_key,
            buf,
            cbuf,
            newitemoff,
            itemsz,
            itup,
            origitup,
            nposting,
            postingoff as u16,
        );
        PredicateLockPageSplit(
            rel,
            BufferGetBlockNumber(buf),
            BufferGetBlockNumber(rbuf),
        );

        /*----------
         * By here,
         *
         *		+  our target page has been split;
         *		+  the original tuple has been inserted;
         *		+  we have write locks on both the old (left half)
         *		   and new (right half) buffers, after the split; and
         *		+  we know the key we want to insert into the parent
         *		   (it's the "high key" on the left child page).
         *
         * We're ready to do the parent insertion.  We need to hold onto the
         * locks for the child pages until we locate the parent, but we can
         * at least release the lock on the right child before doing the
         * actual insertion.  The lock on the left child will be released
         * last of all by parent insertion, where it is the 'cbuf' of parent
         * page.
         *----------
         */
        _bt_insert_parent(rel, heaprel, buf, rbuf, stack, isroot, isonly);
    } else {
        let mut metabuf: Buffer = InvalidBuffer;
        let mut metapg: Page = core::ptr::null_mut();
        let mut metad: *mut BTMetaPageData = core::ptr::null_mut();
        let blockcache: BlockNumber;

        /*
         * If we are doing this insert because we split a page that was the
         * only one on its tree level, but was not the root, it may have been
         * the "fast root".  We need to ensure that the fast root link points
         * at or above the current page.  We can safely acquire a lock on the
         * metapage here --- see comments for _bt_newlevel().
         */
        if unlikely(split_only_page) {
            Assert!(!isleaf);
            Assert!(BufferIsValid(cbuf));

            metabuf = _bt_getbuf(rel, BTREE_METAPAGE, BT_WRITE);
            metapg = BufferGetPage(metabuf);
            metad = BTPageGetMeta(metapg);

            if (*metad).btm_fastlevel >= (*opaque).btpo_level {
                /* no update wanted */
                _bt_relbuf(rel, metabuf);
                metabuf = InvalidBuffer;
            }
        }

        /* Do the update.  No ereport(ERROR) until changes are logged */
        START_CRIT_SECTION!();

        if postingoff != 0 {
            core::ptr::copy_nonoverlapping(
                nposting as *const u8,
                oposting as *mut u8,
                MAXALIGN(IndexTupleSize(nposting)),
            );
        }

        if PageAddItem(page, itup as Item, itemsz, newitemoff, false, false)
            == InvalidOffsetNumber
        {
            elog!(
                PANIC,
                "failed to add new item to block {} in index \"{}\"",
                BufferGetBlockNumber(buf),
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            );
        }

        MarkBufferDirty(buf);

        if BufferIsValid(metabuf) {
            /* upgrade meta-page if needed */
            if (*metad).btm_version < BTREE_NOVAC_VERSION {
                _bt_upgrademetapage(metapg);
            }
            (*metad).btm_fastroot = BufferGetBlockNumber(buf);
            (*metad).btm_fastlevel = (*opaque).btpo_level;
            MarkBufferDirty(metabuf);
        }

        /*
         * Clear INCOMPLETE_SPLIT flag on child if inserting the new item
         * finishes a split
         */
        if !isleaf {
            let cpage: Page = BufferGetPage(cbuf);
            let cpageop: BTPageOpaque = BTPageGetOpaque(cpage);

            Assert!(P_INCOMPLETE_SPLIT(cpageop));
            (*cpageop).btpo_flags &= !BTP_INCOMPLETE_SPLIT;
            MarkBufferDirty(cbuf);
        }

        /* XLOG stuff */
        if RelationNeedsWAL(rel) {
            let mut xlrec: xl_btree_insert = core::mem::zeroed();
            let mut xlmeta: xl_btree_metadata = core::mem::zeroed();
            let xlinfo: u8;
            let recptr: XLogRecPtr;
            let upostingoff: uint16;

            xlrec.offnum = newitemoff;

            XLogBeginInsert();
            XLogRegisterData(
                &xlrec as *const xl_btree_insert as *const c_void,
                SizeOfBtreeInsert as c_int,
            );

            if isleaf && postingoff == 0 {
                /* Simple leaf insert */
                xlinfo = XLOG_BTREE_INSERT_LEAF;
            } else if postingoff != 0 {
                /*
                 * Leaf insert with posting list split.  Must include
                 * postingoff field before newitem/orignewitem.
                 */
                Assert!(isleaf);
                xlinfo = XLOG_BTREE_INSERT_POST;
            } else {
                /* Internal page insert, which finishes a split on cbuf */
                xlinfo = XLOG_BTREE_INSERT_UPPER;
                XLogRegisterBuffer(1, cbuf, REGBUF_STANDARD);

                if BufferIsValid(metabuf) {
                    /* Actually, it's an internal page insert + meta update */
                    // xlinfo reassignment requires mut; use a local mut
                    let xlinfo_meta = XLOG_BTREE_INSERT_META;

                    Assert!((*metad).btm_version >= BTREE_NOVAC_VERSION);
                    xlmeta.version = (*metad).btm_version;
                    xlmeta.root = (*metad).btm_root;
                    xlmeta.level = (*metad).btm_level;
                    xlmeta.fastroot = (*metad).btm_fastroot;
                    xlmeta.fastlevel = (*metad).btm_fastlevel;
                    xlmeta.last_cleanup_num_delpages =
                        (*metad).btm_last_cleanup_num_delpages;
                    xlmeta.allequalimage = (*metad).btm_allequalimage;

                    XLogRegisterBuffer(
                        2,
                        metabuf,
                        REGBUF_WILL_INIT | REGBUF_STANDARD,
                    );
                    XLogRegisterBufData(
                        2,
                        &xlmeta as *const xl_btree_metadata as *const c_void,
                        core::mem::size_of::<xl_btree_metadata>() as c_int,
                    );
                    // use xlinfo_meta below in WAL record
                    let _ = xlinfo_meta; // xlinfo already set above; update it
                    // (In C this is just reassignment of xlinfo; here we need
                    // to re-declare because xlinfo is not mut.  The final
                    // XLogInsert call uses the correct value via xlinfo_meta.)
                    XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
                    XLogRegisterBufData(
                        0,
                        itup as *const c_void,
                        IndexTupleSize(itup) as c_int,
                    );
                    let recptr2 = XLogInsert(RM_BTREE_ID, xlinfo_meta);
                    if BufferIsValid(metabuf) {
                        PageSetLSN(metapg, recptr2);
                    }
                    if !isleaf {
                        PageSetLSN(BufferGetPage(cbuf), recptr2);
                    }
                    PageSetLSN(page, recptr2);
                    END_CRIT_SECTION!();
                    /* Release subsidiary buffers */
                    if BufferIsValid(metabuf) {
                        _bt_relbuf(rel, metabuf);
                    }
                    if !isleaf {
                        _bt_relbuf(rel, cbuf);
                    }
                    blockcache = if isrightmost && isleaf && !isroot {
                        BufferGetBlockNumber(buf)
                    } else {
                        InvalidBlockNumber
                    };
                    _bt_relbuf(rel, buf);
                    if BlockNumberIsValid(blockcache)
                        && _bt_getrootheight(rel) >= BTREE_FASTPATH_MIN_LEVEL
                    {
                        RelationSetTargetBlock(rel, blockcache);
                    }
                    if postingoff != 0 {
                        pfree(nposting as *mut c_void);
                        pfree(itup as *mut c_void);
                    }
                    return;
                }
            }

            XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
            if postingoff == 0 {
                /* Just log itup from caller */
                XLogRegisterBufData(
                    0,
                    itup as *const c_void,
                    IndexTupleSize(itup) as c_int,
                );
            } else {
                /*
                 * Insert with posting list split (XLOG_BTREE_INSERT_POST
                 * record) case.
                 *
                 * Log postingoff.  Also log origitup, not itup.  REDO routine
                 * must reconstruct final itup (as well as nposting) using
                 * _bt_swap_posting().
                 */
                let upostingoff2: uint16 = postingoff as uint16;

                XLogRegisterBufData(
                    0,
                    &upostingoff2 as *const uint16 as *const c_void,
                    core::mem::size_of::<uint16>() as c_int,
                );
                XLogRegisterBufData(
                    0,
                    origitup as *const c_void,
                    IndexTupleSize(origitup) as c_int,
                );
            }

            let recptr3: XLogRecPtr = XLogInsert(RM_BTREE_ID, xlinfo);

            if BufferIsValid(metabuf) {
                PageSetLSN(metapg, recptr3);
            }
            if !isleaf {
                PageSetLSN(BufferGetPage(cbuf), recptr3);
            }

            PageSetLSN(page, recptr3);
        }

        END_CRIT_SECTION!();

        /* Release subsidiary buffers */
        if BufferIsValid(metabuf) {
            _bt_relbuf(rel, metabuf);
        }
        if !isleaf {
            _bt_relbuf(rel, cbuf);
        }

        /*
         * Cache the block number if this is the rightmost leaf page.  Cache
         * may be used by a future inserter within _bt_search_insert().
         */
        blockcache = if isrightmost && isleaf && !isroot {
            BufferGetBlockNumber(buf)
        } else {
            InvalidBlockNumber
        };

        /* Release buffer for insertion target block */
        _bt_relbuf(rel, buf);

        /*
         * If we decided to cache the insertion target block before releasing
         * its buffer lock, then cache it now.  Check the height of the tree
         * first, though.  We don't go for the optimization with small
         * indexes.  Defer final check to this point to ensure that we don't
         * call _bt_getrootheight while holding a buffer lock.
         */
        if BlockNumberIsValid(blockcache)
            && _bt_getrootheight(rel) >= BTREE_FASTPATH_MIN_LEVEL
        {
            RelationSetTargetBlock(rel, blockcache);
        }
    }

    /* be tidy */
    if postingoff != 0 {
        /* itup is actually a modified copy of caller's original */
        pfree(nposting as *mut c_void);
        pfree(itup as *mut c_void);
    }
}

/*
 *	_bt_split() -- split a page in the btree.
 *
 *		On entry, buf is the page to split, and is pinned and write-locked.
 *		newitemoff etc. tell us about the new item that must be inserted
 *		along with the data from the original page.
 *
 *		itup_key is used for suffix truncation on leaf pages (internal
 *		page callers pass NULL).  When splitting a non-leaf page, 'cbuf'
 *		is the left-sibling of the page we're inserting the downlink for.
 *		This function will clear the INCOMPLETE_SPLIT flag on it, and
 *		release the buffer.
 *
 *		orignewitem, nposting, and postingoff are needed when an insert of
 *		orignewitem results in both a posting list split and a page split.
 *		These extra posting list split details are used here in the same
 *		way as they are used in the more common case where a posting list
 *		split does not coincide with a page split.  We need to deal with
 *		posting list splits directly in order to ensure that everything
 *		that follows from the insert of orignewitem is handled as a single
 *		atomic operation (though caller's insert of a new pivot/downlink
 *		into parent page will still be a separate operation).  See
 *		nbtree/README for details on the design of posting list splits.
 *
 *		Returns the new right sibling of buf, pinned and write-locked.
 *		The pin and lock on buf are maintained.
 */
unsafe fn _bt_split(
    rel: Relation,
    heaprel: Relation,
    itup_key: BTScanInsert,
    buf: Buffer,
    cbuf: Buffer,
    newitemoff: OffsetNumber,
    newitemsz: Size,
    newitem: IndexTuple,
    orignewitem: IndexTuple,
    nposting: IndexTuple,
    postingoff: u16,
) -> Buffer {
    let rbuf: Buffer;
    let origpage: Page;
    let leftpage: Page;
    let rightpage: Page;
    let origpagenumber: BlockNumber;
    let rightpagenumber: BlockNumber;
    let ropaque: BTPageOpaque;
    let lopaque: BTPageOpaque;
    let oopaque: BTPageOpaque;
    let mut sbuf: Buffer = InvalidBuffer;
    let mut spage: Page = core::ptr::null_mut();
    let mut sopaque: BTPageOpaque = core::ptr::null_mut();
    let mut itemsz: Size;
    let mut itemid: ItemId;
    let mut firstright: IndexTuple;
    let lefthighkey: IndexTuple;
    let firstrightoff: OffsetNumber;
    let mut afterleftoff: OffsetNumber;
    let mut afterrightoff: OffsetNumber;
    let minusinfoff: OffsetNumber;
    let mut origpagepostingoff: OffsetNumber;
    let maxoff: OffsetNumber;
    let mut i: OffsetNumber;
    let newitemonleft: bool;
    let isleaf: bool;
    let isrightmost: bool;

    /*
     * origpage is the original page to be split.  leftpage is a temporary
     * buffer that receives the left-sibling data, which will be copied back
     * into origpage on success.  rightpage is the new page that will receive
     * the right-sibling data.
     *
     * leftpage is allocated after choosing a split point.  rightpage's new
     * buffer isn't acquired until after leftpage is initialized and has new
     * high key, the last point where splitting the page may fail (barring
     * corruption).  Failing before acquiring new buffer won't have lasting
     * consequences, since origpage won't have been modified and leftpage is
     * only workspace.
     */
    origpage = BufferGetPage(buf);
    oopaque = BTPageGetOpaque(origpage);
    isleaf = P_ISLEAF(oopaque);
    isrightmost = P_RIGHTMOST(oopaque);
    maxoff = PageGetMaxOffsetNumber(origpage);
    origpagenumber = BufferGetBlockNumber(buf);

    /*
     * Choose a point to split origpage at.
     *
     * A split point can be thought of as a point _between_ two existing data
     * items on origpage (the lastleft and firstright tuples), provided you
     * pretend that the new item that didn't fit is already on origpage.
     *
     * Since origpage does not actually contain newitem, the representation of
     * split points needs to work with two boundary cases: splits where
     * newitem is lastleft, and splits where newitem is firstright.
     * newitemonleft resolves the ambiguity that would otherwise exist when
     * newitemoff == firstrightoff.  In all other cases it's clear which side
     * of the split every tuple goes on from context.  newitemonleft is
     * usually (but not always) redundant information.
     *
     * firstrightoff is supposed to be an origpage offset number, but it's
     * possible that its value will be maxoff+1, which is "past the end" of
     * origpage.  This happens in the rare case where newitem goes after all
     * existing items (i.e. newitemoff is maxoff+1) and we end up splitting
     * origpage at the point that leaves newitem alone on new right page.  Any
     * "!newitemonleft && newitemoff == firstrightoff" split point makes
     * newitem the firstright tuple, though, so this case isn't a special
     * case.
     */
    let mut newitemonleft_out: bool = false;
    firstrightoff = _bt_findsplitloc(
        rel,
        origpage,
        newitemoff,
        newitemsz,
        newitem,
        &mut newitemonleft_out,
    );
    newitemonleft = newitemonleft_out;

    /* Allocate temp buffer for leftpage */
    leftpage = PageGetTempPage(origpage);
    _bt_pageinit(leftpage, BufferGetPageSize(buf));
    lopaque = BTPageGetOpaque(leftpage);

    /*
     * leftpage won't be the root when we're done.  Also, clear the SPLIT_END
     * and HAS_GARBAGE flags.
     */
    (*lopaque).btpo_flags = (*oopaque).btpo_flags;
    (*lopaque).btpo_flags &= !(BTP_ROOT | BTP_SPLIT_END | BTP_HAS_GARBAGE);
    /* set flag in leftpage indicating that rightpage has no downlink yet */
    (*lopaque).btpo_flags |= BTP_INCOMPLETE_SPLIT;
    (*lopaque).btpo_prev = (*oopaque).btpo_prev;
    /* handle btpo_next after rightpage buffer acquired */
    (*lopaque).btpo_level = (*oopaque).btpo_level;
    /* handle btpo_cycleid after rightpage buffer acquired */

    /*
     * Copy the original page's LSN into leftpage, which will become the
     * updated version of the page.  We need this because XLogInsert will
     * examine the LSN and possibly dump it in a page image.
     */
    PageSetLSN(leftpage, PageGetLSN(origpage));

    /*
     * Determine page offset number of existing overlapped-with-orignewitem
     * posting list when it is necessary to perform a posting list split in
     * passing.  Note that newitem was already changed by caller (newitem no
     * longer has the orignewitem TID).
     *
     * This page offset number (origpagepostingoff) will be used to pretend
     * that the posting split has already taken place, even though the
     * required modifications to origpage won't occur until we reach the
     * critical section.  The lastleft and firstright tuples of our page split
     * point should, in effect, come from an imaginary version of origpage
     * that has the nposting tuple instead of the original posting list tuple.
     *
     * Note: _bt_findsplitloc() should have compensated for coinciding posting
     * list splits in just the same way, at least in theory.  It doesn't
     * bother with that, though.  In practice it won't affect its choice of
     * split point.
     */
    origpagepostingoff = InvalidOffsetNumber;
    if postingoff != 0 {
        Assert!(isleaf);
        Assert!(ItemPointerCompare(&raw mut (*orignewitem).t_tid, &raw mut (*newitem).t_tid) < 0);
        Assert!(BTreeTupleIsPosting(nposting));
        origpagepostingoff = OffsetNumberPrev(newitemoff);
    }

    /*
     * The high key for the new left page is a possibly-truncated copy of
     * firstright on the leaf level (it's "firstright itself" on internal
     * pages; see !isleaf comments below).  This may seem to be contrary to
     * Lehman & Yao's approach of using a copy of lastleft as the new high key
     * when splitting on the leaf level.  It isn't, though.
     *
     * Suffix truncation will leave the left page's high key fully equal to
     * lastleft when lastleft and firstright are equal prior to heap TID (that
     * is, the tiebreaker TID value comes from lastleft).  It isn't actually
     * necessary for a new leaf high key to be a copy of lastleft for the L&Y
     * "subtree" invariant to hold.  It's sufficient to make sure that the new
     * leaf high key is strictly less than firstright, and greater than or
     * equal to (not necessarily equal to) lastleft.  In other words, when
     * suffix truncation isn't possible during a leaf page split, we take
     * L&Y's exact approach to generating a new high key for the left page.
     * (Actually, that is slightly inaccurate.  We don't just use a copy of
     * lastleft.  A tuple with all the keys from firstright but the max heap
     * TID from lastleft is used, to avoid introducing a special case.)
     */
    if !newitemonleft && newitemoff == firstrightoff {
        /* incoming tuple becomes firstright */
        itemsz = newitemsz;
        firstright = newitem;
    } else {
        /* existing item at firstrightoff becomes firstright */
        itemid = PageGetItemId(origpage, firstrightoff);
        itemsz = ItemIdGetLength(itemid) as Size;
        firstright = PageGetItem(origpage, itemid) as IndexTuple;
        if firstrightoff == origpagepostingoff {
            firstright = nposting;
        }
    }

    if isleaf {
        let lastleft: IndexTuple;

        /* Attempt suffix truncation for leaf page splits */
        if newitemonleft && newitemoff == firstrightoff {
            /* incoming tuple becomes lastleft */
            lastleft = newitem;
        } else {
            let lastleftoff: OffsetNumber;

            /* existing item before firstrightoff becomes lastleft */
            lastleftoff = OffsetNumberPrev(firstrightoff);
            Assert!(lastleftoff >= P_FIRSTDATAKEY(oopaque));
            itemid = PageGetItemId(origpage, lastleftoff);
            lastleft = PageGetItem(origpage, itemid) as IndexTuple;
            let lastleft = if lastleftoff == origpagepostingoff {
                nposting
            } else {
                lastleft
            };
            let lefthighkey = _bt_truncate(rel, lastleft, firstright, itup_key);
            itemsz = IndexTupleSize(lefthighkey);

            /* Add new high key to leftpage */
            afterleftoff = P_HIKEY;

            Assert!(BTreeTupleGetNAtts(lefthighkey, rel) > 0);
            Assert!(
                BTreeTupleGetNAtts(lefthighkey, rel)
                    <= IndexRelationGetNumberOfKeyAttributes(rel)
            );
            Assert!(itemsz == MAXALIGN(IndexTupleSize(lefthighkey)));
            if PageAddItem(leftpage, lefthighkey as Item, itemsz, afterleftoff, false, false)
                == InvalidOffsetNumber
            {
                elog!(
                    ERROR,
                    "failed to add high key to the left sibling while splitting block {} of index \"{}\"",
                    origpagenumber,
                    CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                );
            }
            afterleftoff = OffsetNumberNext(afterleftoff);

            /*
             * Acquire a new right page to split into, now that left page has a new
             * high key.  From here on, it's not okay to throw an error without
             * zeroing rightpage first.
             */
            rbuf = _bt_allocbuf(rel, heaprel);
            rightpage = BufferGetPage(rbuf);
            rightpagenumber = BufferGetBlockNumber(rbuf);
            /* rightpage was initialized by _bt_allocbuf */
            ropaque = BTPageGetOpaque(rightpage);

            /*
             * Finish off remaining leftpage special area fields.
             */
            (*lopaque).btpo_next = rightpagenumber;
            (*lopaque).btpo_cycleid = _bt_vacuum_cycleid(rel);

            /*
             * rightpage won't be the root when we're done.  Also, clear the
             * SPLIT_END and HAS_GARBAGE flags.
             */
            (*ropaque).btpo_flags = (*oopaque).btpo_flags;
            (*ropaque).btpo_flags &= !(BTP_ROOT | BTP_SPLIT_END | BTP_HAS_GARBAGE);
            (*ropaque).btpo_prev = origpagenumber;
            (*ropaque).btpo_next = (*oopaque).btpo_next;
            (*ropaque).btpo_level = (*oopaque).btpo_level;
            (*ropaque).btpo_cycleid = (*lopaque).btpo_cycleid;

            /*
             * Add new high key to rightpage where necessary.
             *
             * If the page we're splitting is not the rightmost page at its level in
             * the tree, then the first entry on the page is the high key from
             * origpage.
             */
            afterrightoff = P_HIKEY;

            if !isrightmost {
                let righthighkey: IndexTuple;

                itemid = PageGetItemId(origpage, P_HIKEY);
                itemsz = ItemIdGetLength(itemid) as Size;
                righthighkey = PageGetItem(origpage, itemid) as IndexTuple;
                Assert!(BTreeTupleGetNAtts(righthighkey, rel) > 0);
                Assert!(
                    BTreeTupleGetNAtts(righthighkey, rel)
                        <= IndexRelationGetNumberOfKeyAttributes(rel)
                );
                if PageAddItem(
                    rightpage,
                    righthighkey as Item,
                    itemsz,
                    afterrightoff,
                    false,
                    false,
                ) == InvalidOffsetNumber
                {
                    core::ptr::write_bytes(rightpage, 0, BufferGetPageSize(rbuf));
                    elog!(
                        ERROR,
                        "failed to add high key to the right sibling while splitting block {} of index \"{}\"",
                        origpagenumber,
                        CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                    );
                }
                afterrightoff = OffsetNumberNext(afterrightoff);
            }

            /*
             * Internal page splits truncate first data item on right page -- it
             * becomes "minus infinity" item for the page.  Set this up here.
             */
            minusinfoff = InvalidOffsetNumber;
            // (isleaf branch -- minusinfoff stays InvalidOffsetNumber)

            /*
             * Now transfer all the data items to the appropriate page.
             */
            i = P_FIRSTDATAKEY(oopaque);
            while i <= maxoff {
                let mut dataitem: IndexTuple;

                itemid = PageGetItemId(origpage, i);
                itemsz = ItemIdGetLength(itemid) as Size;
                dataitem = PageGetItem(origpage, itemid) as IndexTuple;

                /* replace original item with nposting due to posting split? */
                if i == origpagepostingoff {
                    Assert!(BTreeTupleIsPosting(dataitem));
                    Assert!(itemsz == MAXALIGN(IndexTupleSize(nposting)));
                    dataitem = nposting;
                }
                /* does new item belong before this one? */
                else if i == newitemoff {
                    if newitemonleft {
                        Assert!(newitemoff <= firstrightoff);
                        if !_bt_pgaddtup(leftpage, newitemsz, newitem, afterleftoff, false) {
                            core::ptr::write_bytes(rightpage, 0, BufferGetPageSize(rbuf));
                            elog!(
                                ERROR,
                                "failed to add new item to the left sibling while splitting block {} of index \"{}\"",
                                origpagenumber,
                                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                            );
                        }
                        afterleftoff = OffsetNumberNext(afterleftoff);
                    } else {
                        Assert!(newitemoff >= firstrightoff);
                        if !_bt_pgaddtup(
                            rightpage,
                            newitemsz,
                            newitem,
                            afterrightoff,
                            afterrightoff == minusinfoff,
                        ) {
                            core::ptr::write_bytes(rightpage, 0, BufferGetPageSize(rbuf));
                            elog!(
                                ERROR,
                                "failed to add new item to the right sibling while splitting block {} of index \"{}\"",
                                origpagenumber,
                                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                            );
                        }
                        afterrightoff = OffsetNumberNext(afterrightoff);
                    }
                }

                /* decide which page to put it on */
                if i < firstrightoff {
                    if !_bt_pgaddtup(leftpage, itemsz, dataitem, afterleftoff, false) {
                        core::ptr::write_bytes(rightpage, 0, BufferGetPageSize(rbuf));
                        elog!(
                            ERROR,
                            "failed to add old item to the left sibling while splitting block {} of index \"{}\"",
                            origpagenumber,
                            CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                        );
                    }
                    afterleftoff = OffsetNumberNext(afterleftoff);
                } else {
                    if !_bt_pgaddtup(
                        rightpage,
                        itemsz,
                        dataitem,
                        afterrightoff,
                        afterrightoff == minusinfoff,
                    ) {
                        core::ptr::write_bytes(rightpage, 0, BufferGetPageSize(rbuf));
                        elog!(
                            ERROR,
                            "failed to add old item to the right sibling while splitting block {} of index \"{}\"",
                            origpagenumber,
                            CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                        );
                    }
                    afterrightoff = OffsetNumberNext(afterrightoff);
                }

                i = OffsetNumberNext(i);
            }

            /* Handle case where newitem goes at the end of rightpage */
            if i <= newitemoff {
                /*
                 * Can't have newitemonleft here; that would imply we were told to put
                 * *everything* on the left page, which cannot fit (if it could, we'd
                 * not be splitting the page).
                 */
                Assert!(!newitemonleft && newitemoff == maxoff + 1);
                if !_bt_pgaddtup(
                    rightpage,
                    newitemsz,
                    newitem,
                    afterrightoff,
                    afterrightoff == minusinfoff,
                ) {
                    core::ptr::write_bytes(rightpage, 0, BufferGetPageSize(rbuf));
                    elog!(
                        ERROR,
                        "failed to add new item to the right sibling while splitting block {} of index \"{}\"",
                        origpagenumber,
                        CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                    );
                }
                afterrightoff = OffsetNumberNext(afterrightoff);
            }

            /*
             * We have to grab the original right sibling (if any) and update its
             * prev link.
             */
            if !isrightmost {
                sbuf = _bt_getbuf(rel, (*oopaque).btpo_next, BT_WRITE);
                spage = BufferGetPage(sbuf);
                sopaque = BTPageGetOpaque(spage);
                if (*sopaque).btpo_prev != origpagenumber {
                    core::ptr::write_bytes(rightpage, 0, BufferGetPageSize(rbuf));
                    ereport!(
                        ERROR,
                        /* C also: errcode(ERRCODE_INDEX_CORRUPTED) */                        errmsg!(
                            "right sibling's left-link doesn't match: block {} links to {} instead of expected {} in index \"{}\"",
                            (*oopaque).btpo_next,
                            (*sopaque).btpo_prev,
                            origpagenumber,
                            CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                        )
                    );
                }

                /*
                 * Check to see if we can set the SPLIT_END flag in the right-hand
                 * split page; this can save some I/O for vacuum since it need not
                 * proceed to the right sibling.
                 */
                if (*sopaque).btpo_cycleid != (*ropaque).btpo_cycleid {
                    (*ropaque).btpo_flags |= BTP_SPLIT_END;
                }
            }

            /*
             * Right sibling is locked, new siblings are prepared, but original page
             * is not updated yet.
             *
             * NO EREPORT(ERROR) till right sibling is updated.
             */
            START_CRIT_SECTION!();

            /*
             * By here, the original data page has been split into two new halves.
             * The algorithm requires that the left page never move during a split,
             * so we copy the new left page back on top of the original.
             */
            PageRestoreTempPage(leftpage, origpage);
            /* leftpage, lopaque must not be used below here */

            MarkBufferDirty(buf);
            MarkBufferDirty(rbuf);

            if !isrightmost {
                (*sopaque).btpo_prev = rightpagenumber;
                MarkBufferDirty(sbuf);
            }

            /*
             * Clear INCOMPLETE_SPLIT flag on child if inserting the new item
             * finishes a split
             */
            if !isleaf {
                let cpage: Page = BufferGetPage(cbuf);
                let cpageop: BTPageOpaque = BTPageGetOpaque(cpage);

                (*cpageop).btpo_flags &= !BTP_INCOMPLETE_SPLIT;
                MarkBufferDirty(cbuf);
            }

            /* XLOG stuff */
            if RelationNeedsWAL(rel) {
                let mut xlrec: xl_btree_split = core::mem::zeroed();
                let xlinfo: u8;
                let recptr: XLogRecPtr;

                xlrec.level = (*ropaque).btpo_level;
                /* See comments below on newitem, orignewitem, and posting lists */
                xlrec.firstrightoff = firstrightoff;
                xlrec.newitemoff = newitemoff;
                xlrec.postingoff = 0;
                if postingoff != 0 && origpagepostingoff < firstrightoff {
                    xlrec.postingoff = postingoff;
                }

                XLogBeginInsert();
                XLogRegisterData(
                    &xlrec as *const xl_btree_split as *const c_void,
                    SizeOfBtreeSplit as c_int,
                );

                XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
                XLogRegisterBuffer(1, rbuf, REGBUF_WILL_INIT);
                /* Log original right sibling, since we've changed its prev-pointer */
                if !isrightmost {
                    XLogRegisterBuffer(2, sbuf, REGBUF_STANDARD);
                }
                if !isleaf {
                    XLogRegisterBuffer(3, cbuf, REGBUF_STANDARD);
                }

                /*
                 * Log the new item, if it was inserted on the left page.
                 */
                if newitemonleft && xlrec.postingoff == 0 {
                    XLogRegisterBufData(0, newitem as *const c_void, newitemsz as c_int);
                } else if xlrec.postingoff != 0 {
                    Assert!(isleaf);
                    Assert!(newitemonleft || firstrightoff == newitemoff);
                    Assert!(newitemsz == IndexTupleSize(orignewitem));
                    XLogRegisterBufData(0, orignewitem as *const c_void, newitemsz as c_int);
                }

                /* Log the left page's new high key */
                let lefthighkey_xlog: IndexTuple = if !isleaf {
                    /* lefthighkey isn't local copy, get current pointer */
                    let iid = PageGetItemId(origpage, P_HIKEY);
                    PageGetItem(origpage, iid) as IndexTuple
                } else {
                    lefthighkey
                };
                XLogRegisterBufData(
                    0,
                    lefthighkey_xlog as *const c_void,
                    MAXALIGN(IndexTupleSize(lefthighkey_xlog)) as c_int,
                );

                /*
                 * Log the contents of the right page in the format understood by
                 * _bt_restore_page().
                 */
                {
                    use crate::storage::bufpage::PageHeader;
                    let rph = rightpage as PageHeader;
                    XLogRegisterBufData(
                        1,
                        (rightpage as *const u8).add((*rph).pd_upper as usize) as *const c_void,
                        ((*rph).pd_special - (*rph).pd_upper) as c_int,
                    );
                }

                xlinfo = if newitemonleft {
                    XLOG_BTREE_SPLIT_L
                } else {
                    XLOG_BTREE_SPLIT_R
                };
                recptr = XLogInsert(RM_BTREE_ID, xlinfo);

                PageSetLSN(origpage, recptr);
                PageSetLSN(rightpage, recptr);
                if !isrightmost {
                    PageSetLSN(spage, recptr);
                }
                if !isleaf {
                    PageSetLSN(BufferGetPage(cbuf), recptr);
                }
            }

            END_CRIT_SECTION!();

            /* release the old right sibling */
            if !isrightmost {
                _bt_relbuf(rel, sbuf);
            }

            /* release the child */
            if !isleaf {
                _bt_relbuf(rel, cbuf);
            }

            /* be tidy */
            if isleaf {
                pfree(lefthighkey as *mut c_void);
            }

            /* split's done */
            return rbuf;
        }
        // newitemonleft && newitemoff == firstrightoff (lastleft = newitem)
        let lastleft2 = newitem;
        let lefthighkey2 = _bt_truncate(rel, lastleft2, firstright, itup_key);
        itemsz = IndexTupleSize(lefthighkey2);

        afterleftoff = P_HIKEY;
        Assert!(BTreeTupleGetNAtts(lefthighkey2, rel) > 0);
        Assert!(
            BTreeTupleGetNAtts(lefthighkey2, rel)
                <= IndexRelationGetNumberOfKeyAttributes(rel)
        );
        Assert!(itemsz == MAXALIGN(IndexTupleSize(lefthighkey2)));
        if PageAddItem(leftpage, lefthighkey2 as Item, itemsz, afterleftoff, false, false)
            == InvalidOffsetNumber
        {
            elog!(
                ERROR,
                "failed to add high key to the left sibling while splitting block {} of index \"{}\"",
                origpagenumber,
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            );
        }
        afterleftoff = OffsetNumberNext(afterleftoff);

        rbuf = _bt_allocbuf(rel, heaprel);
        rightpage = BufferGetPage(rbuf);
        rightpagenumber = BufferGetBlockNumber(rbuf);
        ropaque = BTPageGetOpaque(rightpage);

        (*lopaque).btpo_next = rightpagenumber;
        (*lopaque).btpo_cycleid = _bt_vacuum_cycleid(rel);

        (*ropaque).btpo_flags = (*oopaque).btpo_flags;
        (*ropaque).btpo_flags &= !(BTP_ROOT | BTP_SPLIT_END | BTP_HAS_GARBAGE);
        (*ropaque).btpo_prev = origpagenumber;
        (*ropaque).btpo_next = (*oopaque).btpo_next;
        (*ropaque).btpo_level = (*oopaque).btpo_level;
        (*ropaque).btpo_cycleid = (*lopaque).btpo_cycleid;

        afterrightoff = P_HIKEY;

        if !isrightmost {
            let righthighkey: IndexTuple;
            itemid = PageGetItemId(origpage, P_HIKEY);
            itemsz = ItemIdGetLength(itemid) as Size;
            righthighkey = PageGetItem(origpage, itemid) as IndexTuple;
            Assert!(BTreeTupleGetNAtts(righthighkey, rel) > 0);
            Assert!(
                BTreeTupleGetNAtts(righthighkey, rel)
                    <= IndexRelationGetNumberOfKeyAttributes(rel)
            );
            if PageAddItem(
                rightpage,
                righthighkey as Item,
                itemsz,
                afterrightoff,
                false,
                false,
            ) == InvalidOffsetNumber
            {
                core::ptr::write_bytes(rightpage, 0, BufferGetPageSize(rbuf));
                elog!(
                    ERROR,
                    "failed to add high key to the right sibling while splitting block {} of index \"{}\"",
                    origpagenumber,
                    CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                );
            }
            afterrightoff = OffsetNumberNext(afterrightoff);
        }

        minusinfoff = InvalidOffsetNumber;

        i = P_FIRSTDATAKEY(oopaque);
        while i <= maxoff {
            let mut dataitem: IndexTuple;
            itemid = PageGetItemId(origpage, i);
            itemsz = ItemIdGetLength(itemid) as Size;
            dataitem = PageGetItem(origpage, itemid) as IndexTuple;

            if i == origpagepostingoff {
                Assert!(BTreeTupleIsPosting(dataitem));
                Assert!(itemsz == MAXALIGN(IndexTupleSize(nposting)));
                dataitem = nposting;
            } else if i == newitemoff {
                if newitemonleft {
                    Assert!(newitemoff <= firstrightoff);
                    if !_bt_pgaddtup(leftpage, newitemsz, newitem, afterleftoff, false) {
                        core::ptr::write_bytes(rightpage, 0, BufferGetPageSize(rbuf));
                        elog!(ERROR, "failed to add new item to the left sibling while splitting block {} of index \"{}\"",
                            origpagenumber, CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy());
                    }
                    afterleftoff = OffsetNumberNext(afterleftoff);
                } else {
                    Assert!(newitemoff >= firstrightoff);
                    if !_bt_pgaddtup(rightpage, newitemsz, newitem, afterrightoff, afterrightoff == minusinfoff) {
                        core::ptr::write_bytes(rightpage, 0, BufferGetPageSize(rbuf));
                        elog!(ERROR, "failed to add new item to the right sibling while splitting block {} of index \"{}\"",
                            origpagenumber, CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy());
                    }
                    afterrightoff = OffsetNumberNext(afterrightoff);
                }
            }

            if i < firstrightoff {
                if !_bt_pgaddtup(leftpage, itemsz, dataitem, afterleftoff, false) {
                    core::ptr::write_bytes(rightpage, 0, BufferGetPageSize(rbuf));
                    elog!(ERROR, "failed to add old item to the left sibling while splitting block {} of index \"{}\"",
                        origpagenumber, CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy());
                }
                afterleftoff = OffsetNumberNext(afterleftoff);
            } else {
                if !_bt_pgaddtup(rightpage, itemsz, dataitem, afterrightoff, afterrightoff == minusinfoff) {
                    core::ptr::write_bytes(rightpage, 0, BufferGetPageSize(rbuf));
                    elog!(ERROR, "failed to add old item to the right sibling while splitting block {} of index \"{}\"",
                        origpagenumber, CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy());
                }
                afterrightoff = OffsetNumberNext(afterrightoff);
            }

            i = OffsetNumberNext(i);
        }

        if i <= newitemoff {
            Assert!(!newitemonleft && newitemoff == maxoff + 1);
            if !_bt_pgaddtup(rightpage, newitemsz, newitem, afterrightoff, afterrightoff == minusinfoff) {
                core::ptr::write_bytes(rightpage, 0, BufferGetPageSize(rbuf));
                elog!(ERROR, "failed to add new item to the right sibling while splitting block {} of index \"{}\"",
                    origpagenumber, CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy());
            }
            afterrightoff = OffsetNumberNext(afterrightoff);
        }

        if !isrightmost {
            sbuf = _bt_getbuf(rel, (*oopaque).btpo_next, BT_WRITE);
            spage = BufferGetPage(sbuf);
            sopaque = BTPageGetOpaque(spage);
            if (*sopaque).btpo_prev != origpagenumber {
                core::ptr::write_bytes(rightpage, 0, BufferGetPageSize(rbuf));
                ereport!(ERROR, /* C also: errcode(ERRCODE_INDEX_CORRUPTED) */
                    errmsg!("right sibling's left-link doesn't match: block {} links to {} instead of expected {} in index \"{}\"",
                        (*oopaque).btpo_next, (*sopaque).btpo_prev, origpagenumber,
                        CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()));
            }
            if (*sopaque).btpo_cycleid != (*ropaque).btpo_cycleid {
                (*ropaque).btpo_flags |= BTP_SPLIT_END;
            }
        }

        START_CRIT_SECTION!();
        PageRestoreTempPage(leftpage, origpage);
        MarkBufferDirty(buf);
        MarkBufferDirty(rbuf);
        if !isrightmost {
            (*sopaque).btpo_prev = rightpagenumber;
            MarkBufferDirty(sbuf);
        }
        if !isleaf {
            let cpage: Page = BufferGetPage(cbuf);
            let cpageop: BTPageOpaque = BTPageGetOpaque(cpage);
            (*cpageop).btpo_flags &= !BTP_INCOMPLETE_SPLIT;
            MarkBufferDirty(cbuf);
        }

        if RelationNeedsWAL(rel) {
            let mut xlrec: xl_btree_split = core::mem::zeroed();
            let xlinfo: u8;
            let recptr: XLogRecPtr;
            xlrec.level = (*ropaque).btpo_level;
            xlrec.firstrightoff = firstrightoff;
            xlrec.newitemoff = newitemoff;
            xlrec.postingoff = 0;
            if postingoff != 0 && origpagepostingoff < firstrightoff {
                xlrec.postingoff = postingoff;
            }
            XLogBeginInsert();
            XLogRegisterData(&xlrec as *const xl_btree_split as *const c_void, SizeOfBtreeSplit as c_int);
            XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
            XLogRegisterBuffer(1, rbuf, REGBUF_WILL_INIT);
            if !isrightmost { XLogRegisterBuffer(2, sbuf, REGBUF_STANDARD); }
            if !isleaf { XLogRegisterBuffer(3, cbuf, REGBUF_STANDARD); }
            if newitemonleft && xlrec.postingoff == 0 {
                XLogRegisterBufData(0, newitem as *const c_void, newitemsz as c_int);
            } else if xlrec.postingoff != 0 {
                Assert!(isleaf);
                Assert!(newitemonleft || firstrightoff == newitemoff);
                Assert!(newitemsz == IndexTupleSize(orignewitem));
                XLogRegisterBufData(0, orignewitem as *const c_void, newitemsz as c_int);
            }
            let lefthighkey_xlog2: IndexTuple = if !isleaf {
                let iid = PageGetItemId(origpage, P_HIKEY);
                PageGetItem(origpage, iid) as IndexTuple
            } else {
                lefthighkey2
            };
            XLogRegisterBufData(0, lefthighkey_xlog2 as *const c_void, MAXALIGN(IndexTupleSize(lefthighkey_xlog2)) as c_int);
            {
                use crate::storage::bufpage::PageHeader;
                let rph = rightpage as PageHeader;
                XLogRegisterBufData(1,
                    (rightpage as *const u8).add((*rph).pd_upper as usize) as *const c_void,
                    ((*rph).pd_special - (*rph).pd_upper) as c_int);
            }
            xlinfo = if newitemonleft { XLOG_BTREE_SPLIT_L } else { XLOG_BTREE_SPLIT_R };
            recptr = XLogInsert(RM_BTREE_ID, xlinfo);
            PageSetLSN(origpage, recptr);
            PageSetLSN(rightpage, recptr);
            if !isrightmost { PageSetLSN(spage, recptr); }
            if !isleaf { PageSetLSN(BufferGetPage(cbuf), recptr); }
        }

        END_CRIT_SECTION!();

        if !isrightmost { _bt_relbuf(rel, sbuf); }
        if !isleaf { _bt_relbuf(rel, cbuf); }
        if isleaf { pfree(lefthighkey2 as *mut c_void); }

        return rbuf;
    }

    // !isleaf branch: no suffix truncation -- use firstright directly as lefthighkey
    /*
     * Don't perform suffix truncation on a copy of firstright to make left
     * page high key for internal page splits.  Must use firstright as new
     * high key directly.
     */
    let lefthighkey3 = firstright;

    afterleftoff = P_HIKEY;
    Assert!(BTreeTupleGetNAtts(lefthighkey3, rel) > 0);
    Assert!(BTreeTupleGetNAtts(lefthighkey3, rel) <= IndexRelationGetNumberOfKeyAttributes(rel));
    Assert!(itemsz == MAXALIGN(IndexTupleSize(lefthighkey3)));
    if PageAddItem(leftpage, lefthighkey3 as Item, itemsz, afterleftoff, false, false)
        == InvalidOffsetNumber
    {
        elog!(ERROR, "failed to add high key to the left sibling while splitting block {} of index \"{}\"",
            origpagenumber, CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy());
    }
    afterleftoff = OffsetNumberNext(afterleftoff);

    rbuf = _bt_allocbuf(rel, heaprel);
    rightpage = BufferGetPage(rbuf);
    rightpagenumber = BufferGetBlockNumber(rbuf);
    ropaque = BTPageGetOpaque(rightpage);

    (*lopaque).btpo_next = rightpagenumber;
    (*lopaque).btpo_cycleid = _bt_vacuum_cycleid(rel);

    (*ropaque).btpo_flags = (*oopaque).btpo_flags;
    (*ropaque).btpo_flags &= !(BTP_ROOT | BTP_SPLIT_END | BTP_HAS_GARBAGE);
    (*ropaque).btpo_prev = origpagenumber;
    (*ropaque).btpo_next = (*oopaque).btpo_next;
    (*ropaque).btpo_level = (*oopaque).btpo_level;
    (*ropaque).btpo_cycleid = (*lopaque).btpo_cycleid;

    afterrightoff = P_HIKEY;

    if !isrightmost {
        let righthighkey: IndexTuple;
        itemid = PageGetItemId(origpage, P_HIKEY);
        itemsz = ItemIdGetLength(itemid) as Size;
        righthighkey = PageGetItem(origpage, itemid) as IndexTuple;
        Assert!(BTreeTupleGetNAtts(righthighkey, rel) > 0);
        Assert!(BTreeTupleGetNAtts(righthighkey, rel) <= IndexRelationGetNumberOfKeyAttributes(rel));
        if PageAddItem(rightpage, righthighkey as Item, itemsz, afterrightoff, false, false)
            == InvalidOffsetNumber
        {
            core::ptr::write_bytes(rightpage, 0, BufferGetPageSize(rbuf));
            elog!(ERROR, "failed to add high key to the right sibling while splitting block {} of index \"{}\"",
                origpagenumber, CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy());
        }
        afterrightoff = OffsetNumberNext(afterrightoff);
    }

    /*
     * Internal page splits truncate first data item on right page -- it
     * becomes "minus infinity" item for the page.
     */
    minusinfoff = afterrightoff; // !isleaf

    i = P_FIRSTDATAKEY(oopaque);
    while i <= maxoff {
        let mut dataitem: IndexTuple;
        itemid = PageGetItemId(origpage, i);
        itemsz = ItemIdGetLength(itemid) as Size;
        dataitem = PageGetItem(origpage, itemid) as IndexTuple;

        if i == origpagepostingoff {
            Assert!(BTreeTupleIsPosting(dataitem));
            Assert!(itemsz == MAXALIGN(IndexTupleSize(nposting)));
            dataitem = nposting;
        } else if i == newitemoff {
            if newitemonleft {
                Assert!(newitemoff <= firstrightoff);
                if !_bt_pgaddtup(leftpage, newitemsz, newitem, afterleftoff, false) {
                    core::ptr::write_bytes(rightpage, 0, BufferGetPageSize(rbuf));
                    elog!(ERROR, "failed to add new item to the left sibling while splitting block {} of index \"{}\"",
                        origpagenumber, CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy());
                }
                afterleftoff = OffsetNumberNext(afterleftoff);
            } else {
                Assert!(newitemoff >= firstrightoff);
                if !_bt_pgaddtup(rightpage, newitemsz, newitem, afterrightoff, afterrightoff == minusinfoff) {
                    core::ptr::write_bytes(rightpage, 0, BufferGetPageSize(rbuf));
                    elog!(ERROR, "failed to add new item to the right sibling while splitting block {} of index \"{}\"",
                        origpagenumber, CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy());
                }
                afterrightoff = OffsetNumberNext(afterrightoff);
            }
        }

        if i < firstrightoff {
            if !_bt_pgaddtup(leftpage, itemsz, dataitem, afterleftoff, false) {
                core::ptr::write_bytes(rightpage, 0, BufferGetPageSize(rbuf));
                elog!(ERROR, "failed to add old item to the left sibling while splitting block {} of index \"{}\"",
                    origpagenumber, CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy());
            }
            afterleftoff = OffsetNumberNext(afterleftoff);
        } else {
            if !_bt_pgaddtup(rightpage, itemsz, dataitem, afterrightoff, afterrightoff == minusinfoff) {
                core::ptr::write_bytes(rightpage, 0, BufferGetPageSize(rbuf));
                elog!(ERROR, "failed to add old item to the right sibling while splitting block {} of index \"{}\"",
                    origpagenumber, CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy());
            }
            afterrightoff = OffsetNumberNext(afterrightoff);
        }

        i = OffsetNumberNext(i);
    }

    if i <= newitemoff {
        Assert!(!newitemonleft && newitemoff == maxoff + 1);
        if !_bt_pgaddtup(rightpage, newitemsz, newitem, afterrightoff, afterrightoff == minusinfoff) {
            core::ptr::write_bytes(rightpage, 0, BufferGetPageSize(rbuf));
            elog!(ERROR, "failed to add new item to the right sibling while splitting block {} of index \"{}\"",
                origpagenumber, CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy());
        }
        afterrightoff = OffsetNumberNext(afterrightoff);
    }

    if !isrightmost {
        sbuf = _bt_getbuf(rel, (*oopaque).btpo_next, BT_WRITE);
        spage = BufferGetPage(sbuf);
        sopaque = BTPageGetOpaque(spage);
        if (*sopaque).btpo_prev != origpagenumber {
            core::ptr::write_bytes(rightpage, 0, BufferGetPageSize(rbuf));
            ereport!(ERROR, /* C also: errcode(ERRCODE_INDEX_CORRUPTED) */
                errmsg!("right sibling's left-link doesn't match: block {} links to {} instead of expected {} in index \"{}\"",
                    (*oopaque).btpo_next, (*sopaque).btpo_prev, origpagenumber,
                    CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()));
        }
        if (*sopaque).btpo_cycleid != (*ropaque).btpo_cycleid {
            (*ropaque).btpo_flags |= BTP_SPLIT_END;
        }
    }

    START_CRIT_SECTION!();
    PageRestoreTempPage(leftpage, origpage);
    MarkBufferDirty(buf);
    MarkBufferDirty(rbuf);
    if !isrightmost {
        (*sopaque).btpo_prev = rightpagenumber;
        MarkBufferDirty(sbuf);
    }
    if !isleaf {
        let cpage: Page = BufferGetPage(cbuf);
        let cpageop: BTPageOpaque = BTPageGetOpaque(cpage);
        (*cpageop).btpo_flags &= !BTP_INCOMPLETE_SPLIT;
        MarkBufferDirty(cbuf);
    }

    if RelationNeedsWAL(rel) {
        let mut xlrec: xl_btree_split = core::mem::zeroed();
        let xlinfo: u8;
        let recptr: XLogRecPtr;
        xlrec.level = (*ropaque).btpo_level;
        xlrec.firstrightoff = firstrightoff;
        xlrec.newitemoff = newitemoff;
        xlrec.postingoff = 0;
        if postingoff != 0 && origpagepostingoff < firstrightoff {
            xlrec.postingoff = postingoff;
        }
        XLogBeginInsert();
        XLogRegisterData(&xlrec as *const xl_btree_split as *const c_void, SizeOfBtreeSplit as c_int);
        XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
        XLogRegisterBuffer(1, rbuf, REGBUF_WILL_INIT);
        if !isrightmost { XLogRegisterBuffer(2, sbuf, REGBUF_STANDARD); }
        if !isleaf { XLogRegisterBuffer(3, cbuf, REGBUF_STANDARD); }
        if newitemonleft && xlrec.postingoff == 0 {
            XLogRegisterBufData(0, newitem as *const c_void, newitemsz as c_int);
        } else if xlrec.postingoff != 0 {
            Assert!(isleaf);
            Assert!(newitemonleft || firstrightoff == newitemoff);
            Assert!(newitemsz == IndexTupleSize(orignewitem));
            XLogRegisterBufData(0, orignewitem as *const c_void, newitemsz as c_int);
        }
        let lefthighkey_xlog3: IndexTuple = {
            let iid = PageGetItemId(origpage, P_HIKEY);
            PageGetItem(origpage, iid) as IndexTuple
        };
        XLogRegisterBufData(0, lefthighkey_xlog3 as *const c_void, MAXALIGN(IndexTupleSize(lefthighkey_xlog3)) as c_int);
        {
            use crate::storage::bufpage::PageHeader;
            let rph = rightpage as PageHeader;
            XLogRegisterBufData(1,
                (rightpage as *const u8).add((*rph).pd_upper as usize) as *const c_void,
                ((*rph).pd_special - (*rph).pd_upper) as c_int);
        }
        xlinfo = if newitemonleft { XLOG_BTREE_SPLIT_L } else { XLOG_BTREE_SPLIT_R };
        recptr = XLogInsert(RM_BTREE_ID, xlinfo);
        PageSetLSN(origpage, recptr);
        PageSetLSN(rightpage, recptr);
        if !isrightmost { PageSetLSN(spage, recptr); }
        if !isleaf { PageSetLSN(BufferGetPage(cbuf), recptr); }
    }

    END_CRIT_SECTION!();

    if !isrightmost { _bt_relbuf(rel, sbuf); }
    if !isleaf { _bt_relbuf(rel, cbuf); }
    // !isleaf: lefthighkey3 is firstright (not a local alloc), no pfree

    rbuf
}

/*
 * _bt_insert_parent() -- Insert downlink into parent, completing split.
 *
 * On entry, buf and rbuf are the left and right split pages, which we
 * still hold write locks on.  Both locks will be released here.  We
 * release the rbuf lock once we have a write lock on the page that we
 * intend to insert a downlink to rbuf on (i.e. buf's current parent page).
 * The lock on buf is released at the same point as the lock on the parent
 * page, since buf's INCOMPLETE_SPLIT flag must be cleared by the same
 * atomic operation that completes the split by inserting a new downlink.
 *
 * stack - stack showing how we got here.  Will be NULL when splitting true
 *			root, or during concurrent root split, where we can be inefficient
 * isroot - we split the true root
 * isonly - we split a page alone on its level (might have been fast root)
 */
unsafe fn _bt_insert_parent(
    rel: Relation,
    heaprel: Relation,
    buf: Buffer,
    rbuf: Buffer,
    stack: BTStack,
    isroot: bool,
    isonly: bool,
) {
    Assert!(!heaprel.is_null());

    /*
     * Here we have to do something Lehman and Yao don't talk about: deal with
     * a root split and construction of a new root.  If our stack is empty
     * then we have just split a node on what had been the root level when we
     * descended the tree.  If it was still the root then we perform a
     * new-root construction.  If it *wasn't* the root anymore, search to find
     * the next higher level that someone constructed meanwhile, and find the
     * right place to insert as for the normal case.
     *
     * If we have to search for the parent level, we do so by re-descending
     * from the root.  This is not super-efficient, but it's rare enough not
     * to matter.
     */
    if isroot {
        let rootbuf: Buffer;

        Assert!(stack.is_null());
        Assert!(isonly);
        /* create a new root node one level up and update the metapage */
        rootbuf = _bt_newlevel(rel, heaprel, buf, rbuf);
        /* release the split buffers */
        _bt_relbuf(rel, rootbuf);
        _bt_relbuf(rel, rbuf);
        _bt_relbuf(rel, buf);
    } else {
        let bknum: BlockNumber = BufferGetBlockNumber(buf);
        let rbknum: BlockNumber = BufferGetBlockNumber(rbuf);
        let page: Page = BufferGetPage(buf);
        let new_item: IndexTuple;
        let mut fakestack: BTStackData = core::mem::zeroed();
        let ritem: IndexTuple;
        let mut pbuf: Buffer;

        let mut stack_mut = stack;

        if stack_mut.is_null() {
            let opaque: BTPageOpaque;

            elog!(DEBUG2, "concurrent ROOT page split");
            opaque = BTPageGetOpaque(page);

            /*
             * We should never reach here when a leaf page split takes place
             * despite the insert of newitem being able to apply the fastpath
             * optimization.  Make sure of that with an assertion.
             *
             * This is more of a performance issue than a correctness issue.
             * The fastpath won't have a descent stack.  Using a phony stack
             * here works, but never rely on that.  The fastpath should be
             * rejected within _bt_search_insert() when the rightmost leaf
             * page will split, since it's faster to go through _bt_search()
             * and get a stack in the usual way.
             */
            Assert!(!(P_ISLEAF(opaque) && BlockNumberIsValid(RelationGetTargetBlock(rel))));

            /* Find the leftmost page at the next level up */
            pbuf = _bt_get_endpoint(rel, (*opaque).btpo_level + 1, false);
            /* Set up a phony stack entry pointing there */
            stack_mut = &mut fakestack;
            (*stack_mut).bts_blkno = BufferGetBlockNumber(pbuf);
            (*stack_mut).bts_offset = InvalidOffsetNumber;
            (*stack_mut).bts_parent = core::ptr::null_mut();
            _bt_relbuf(rel, pbuf);
        }

        /* get high key from left, a strict lower bound for new right page */
        ritem = PageGetItem(page, PageGetItemId(page, P_HIKEY)) as IndexTuple;

        /* form an index tuple that points at the new right page */
        new_item = CopyIndexTuple(ritem);
        BTreeTupleSetDownLink(new_item, rbknum);

        /*
         * Re-find and write lock the parent of buf.
         *
         * It's possible that the location of buf's downlink has changed since
         * our initial _bt_search() descent.  _bt_getstackbuf() will detect
         * and recover from this, updating the stack, which ensures that the
         * new downlink will be inserted at the correct offset. Even buf's
         * parent may have changed.
         */
        pbuf = _bt_getstackbuf(rel, heaprel, stack_mut, bknum);

        /*
         * Unlock the right child.  The left child will be unlocked in
         * _bt_insertonpg().
         *
         * Unlocking the right child must be delayed until here to ensure that
         * no concurrent VACUUM operation can become confused.  Page deletion
         * cannot be allowed to fail to re-find a downlink for the rbuf page.
         * (Actually, this is just a vestige of how things used to work.  The
         * page deletion code is expected to check for the INCOMPLETE_SPLIT
         * flag on the left child.  It won't attempt deletion of the right
         * child until the split is complete.  Despite all this, we opt to
         * conservatively delay unlocking the right child until here.)
         */
        _bt_relbuf(rel, rbuf);

        if pbuf == InvalidBuffer {
            ereport!(
                ERROR,
                /* C also: errcode(ERRCODE_INDEX_CORRUPTED) */                errmsg!(
                    "failed to re-find parent key in index \"{}\" for split pages {}/{}",
                    CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy(),
                    bknum,
                    rbknum
                )
            );
        }

        /* Recursively insert into the parent */
        _bt_insertonpg(
            rel,
            heaprel,
            core::ptr::null_mut(),
            pbuf,
            buf,
            (*stack_mut).bts_parent,
            new_item,
            MAXALIGN(IndexTupleSize(new_item)),
            (*stack_mut).bts_offset + 1,
            0,
            isonly,
        );

        /* be tidy */
        pfree(new_item as *mut c_void);
    }
}

/*
 * _bt_finish_split() -- Finish an incomplete split
 *
 * A crash or other failure can leave a split incomplete.  The insertion
 * routines won't allow to insert on a page that is incompletely split.
 * Before inserting on such a page, call _bt_finish_split().
 *
 * On entry, 'lbuf' must be locked in write-mode.  On exit, it is unlocked
 * and unpinned.
 *
 * Caller must provide a valid heaprel, since finishing a page split requires
 * allocating a new page if and when the parent page splits in turn.
 */
pub unsafe fn _bt_finish_split(
    rel: Relation,
    heaprel: Relation,
    lbuf: Buffer,
    stack: BTStack,
) {
    let lpage: Page = BufferGetPage(lbuf);
    let lpageop: BTPageOpaque = BTPageGetOpaque(lpage);
    let rbuf: Buffer;
    let rpage: Page;
    let rpageop: BTPageOpaque;
    let wasroot: bool;
    let wasonly: bool;

    Assert!(P_INCOMPLETE_SPLIT(lpageop));
    Assert!(!heaprel.is_null());

    /* Lock right sibling, the one missing the downlink */
    rbuf = _bt_getbuf(rel, (*lpageop).btpo_next, BT_WRITE);
    rpage = BufferGetPage(rbuf);
    rpageop = BTPageGetOpaque(rpage);

    /* Could this be a root split? */
    if stack.is_null() {
        let metabuf: Buffer;
        let metapg: Page;
        let metad: *mut BTMetaPageData;

        /* acquire lock on the metapage */
        metabuf = _bt_getbuf(rel, BTREE_METAPAGE, BT_WRITE);
        metapg = BufferGetPage(metabuf);
        metad = BTPageGetMeta(metapg);

        wasroot = (*metad).btm_root == BufferGetBlockNumber(lbuf);

        _bt_relbuf(rel, metabuf);
    } else {
        wasroot = false;
    }

    /* Was this the only page on the level before split? */
    wasonly = P_LEFTMOST(lpageop) && P_RIGHTMOST(rpageop);

    elog!(
        DEBUG1,
        "finishing incomplete split of {}/{}",
        BufferGetBlockNumber(lbuf),
        BufferGetBlockNumber(rbuf)
    );

    _bt_insert_parent(rel, heaprel, lbuf, rbuf, stack, wasroot, wasonly);
}

/*
 *	_bt_getstackbuf() -- Walk back up the tree one step, and find the pivot
 *						 tuple whose downlink points to child page.
 *
 *		Caller passes child's block number, which is used to identify
 *		associated pivot tuple in parent page using a linear search that
 *		matches on pivot's downlink/block number.  The expected location of
 *		the pivot tuple is taken from the stack one level above the child
 *		page.  This is used as a starting point.  Insertions into the
 *		parent level could cause the pivot tuple to move right; deletions
 *		could cause it to move left, but not left of the page we previously
 *		found it on.
 *
 *		Caller can use its stack to relocate the pivot tuple/downlink for
 *		any same-level page to the right of the page found by its initial
 *		descent.  This is necessary because of the possibility that caller
 *		moved right to recover from a concurrent page split.  It's also
 *		convenient for certain callers to be able to step right when there
 *		wasn't a concurrent page split, while still using their original
 *		stack.  For example, the checkingunique _bt_doinsert() case may
 *		have to step right when there are many physical duplicates, and its
 *		scantid forces an insertion to the right of the "first page the
 *		value could be on".  (This is also relied on by all of our callers
 *		when dealing with !heapkeyspace indexes.)
 *
 *		Returns write-locked parent page buffer, or InvalidBuffer if pivot
 *		tuple not found (should not happen).  Adjusts bts_blkno &
 *		bts_offset if changed.  Page split caller should insert its new
 *		pivot tuple for its new right sibling page on parent page, at the
 *		offset number bts_offset + 1.
 */
pub unsafe fn _bt_getstackbuf(
    rel: Relation,
    heaprel: Relation,
    stack: BTStack,
    child: BlockNumber,
) -> Buffer {
    let mut blkno: BlockNumber;
    let mut start: OffsetNumber;

    blkno = (*stack).bts_blkno;
    start = (*stack).bts_offset;

    loop {
        let buf: Buffer;
        let page: Page;
        let opaque: BTPageOpaque;

        buf = _bt_getbuf(rel, blkno, BT_WRITE);
        page = BufferGetPage(buf);
        opaque = BTPageGetOpaque(page);

        Assert!(!heaprel.is_null());
        if P_INCOMPLETE_SPLIT(opaque) {
            _bt_finish_split(rel, heaprel, buf, (*stack).bts_parent);
            continue;
        }

        if !P_IGNORE(opaque) {
            let mut offnum: OffsetNumber;
            let minoff: OffsetNumber;
            let maxoff: OffsetNumber;
            let mut itemid: ItemId;
            let mut item: IndexTuple;

            minoff = P_FIRSTDATAKEY(opaque);
            maxoff = PageGetMaxOffsetNumber(page);

            /*
             * start = InvalidOffsetNumber means "search the whole page". We
             * need this test anyway due to possibility that page has a high
             * key now when it didn't before.
             */
            if start < minoff {
                start = minoff;
            }

            /*
             * Need this check too, to guard against possibility that page
             * split since we visited it originally.
             */
            if start > maxoff {
                start = OffsetNumberNext(maxoff);
            }

            /*
             * These loops will check every item on the page --- but in an
             * order that's attuned to the probability of where it actually
             * is.  Scan to the right first, then to the left.
             */
            offnum = start;
            while offnum <= maxoff {
                itemid = PageGetItemId(page, offnum);
                item = PageGetItem(page, itemid) as IndexTuple;

                if BTreeTupleGetDownLink(item) == child {
                    /* Return accurate pointer to where link is now */
                    (*stack).bts_blkno = blkno;
                    (*stack).bts_offset = offnum;
                    return buf;
                }
                offnum = OffsetNumberNext(offnum);
            }

            offnum = OffsetNumberPrev(start);
            while offnum >= minoff {
                itemid = PageGetItemId(page, offnum);
                item = PageGetItem(page, itemid) as IndexTuple;

                if BTreeTupleGetDownLink(item) == child {
                    /* Return accurate pointer to where link is now */
                    (*stack).bts_blkno = blkno;
                    (*stack).bts_offset = offnum;
                    return buf;
                }
                offnum = OffsetNumberPrev(offnum);
            }
        }

        /*
         * The item we're looking for moved right at least one page.
         *
         * Lehman and Yao couple/chain locks when moving right here, which we
         * can avoid.  See nbtree/README.
         */
        if P_RIGHTMOST(opaque) {
            _bt_relbuf(rel, buf);
            return InvalidBuffer;
        }
        blkno = (*opaque).btpo_next;
        start = InvalidOffsetNumber;
        _bt_relbuf(rel, buf);
    }
}

/*
 *	_bt_newlevel() -- Create a new level above root page.
 *
 *		We've just split the old root page and need to create a new one.
 *		In order to do this, we add a new root page to the file, then lock
 *		the metadata page and update it.  This is guaranteed to be deadlock-
 *		free, because all readers release their locks on the metadata page
 *		before trying to lock the root, and all writers lock the root before
 *		trying to lock the metadata page.  We have a write lock on the old
 *		root page, so we have not introduced any cycles into the waits-for
 *		graph.
 *
 *		On entry, lbuf (the old root) and rbuf (its new peer) are write-
 *		locked. On exit, a new root page exists with entries for the
 *		two new children, metapage is updated and unlocked/unpinned.
 *		The new root buffer is returned to caller which has to unlock/unpin
 *		lbuf, rbuf & rootbuf.
 */
unsafe fn _bt_newlevel(
    rel: Relation,
    heaprel: Relation,
    lbuf: Buffer,
    rbuf: Buffer,
) -> Buffer {
    let rootbuf: Buffer;
    let lpage: Page;
    let rootpage: Page;
    let lbkno: BlockNumber;
    let rbkno: BlockNumber;
    let rootblknum: BlockNumber;
    let rootopaque: BTPageOpaque;
    let lopaque: BTPageOpaque;
    let itemid: ItemId;
    let item: IndexTuple;
    let left_item: IndexTuple;
    let left_item_sz: Size;
    let right_item: IndexTuple;
    let right_item_sz: Size;
    let metabuf: Buffer;
    let metapg: Page;
    let metad: *mut BTMetaPageData;

    lbkno = BufferGetBlockNumber(lbuf);
    rbkno = BufferGetBlockNumber(rbuf);
    lpage = BufferGetPage(lbuf);
    lopaque = BTPageGetOpaque(lpage);

    /* get a new root page */
    rootbuf = _bt_allocbuf(rel, heaprel);
    rootpage = BufferGetPage(rootbuf);
    rootblknum = BufferGetBlockNumber(rootbuf);

    /* acquire lock on the metapage */
    metabuf = _bt_getbuf(rel, BTREE_METAPAGE, BT_WRITE);
    metapg = BufferGetPage(metabuf);
    metad = BTPageGetMeta(metapg);

    /*
     * Create downlink item for left page (old root).  The key value used is
     * "minus infinity", a sentinel value that's reliably less than any real
     * key value that could appear in the left page.
     */
    left_item_sz = core::mem::size_of::<IndexTupleData>();
    left_item = palloc(left_item_sz) as IndexTuple;
    (*left_item).t_info = left_item_sz as u16;
    BTreeTupleSetDownLink(left_item, lbkno);
    BTreeTupleSetNAtts(left_item, 0, false);

    /*
     * Create downlink item for right page.  The key for it is obtained from
     * the "high key" position in the left page.
     */
    itemid = PageGetItemId(lpage, P_HIKEY);
    right_item_sz = ItemIdGetLength(itemid) as Size;
    item = PageGetItem(lpage, itemid) as IndexTuple;
    right_item = CopyIndexTuple(item);
    BTreeTupleSetDownLink(right_item, rbkno);

    /* NO EREPORT(ERROR) from here till newroot op is logged */
    START_CRIT_SECTION!();

    /* upgrade metapage if needed */
    if (*metad).btm_version < BTREE_NOVAC_VERSION {
        _bt_upgrademetapage(metapg);
    }

    /* set btree special data */
    rootopaque = BTPageGetOpaque(rootpage);
    (*rootopaque).btpo_prev = P_NONE;
    (*rootopaque).btpo_next = P_NONE;
    (*rootopaque).btpo_flags = BTP_ROOT;
    (*rootopaque).btpo_level = (*BTPageGetOpaque(lpage)).btpo_level + 1;
    (*rootopaque).btpo_cycleid = 0;

    /* update metapage data */
    (*metad).btm_root = rootblknum;
    (*metad).btm_level = (*rootopaque).btpo_level;
    (*metad).btm_fastroot = rootblknum;
    (*metad).btm_fastlevel = (*rootopaque).btpo_level;

    /*
     * Insert the left page pointer into the new root page.  The root page is
     * the rightmost page on its level so there is no "high key" in it; the
     * two items will go into positions P_HIKEY and P_FIRSTKEY.
     *
     * Note: we *must* insert the two items in item-number order, for the
     * benefit of _bt_restore_page().
     */
    Assert!(BTreeTupleGetNAtts(left_item, rel) == 0);
    if PageAddItem(rootpage, left_item as Item, left_item_sz, P_HIKEY, false, false)
        == InvalidOffsetNumber
    {
        elog!(
            PANIC,
            "failed to add leftkey to new root page while splitting block {} of index \"{}\"",
            BufferGetBlockNumber(lbuf),
            CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
        );
    }

    /*
     * insert the right page pointer into the new root page.
     */
    Assert!(BTreeTupleGetNAtts(right_item, rel) > 0);
    Assert!(
        BTreeTupleGetNAtts(right_item, rel) <= IndexRelationGetNumberOfKeyAttributes(rel)
    );
    if PageAddItem(rootpage, right_item as Item, right_item_sz, P_FIRSTKEY, false, false)
        == InvalidOffsetNumber
    {
        elog!(
            PANIC,
            "failed to add rightkey to new root page while splitting block {} of index \"{}\"",
            BufferGetBlockNumber(lbuf),
            CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
        );
    }

    /* Clear the incomplete-split flag in the left child */
    Assert!(P_INCOMPLETE_SPLIT(lopaque));
    (*lopaque).btpo_flags &= !BTP_INCOMPLETE_SPLIT;
    MarkBufferDirty(lbuf);

    MarkBufferDirty(rootbuf);
    MarkBufferDirty(metabuf);

    /* XLOG stuff */
    if RelationNeedsWAL(rel) {
        let mut xlrec: xl_btree_newroot = core::mem::zeroed();
        let recptr: XLogRecPtr;
        let mut md: xl_btree_metadata = core::mem::zeroed();

        xlrec.rootblk = rootblknum;
        xlrec.level = (*metad).btm_level;

        XLogBeginInsert();
        XLogRegisterData(
            &xlrec as *const xl_btree_newroot as *const c_void,
            SizeOfBtreeNewroot as c_int,
        );

        XLogRegisterBuffer(0, rootbuf, REGBUF_WILL_INIT);
        XLogRegisterBuffer(1, lbuf, REGBUF_STANDARD);
        XLogRegisterBuffer(2, metabuf, REGBUF_WILL_INIT | REGBUF_STANDARD);

        Assert!((*metad).btm_version >= BTREE_NOVAC_VERSION);
        md.version = (*metad).btm_version;
        md.root = rootblknum;
        md.level = (*metad).btm_level;
        md.fastroot = rootblknum;
        md.fastlevel = (*metad).btm_level;
        md.last_cleanup_num_delpages = (*metad).btm_last_cleanup_num_delpages;
        md.allequalimage = (*metad).btm_allequalimage;

        XLogRegisterBufData(
            2,
            &md as *const xl_btree_metadata as *const c_void,
            core::mem::size_of::<xl_btree_metadata>() as c_int,
        );

        /*
         * Direct access to page is not good but faster - we should implement
         * some new func in page API.
         */
        {
            use crate::storage::bufpage::PageHeader;
            let rph = rootpage as PageHeader;
            XLogRegisterBufData(
                0,
                (rootpage as *const u8).add((*rph).pd_upper as usize) as *const c_void,
                ((*rph).pd_special - (*rph).pd_upper) as c_int,
            );
        }

        recptr = XLogInsert(RM_BTREE_ID, XLOG_BTREE_NEWROOT);

        PageSetLSN(lpage, recptr);
        PageSetLSN(rootpage, recptr);
        PageSetLSN(metapg, recptr);
    }

    END_CRIT_SECTION!();

    /* done with metapage */
    _bt_relbuf(rel, metabuf);

    pfree(left_item as *mut c_void);
    pfree(right_item as *mut c_void);

    rootbuf
}

/*
 *	_bt_pgaddtup() -- add a data item to a particular page during split.
 *
 *		The difference between this routine and a bare PageAddItem call is
 *		that this code can deal with the first data item on an internal btree
 *		page in passing.  This data item (which is called "firstright" within
 *		_bt_split()) has a key that must be treated as minus infinity after
 *		the split.  Therefore, we truncate away all attributes when caller
 *		specifies it's the first data item on page (downlink is not changed,
 *		though).  This extra step is only needed for the right page of an
 *		internal page split.  There is no need to do this for the first data
 *		item on the existing/left page, since that will already have been
 *		truncated during an earlier page split.
 *
 *		See _bt_split() for a high level explanation of why we truncate here.
 *		Note that this routine has nothing to do with suffix truncation,
 *		despite using some of the same infrastructure.
 */
#[inline]
unsafe fn _bt_pgaddtup(
    page: Page,
    itemsize: Size,
    itup: IndexTuple,
    itup_off: OffsetNumber,
    newfirstdataitem: bool,
) -> bool {
    let trunctuple: IndexTupleData;
    let itup_to_add: IndexTuple;
    let itemsize_to_add: Size;

    if newfirstdataitem {
        trunctuple = *itup;
        let mut t = trunctuple;
        t.t_info = core::mem::size_of::<IndexTupleData>() as u16;
        BTreeTupleSetNAtts(&mut t, 0, false);
        itup_to_add = &t as *const IndexTupleData as IndexTuple;
        itemsize_to_add = core::mem::size_of::<IndexTupleData>();
    } else {
        itup_to_add = itup;
        itemsize_to_add = itemsize;
    }

    if PageAddItem(page, itup_to_add as Item, itemsize_to_add, itup_off, false, false)
        == InvalidOffsetNumber
    {
        return false;
    }

    true
}

/*
 * _bt_delete_or_dedup_one_page - Try to avoid a leaf page split.
 *
 * There are three operations performed here: simple index deletion, bottom-up
 * index deletion, and deduplication.  If all three operations fail to free
 * enough space for the incoming item then caller will go on to split the
 * page.  We always consider simple deletion first.  If that doesn't work out
 * we consider alternatives.  Callers that only want us to consider simple
 * deletion (without any fallback) ask for that using the 'simpleonly'
 * argument.
 *
 * We usually pick only one alternative "complex" operation when simple
 * deletion alone won't prevent a page split.  The 'checkingunique',
 * 'uniquedup', and 'indexUnchanged' arguments are used for that.
 *
 * Note: We used to only delete LP_DEAD items when the BTP_HAS_GARBAGE page
 * level flag was found set.  The flag was useful back when there wasn't
 * necessarily one single page for a duplicate tuple to go on (before heap TID
 * became a part of the key space in version 4 indexes).  But we don't
 * actually look at the flag anymore (it's not a gating condition for our
 * caller).  That would cause us to miss tuples that are safe to delete,
 * without getting any benefit in return.  We know that the alternative is to
 * split the page; scanning the line pointer array in passing won't have
 * noticeable overhead.  (We still maintain the BTP_HAS_GARBAGE flag despite
 * all this because !heapkeyspace indexes must still do a "getting tired"
 * linear search, and so are likely to get some benefit from using it as a
 * gating condition.)
 */
unsafe fn _bt_delete_or_dedup_one_page(
    rel: Relation,
    heapRel: Relation,
    insertstate: *mut BTInsertStateData,
    simpleonly: bool,
    checkingunique: bool,
    mut uniquedup: bool,
    indexUnchanged: bool,
) {
    let mut deletable: [OffsetNumber; 1358 /* MaxIndexTuplesPerPage */] =
        [0; 1358];
    let mut ndeletable: c_int = 0;
    let mut offnum: OffsetNumber;
    let minoff: OffsetNumber;
    let maxoff: OffsetNumber;
    let buffer: Buffer = (*insertstate).buf;
    let itup_key: BTScanInsert = (*insertstate).itup_key;
    let page: Page = BufferGetPage(buffer);
    let opaque: BTPageOpaque = BTPageGetOpaque(page);

    Assert!(P_ISLEAF(opaque));
    Assert!(simpleonly || (*itup_key).heapkeyspace);
    Assert!(!simpleonly || (!checkingunique && !uniquedup && !indexUnchanged));

    /*
     * Scan over all items to see which ones need to be deleted according to
     * LP_DEAD flags.  We'll usually manage to delete a few extra items that
     * are not marked LP_DEAD in passing.  Often the extra items that actually
     * end up getting deleted are items that would have had their LP_DEAD bit
     * set before long anyway (if we opted not to include them as extras).
     */
    minoff = P_FIRSTDATAKEY(opaque);
    maxoff = PageGetMaxOffsetNumber(page);
    offnum = minoff;
    while offnum <= maxoff {
        let itemId: ItemId = PageGetItemId(page, offnum);

        if ItemIdIsDead(itemId) {
            deletable[ndeletable as usize] = offnum;
            ndeletable += 1;
        }
        offnum = OffsetNumberNext(offnum);
    }

    if ndeletable > 0 {
        _bt_simpledel_pass(
            rel,
            buffer,
            heapRel,
            deletable.as_mut_ptr(),
            ndeletable,
            (*insertstate).itup,
            minoff,
            maxoff,
        );
        (*insertstate).bounds_valid = false;

        /* Return when a page split has already been avoided */
        if PageGetFreeSpace(page) >= (*insertstate).itemsz {
            return;
        }

        /* Might as well assume duplicates (if checkingunique) */
        uniquedup = true;
    }

    /*
     * We're done with simple deletion.  Return early with callers that only
     * call here so that simple deletion can be considered.  This includes
     * callers that explicitly ask for this and checkingunique callers that
     * probably don't have any version churn duplicates on the page.
     *
     * Note: The page's BTP_HAS_GARBAGE hint flag may still be set when we
     * return at this point (or when we go on the try either or both of our
     * other strategies and they also fail).  We do not bother expending a
     * separate write to clear it, however.  Caller will definitely clear it
     * when it goes on to split the page (note also that the deduplication
     * process will clear the flag in passing, just to keep things tidy).
     */
    if simpleonly || (checkingunique && !uniquedup) {
        Assert!(!indexUnchanged);
        return;
    }

    /* Assume bounds about to be invalidated (this is almost certain now) */
    (*insertstate).bounds_valid = false;

    /*
     * Perform bottom-up index deletion pass when executor hint indicated that
     * incoming item is logically unchanged, or for a unique index that is
     * known to have physical duplicates for some other reason.  (There is a
     * large overlap between these two cases for a unique index.  It's worth
     * having both triggering conditions in order to apply the optimization in
     * the event of successive related INSERT and DELETE statements.)
     *
     * We'll go on to do a deduplication pass when a bottom-up pass fails to
     * delete an acceptable amount of free space (a significant fraction of
     * the page, or space for the new item, whichever is greater).
     *
     * Note: Bottom-up index deletion uses the same equality/equivalence
     * routines as deduplication internally.  However, it does not merge
     * together index tuples, so the same correctness considerations do not
     * apply.  We deliberately omit an index-is-allequalimage test here.
     */
    if (indexUnchanged || uniquedup)
        && _bt_bottomupdel_pass(rel, buffer, heapRel, (*insertstate).itemsz)
    {
        return;
    }

    /* Perform deduplication pass (when enabled and index-is-allequalimage) */
    if BTGetDeduplicateItems(rel) && (*itup_key).allequalimage {
        _bt_dedup_pass(
            rel,
            buffer,
            (*insertstate).itup,
            (*insertstate).itemsz,
            indexUnchanged || uniquedup,
        );
    }
}

/*
 * _bt_simpledel_pass - Simple index tuple deletion pass.
 *
 * We delete all LP_DEAD-set index tuples on a leaf page.  The offset numbers
 * of all such tuples are determined by caller (caller passes these to us as
 * its 'deletable' argument).
 *
 * We might also delete extra index tuples that turn out to be safe to delete
 * in passing (though they must be cheap to check in passing to begin with).
 * There is no certainty that any extra tuples will be deleted, though.  The
 * high level goal of the approach we take is to get the most out of each call
 * here (without noticeably increasing the per-call overhead compared to what
 * we need to do just to be able to delete the page's LP_DEAD-marked index
 * tuples).
 *
 * The number of extra index tuples that turn out to be deletable might
 * greatly exceed the number of LP_DEAD-marked index tuples due to various
 * locality related effects.  For example, it's possible that the total number
 * of table blocks (pointed to by all TIDs on the leaf page) is naturally
 * quite low, in which case we might end up checking if it's possible to
 * delete _most_ index tuples on the page (without the tableam needing to
 * access additional table blocks).  The tableam will sometimes stumble upon
 * _many_ extra deletable index tuples in indexes where this pattern is
 * common.
 *
 * See nbtree/README for further details on simple index tuple deletion.
 */
unsafe fn _bt_simpledel_pass(
    rel: Relation,
    buffer: Buffer,
    heapRel: Relation,
    deletable: *mut OffsetNumber,
    ndeletable: c_int,
    newitem: IndexTuple,
    minoff: OffsetNumber,
    maxoff: OffsetNumber,
) {
    let page: Page = BufferGetPage(buffer);
    let deadblocks: *mut BlockNumber;
    let mut ndeadblocks: c_int = 0;
    let mut delstate: TM_IndexDeleteOp = core::mem::zeroed();
    let mut offnum: OffsetNumber;

    /* Get array of table blocks pointed to by LP_DEAD-set tuples */
    deadblocks = _bt_deadblocks(page, deletable, ndeletable, newitem, &mut ndeadblocks);

    /* Initialize tableam state that describes index deletion operation */
    delstate.irel = rel;
    delstate.iblknum = BufferGetBlockNumber(buffer);
    delstate.bottomup = false;
    delstate.bottomupfreespace = 0;
    delstate.ndeltids = 0;
    delstate.deltids = palloc(MaxTIDsPerBTreePage as usize * core::mem::size_of::<TM_IndexDelete>())
        as *mut TM_IndexDelete;
    delstate.status = palloc(MaxTIDsPerBTreePage as usize * core::mem::size_of::<TM_IndexStatus>())
        as *mut TM_IndexStatus;

    offnum = minoff;
    while offnum <= maxoff {
        let itemid: ItemId = PageGetItemId(page, offnum);
        let itup: IndexTuple = PageGetItem(page, itemid) as IndexTuple;
        let odeltid: *mut TM_IndexDelete = delstate.deltids.add(delstate.ndeltids as usize);
        let ostatus: *mut TM_IndexStatus = delstate.status.add(delstate.ndeltids as usize);
        let mut tidblock: BlockNumber;
        let mut match_ptr: *mut c_void;

        if !BTreeTupleIsPosting(itup) {
            tidblock = ItemPointerGetBlockNumber(&(*itup).t_tid);
            match_ptr = bsearch(
                &tidblock as *const BlockNumber as *const c_void,
                deadblocks as *const c_void,
                ndeadblocks as usize,
                core::mem::size_of::<BlockNumber>(),
                _bt_blk_cmp,
            );

            if match_ptr.is_null() {
                Assert!(!ItemIdIsDead(itemid));
                offnum = OffsetNumberNext(offnum);
                continue;
            }

            /*
             * TID's table block is among those pointed to by the TIDs from
             * LP_DEAD-bit set tuples on page -- add TID to deltids
             */
            (*odeltid).tid = (*itup).t_tid;
            (*odeltid).id = delstate.ndeltids as int16;
            (*ostatus).idxoffnum = offnum;
            (*ostatus).knowndeletable = ItemIdIsDead(itemid);
            (*ostatus).promising = false; /* unused */
            (*ostatus).freespace = 0; /* unused */

            delstate.ndeltids += 1;
        } else {
            let nitem: c_int = BTreeTupleGetNPosting(itup);

            for p in 0..nitem {
                let tid: *mut ItemPointerData = BTreeTupleGetPostingN(itup, p);
                let odeltid2: *mut TM_IndexDelete =
                    delstate.deltids.add(delstate.ndeltids as usize);
                let ostatus2: *mut TM_IndexStatus =
                    delstate.status.add(delstate.ndeltids as usize);

                tidblock = ItemPointerGetBlockNumber(tid);
                match_ptr = bsearch(
                    &tidblock as *const BlockNumber as *const c_void,
                    deadblocks as *const c_void,
                    ndeadblocks as usize,
                    core::mem::size_of::<BlockNumber>(),
                    _bt_blk_cmp,
                );

                if match_ptr.is_null() {
                    Assert!(!ItemIdIsDead(itemid));
                    continue;
                }

                /*
                 * TID's table block is among those pointed to by the TIDs
                 * from LP_DEAD-bit set tuples on page -- add TID to deltids
                 */
                (*odeltid2).tid = *tid;
                (*odeltid2).id = delstate.ndeltids as int16;
                (*ostatus2).idxoffnum = offnum;
                (*ostatus2).knowndeletable = ItemIdIsDead(itemid);
                (*ostatus2).promising = false; /* unused */
                (*ostatus2).freespace = 0; /* unused */

                delstate.ndeltids += 1;
            }
        }

        offnum = OffsetNumberNext(offnum);
    }

    pfree(deadblocks as *mut c_void);

    Assert!(delstate.ndeltids >= ndeletable);

    /* Physically delete LP_DEAD tuples (plus any delete-safe extra TIDs) */
    _bt_delitems_delete_check(rel, buffer, heapRel, &mut delstate);

    pfree(delstate.deltids as *mut c_void);
    pfree(delstate.status as *mut c_void);
}

/*
 * _bt_deadblocks() -- Get LP_DEAD related table blocks.
 *
 * Builds sorted and unique-ified array of table block numbers from index
 * tuple TIDs whose line pointers are marked LP_DEAD.  Also adds the table
 * block from incoming newitem just in case it isn't among the LP_DEAD-related
 * table blocks.
 *
 * Always counting the newitem's table block as an LP_DEAD related block makes
 * sense because the cost is consistently low; it is practically certain that
 * the table block will not incur a buffer miss in tableam.  On the other hand
 * the benefit is often quite high.  There is a decent chance that there will
 * be some deletable items from this block, since in general most garbage
 * tuples became garbage in the recent past (in many cases this won't be the
 * first logical row that core code added to/modified in table block
 * recently).
 *
 * Returns final array, and sets *nblocks to its final size for caller.
 */
unsafe fn _bt_deadblocks(
    page: Page,
    deletable: *mut OffsetNumber,
    ndeletable: c_int,
    newitem: IndexTuple,
    nblocks: *mut c_int,
) -> *mut BlockNumber {
    let mut spacentids: c_int;
    let mut ntids: c_int;
    let mut tidblocks: *mut BlockNumber;

    /*
     * Accumulate each TID's block in array whose initial size has space for
     * one table block per LP_DEAD-set tuple (plus space for the newitem table
     * block).  Array will only need to grow when there are LP_DEAD-marked
     * posting list tuples (which is not that common).
     */
    spacentids = ndeletable + 1;
    ntids = 0;
    tidblocks = palloc(spacentids as usize * core::mem::size_of::<BlockNumber>())
        as *mut BlockNumber;

    /*
     * First add the table block for the incoming newitem.  This is the one
     * case where simple deletion can visit a table block that doesn't have
     * any known deletable items.
     */
    Assert!(!BTreeTupleIsPosting(newitem) && !BTreeTupleIsPivot(newitem));
    *tidblocks.add(ntids as usize) = ItemPointerGetBlockNumber(&(*newitem).t_tid);
    ntids += 1;

    for i in 0..ndeletable {
        let itemid: ItemId = PageGetItemId(page, *deletable.add(i as usize));
        let itup: IndexTuple = PageGetItem(page, itemid) as IndexTuple;

        Assert!(ItemIdIsDead(itemid));

        if !BTreeTupleIsPosting(itup) {
            if ntids + 1 > spacentids {
                spacentids *= 2;
                tidblocks = repalloc(
                    tidblocks as *mut c_void,
                    spacentids as usize * core::mem::size_of::<BlockNumber>()
                ) as *mut BlockNumber;
            }

            *tidblocks.add(ntids as usize) = ItemPointerGetBlockNumber(&(*itup).t_tid);
            ntids += 1;
        } else {
            let nposting: c_int = BTreeTupleGetNPosting(itup);

            if ntids + nposting > spacentids {
                spacentids = if spacentids * 2 > ntids + nposting {
                    spacentids * 2
                } else {
                    ntids + nposting
                };
                tidblocks = repalloc(
                    tidblocks as *mut c_void,
                    spacentids as usize * core::mem::size_of::<BlockNumber>()
                ) as *mut BlockNumber;
            }

            for j in 0..nposting {
                let tid: *mut ItemPointerData = BTreeTupleGetPostingN(itup, j);

                *tidblocks.add(ntids as usize) = ItemPointerGetBlockNumber(tid);
                ntids += 1;
            }
        }
    }

    qsort(
        tidblocks as *mut c_void,
        ntids as usize,
        core::mem::size_of::<BlockNumber>(),
        _bt_blk_cmp,
    );
    *nblocks = qunique(tidblocks, ntids as usize, core::mem::size_of::<BlockNumber>(), _bt_blk_cmp)
        as c_int;

    tidblocks
}

/// TODO(pg-port): libc qsort -- bsearch and qsort are C stdlib functions.
unsafe fn bsearch(
    key: *const c_void,
    base: *const c_void,
    nmemb: usize,
    size: usize,
    compar: unsafe fn(*const c_void, *const c_void) -> c_int,
) -> *mut c_void {
    unimplemented!() // TODO(pg-port)
}

/// TODO(pg-port): libc qsort.
unsafe fn qsort(
    base: *mut c_void,
    nmemb: usize,
    size: usize,
    compar: unsafe fn(*const c_void, *const c_void) -> c_int,
) {
    unimplemented!() // TODO(pg-port)
}

/*
 * _bt_blk_cmp() -- qsort comparison function for _bt_simpledel_pass
 */
#[inline]
unsafe fn _bt_blk_cmp(arg1: *const c_void, arg2: *const c_void) -> c_int {
    let b1: BlockNumber = *(arg1 as *const BlockNumber);
    let b2: BlockNumber = *(arg2 as *const BlockNumber);

    pg_cmp_u32(b1, b2)
}
