//! Translated from PostgreSQL src/include/access/heapam_xlog.h

use bitflags::bitflags;

use crate::access::htup::HeapTupleHeaderData;
use crate::access::xlogdefs::XLogRecPtr;
use crate::access::xlogreader::XLogReaderState;
use crate::c::{CommandId, TransactionId};
use crate::lib::stringinfo::StringInfo;
use crate::postgres_ext::Oid;
use crate::storage::block::BlockNumber;
use crate::storage::buf::Buffer;
use crate::storage::itemptr::ItemPointerData;
use crate::storage::off::OffsetNumber;
use crate::storage::relfilelocator::RelFileLocator;
use crate::storage::sinval::SharedInvalidationMessage;
use crate::utils::relcache::Relation;

// WAL opcodes (info high nibble): 3 bits opcode + 1 init bit. Raw consts.
pub const XLOG_HEAP_INSERT: u8 = 0x00;
pub const XLOG_HEAP_DELETE: u8 = 0x10;
pub const XLOG_HEAP_UPDATE: u8 = 0x20;
pub const XLOG_HEAP_TRUNCATE: u8 = 0x30;
pub const XLOG_HEAP_HOT_UPDATE: u8 = 0x40;
pub const XLOG_HEAP_CONFIRM: u8 = 0x50;
pub const XLOG_HEAP_LOCK: u8 = 0x60;
pub const XLOG_HEAP_INPLACE: u8 = 0x70;

pub const XLOG_HEAP_OPMASK: u8 = 0x70;
/// On first item on a new page in INSERT/UPDATE/HOT_UPDATE/MULTI_INSERT, restore
/// the entire page in redo.
pub const XLOG_HEAP_INIT_PAGE: u8 = 0x80;

// RM_HEAP2_ID opcodes (XLOG_HEAP_OPMASK applies). Raw consts.
pub const XLOG_HEAP2_REWRITE: u8 = 0x00;
pub const XLOG_HEAP2_PRUNE_ON_ACCESS: u8 = 0x10;
pub const XLOG_HEAP2_PRUNE_VACUUM_SCAN: u8 = 0x20;
pub const XLOG_HEAP2_PRUNE_VACUUM_CLEANUP: u8 = 0x30;
pub const XLOG_HEAP2_VISIBLE: u8 = 0x40;
pub const XLOG_HEAP2_MULTI_INSERT: u8 = 0x50;
pub const XLOG_HEAP2_LOCK_UPDATED: u8 = 0x60;
pub const XLOG_HEAP2_NEW_CID: u8 = 0x70;

bitflags! {
    /// xl_heap_insert/xl_heap_multi_insert flag values (single-bit set, 8 bits).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct XlhInsert: u8 {
        const ALL_VISIBLE_CLEARED = 1 << 0; // PD_ALL_VISIBLE was cleared
        const LAST_IN_MULTI       = 1 << 1;
        const IS_SPECULATIVE      = 1 << 2;
        const CONTAINS_NEW_TUPLE  = 1 << 3;
        const ON_TOAST_RELATION   = 1 << 4;
        const ALL_FROZEN_SET      = 1 << 5; // implies all_visible_set
    }
}
pub const XLH_INSERT_ALL_VISIBLE_CLEARED: u8 = 1 << 0;
pub const XLH_INSERT_LAST_IN_MULTI: u8 = 1 << 1;
pub const XLH_INSERT_IS_SPECULATIVE: u8 = 1 << 2;
pub const XLH_INSERT_CONTAINS_NEW_TUPLE: u8 = 1 << 3;
pub const XLH_INSERT_ON_TOAST_RELATION: u8 = 1 << 4;
pub const XLH_INSERT_ALL_FROZEN_SET: u8 = 1 << 5;

bitflags! {
    /// xl_heap_update flag values (single-bit set, 8 bits).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct XlhUpdate: u8 {
        const OLD_ALL_VISIBLE_CLEARED = 1 << 0; // PD_ALL_VISIBLE was cleared
        const NEW_ALL_VISIBLE_CLEARED = 1 << 1; // cleared in the 2nd page
        const CONTAINS_OLD_TUPLE      = 1 << 2;
        const CONTAINS_OLD_KEY        = 1 << 3;
        const CONTAINS_NEW_TUPLE      = 1 << 4;
        const PREFIX_FROM_OLD         = 1 << 5;
        const SUFFIX_FROM_OLD         = 1 << 6;
        const CONTAINS_OLD = Self::CONTAINS_OLD_TUPLE.bits() | Self::CONTAINS_OLD_KEY.bits();
    }
}
pub const XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED: u8 = 1 << 0;
pub const XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED: u8 = 1 << 1;
pub const XLH_UPDATE_CONTAINS_OLD_TUPLE: u8 = 1 << 2;
pub const XLH_UPDATE_CONTAINS_OLD_KEY: u8 = 1 << 3;
pub const XLH_UPDATE_CONTAINS_NEW_TUPLE: u8 = 1 << 4;
pub const XLH_UPDATE_PREFIX_FROM_OLD: u8 = 1 << 5;
pub const XLH_UPDATE_SUFFIX_FROM_OLD: u8 = 1 << 6;
pub const XLH_UPDATE_CONTAINS_OLD: u8 =
    XLH_UPDATE_CONTAINS_OLD_TUPLE | XLH_UPDATE_CONTAINS_OLD_KEY;

bitflags! {
    /// xl_heap_delete flag values (single-bit set, 8 bits).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct XlhDelete: u8 {
        const ALL_VISIBLE_CLEARED = 1 << 0; // PD_ALL_VISIBLE was cleared
        const CONTAINS_OLD_TUPLE  = 1 << 1;
        const CONTAINS_OLD_KEY    = 1 << 2;
        const IS_SUPER           = 1 << 3;
        const IS_PARTITION_MOVE  = 1 << 4;
        const CONTAINS_OLD = Self::CONTAINS_OLD_TUPLE.bits() | Self::CONTAINS_OLD_KEY.bits();
    }
}
pub const XLH_DELETE_ALL_VISIBLE_CLEARED: u8 = 1 << 0;
pub const XLH_DELETE_CONTAINS_OLD_TUPLE: u8 = 1 << 1;
pub const XLH_DELETE_CONTAINS_OLD_KEY: u8 = 1 << 2;
pub const XLH_DELETE_IS_SUPER: u8 = 1 << 3;
pub const XLH_DELETE_IS_PARTITION_MOVE: u8 = 1 << 4;
pub const XLH_DELETE_CONTAINS_OLD: u8 =
    XLH_DELETE_CONTAINS_OLD_TUPLE | XLH_DELETE_CONTAINS_OLD_KEY;

/// What we need to know about delete.
#[repr(C)]
pub struct xl_heap_delete {
    pub xmax: TransactionId,   // xmax of the deleted tuple
    pub offnum: OffsetNumber,  // deleted tuple's offset
    pub infobits_set: u8,      // infomask bits
    pub flags: u8,
}
pub const SizeOfHeapDelete: usize =
    core::mem::offset_of!(xl_heap_delete, flags) + core::mem::size_of::<u8>();

bitflags! {
    /// xl_heap_truncate flag values (single-bit set, 8 bits).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct XlhTruncate: u8 {
        const CASCADE       = 1 << 0;
        const RESTART_SEQS  = 1 << 1;
    }
}
pub const XLH_TRUNCATE_CASCADE: u8 = 1 << 0;
pub const XLH_TRUNCATE_RESTART_SEQS: u8 = 1 << 1;

/// All truncated relids, then sequence relids to restart. FAM `relids: [Oid]`.
#[repr(C)]
pub struct xl_heap_truncate {
    pub dbId: Oid,
    pub nrelids: u32,
    pub flags: u8,
    // FAM: relids: [Oid]
}
pub const SizeOfHeapTruncate: usize = core::mem::offset_of!(xl_heap_truncate, flags) + 1;

/// Reduced fixed-part header for an inserted/updated tuple stored in WAL.
#[repr(C)]
pub struct xl_heap_header {
    pub t_infomask2: u16,
    pub t_infomask: u16,
    pub t_hoff: u8,
}
pub const SizeOfHeapHeader: usize =
    core::mem::offset_of!(xl_heap_header, t_hoff) + core::mem::size_of::<u8>();

/// What we need to know about insert. xl_heap_header + TUPLE DATA in backup blk 0.
#[repr(C)]
pub struct xl_heap_insert {
    pub offnum: OffsetNumber, // inserted tuple's offset
    pub flags: u8,
}
pub const SizeOfHeapInsert: usize =
    core::mem::offset_of!(xl_heap_insert, flags) + core::mem::size_of::<u8>();

/// Multi-insert header. FAM `offsets` omitted if XLOG_HEAP_INIT_PAGE.
#[repr(C)]
pub struct xl_heap_multi_insert {
    pub flags: u8,
    pub ntuples: u16,
    // FAM: offsets: [OffsetNumber]
}
pub const SizeOfHeapMultiInsert: usize = core::mem::offset_of!(xl_heap_multi_insert, ntuples) + 2;

#[repr(C)]
pub struct xl_multi_insert_tuple {
    pub datalen: u16, // size of tuple data that follows
    pub t_infomask2: u16,
    pub t_infomask: u16,
    pub t_hoff: u8,
    // TUPLE DATA FOLLOWS AT END OF STRUCT
}
pub const SizeOfMultiInsertTuple: usize =
    core::mem::offset_of!(xl_multi_insert_tuple, t_hoff) + core::mem::size_of::<u8>();

/// What we need to know about update|hot_update. Backup blk 0: new page.
#[repr(C)]
pub struct xl_heap_update {
    pub old_xmax: TransactionId,  // xmax of the old tuple
    pub old_offnum: OffsetNumber, // old tuple's offset
    pub old_infobits_set: u8,     // infomask bits to set on old tuple
    pub flags: u8,
    pub new_xmax: TransactionId,  // xmax of the new tuple
    pub new_offnum: OffsetNumber, // new tuple's offset
}
pub const SizeOfHeapUpdate: usize =
    core::mem::offset_of!(xl_heap_update, new_offnum) + core::mem::size_of::<OffsetNumber>();

/// Main prune/freeze/on-access-prune record. snapshot_conflict_horizon follows
/// unaligned. See xlhp_* sub-records for block-0 data.
#[repr(C)]
pub struct xl_heap_prune {
    pub reason: u8,
    pub flags: u8,
    // If XLHP_HAS_CONFLICT_HORIZON: conflict horizon XID follows, unaligned.
}
pub const SizeOfHeapPrune: usize =
    core::mem::offset_of!(xl_heap_prune, flags) + core::mem::size_of::<u8>();

bitflags! {
    /// XLHP_* flags for xl_heap_prune (single-bit set). Bit 0 unused.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct Xlhp: u8 {
        const IS_CATALOG_REL      = 1 << 1; // recovery conflict during logical decoding
        const CLEANUP_LOCK        = 1 << 2; // replay requires a cleanup lock
        const HAS_CONFLICT_HORIZON = 1 << 3;
        const HAS_FREEZE_PLANS    = 1 << 4;
        const HAS_REDIRECTIONS    = 1 << 5;
        const HAS_DEAD_ITEMS      = 1 << 6;
        const HAS_NOW_UNUSED_ITEMS = 1 << 7;
    }
}
pub const XLHP_IS_CATALOG_REL: u8 = 1 << 1;
pub const XLHP_CLEANUP_LOCK: u8 = 1 << 2;
pub const XLHP_HAS_CONFLICT_HORIZON: u8 = 1 << 3;
pub const XLHP_HAS_FREEZE_PLANS: u8 = 1 << 4;
pub const XLHP_HAS_REDIRECTIONS: u8 = 1 << 5;
pub const XLHP_HAS_DEAD_ITEMS: u8 = 1 << 6;
pub const XLHP_HAS_NOW_UNUSED_ITEMS: u8 = 1 << 7;

bitflags! {
    /// xlhp_freeze_plan frzflags (single-bit set). 0x01 was retired XLH_FREEZE_XMIN.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct XlhFreeze: u8 {
        const XVAC         = 0x02;
        const INVALID_XVAC = 0x04;
    }
}
pub const XLH_FREEZE_XVAC: u8 = 0x02;
pub const XLH_INVALID_XVAC: u8 = 0x04;

/// How to freeze a group of heap tuples (in xl_heap_prune's freeze_plans).
#[repr(C)]
pub struct xlhp_freeze_plan {
    pub xmax: TransactionId,
    pub t_infomask2: u16,
    pub t_infomask: u16,
    pub frzflags: u8,
    pub ntuples: u16, // length of this plan's page offset numbers array
}

/// FAM `plans: [xlhp_freeze_plan]`; per-plan offsets live at end of whole record.
#[repr(C)]
pub struct xlhp_freeze_plans {
    pub nplans: u16,
    // FAM: plans: [xlhp_freeze_plan]
}

/// Generic sub-record for redirect/dead/unused items. For redirections, `data`
/// holds 2*ntargets OffsetNumbers.
#[repr(C)]
pub struct xlhp_prune_items {
    pub ntargets: u16,
    // FAM: data: [OffsetNumber]
}

bitflags! {
    /// flags for infobits_set (single-bit set).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct Xlhl: u8 {
        const XMAX_IS_MULTI     = 0x01;
        const XMAX_LOCK_ONLY    = 0x02;
        const XMAX_EXCL_LOCK    = 0x04;
        const XMAX_KEYSHR_LOCK  = 0x08;
        const KEYS_UPDATED      = 0x10;
    }
}
pub const XLHL_XMAX_IS_MULTI: u8 = 0x01;
pub const XLHL_XMAX_LOCK_ONLY: u8 = 0x02;
pub const XLHL_XMAX_EXCL_LOCK: u8 = 0x04;
pub const XLHL_XMAX_KEYSHR_LOCK: u8 = 0x08;
pub const XLHL_KEYS_UPDATED: u8 = 0x10;

bitflags! {
    /// flag bits for xl_heap_lock / xl_heap_lock_updated's flag field.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct XlhLock: u8 {
        const ALL_FROZEN_CLEARED = 0x01;
    }
}
pub const XLH_LOCK_ALL_FROZEN_CLEARED: u8 = 0x01;

/// What we need to know about lock.
#[repr(C)]
pub struct xl_heap_lock {
    pub xmax: TransactionId,  // might be a MultiXactId
    pub offnum: OffsetNumber, // locked tuple's offset on page
    pub infobits_set: u8,     // infomask and infomask2 bits to set
    pub flags: u8,            // XLH_LOCK_* flag bits
}
pub const SizeOfHeapLock: usize =
    core::mem::offset_of!(xl_heap_lock, flags) + core::mem::size_of::<u8>();

/// What we need to know about locking an updated version of a row.
#[repr(C)]
pub struct xl_heap_lock_updated {
    pub xmax: TransactionId,
    pub offnum: OffsetNumber,
    pub infobits_set: u8,
    pub flags: u8,
}
pub const SizeOfHeapLockUpdated: usize =
    core::mem::offset_of!(xl_heap_lock_updated, flags) + core::mem::size_of::<u8>();

/// Confirmation of speculative insertion.
#[repr(C)]
pub struct xl_heap_confirm {
    pub offnum: OffsetNumber, // confirmed tuple's offset on page
}
pub const SizeOfHeapConfirm: usize =
    core::mem::offset_of!(xl_heap_confirm, offnum) + core::mem::size_of::<OffsetNumber>();

/// In-place update. FAM `msgs: [SharedInvalidationMessage]`.
#[repr(C)]
pub struct xl_heap_inplace {
    pub offnum: OffsetNumber,       // updated tuple's offset on page
    pub dbId: Oid,                  // MyDatabaseId
    pub tsId: Oid,                  // MyDatabaseTableSpace
    pub relcacheInitFileInval: bool, // invalidate relcache init files
    pub nmsgs: i32,                 // number of shared inval msgs
    // FAM: msgs: [SharedInvalidationMessage]
}
pub const MinSizeOfHeapInplace: usize =
    core::mem::offset_of!(xl_heap_inplace, nmsgs) + core::mem::size_of::<i32>();

/// Setting a visibility map bit. Backup blk 0: VM buffer; blk 1: heap buffer.
#[repr(C)]
pub struct xl_heap_visible {
    pub snapshotConflictHorizon: TransactionId,
    pub flags: u8,
}
pub const SizeOfHeapVisible: usize =
    core::mem::offset_of!(xl_heap_visible, flags) + core::mem::size_of::<u8>();

#[repr(C)]
pub struct xl_heap_new_cid {
    /// toplevel xid so we don't have to merge cids from different transactions
    pub top_xid: TransactionId,
    pub cmin: CommandId,
    pub cmax: CommandId,
    pub combocid: CommandId, // just for debugging
    /// relfilelocator/ctid pair to facilitate lookups
    pub target_locator: RelFileLocator,
    pub target_tid: ItemPointerData,
}
pub const SizeOfHeapNewCid: usize =
    core::mem::offset_of!(xl_heap_new_cid, target_tid) + core::mem::size_of::<ItemPointerData>();

/// Logical rewrite xlog record header.
#[repr(C)]
pub struct xl_heap_rewrite_mapping {
    pub mapped_xid: TransactionId, // xid that might need to see the row
    pub mapped_db: Oid,            // DbOid or InvalidOid for shared rels
    pub mapped_rel: Oid,           // Oid of the mapped relation
    pub offset: i64,               // off_t: how far we've written so far
    pub num_mappings: u32,         // number of in-memory mappings
    pub start_lsn: XLogRecPtr,     // insert LSN at begin of rewrite
}

/// Advances the conflict horizon (out-param) per the tuple's xids.
pub fn HeapTupleHeaderAdvanceConflictHorizon(
    _tuple: &HeapTupleHeaderData,
    _snapshotConflictHorizon: &mut TransactionId,
) {
    unimplemented!()
}

pub fn heap_redo(_record: &mut XLogReaderState) {
    unimplemented!()
}
pub fn heap_desc(_buf: &mut StringInfo, _record: &mut XLogReaderState) {
    unimplemented!()
}
pub fn heap_identify(_info: u8) -> Option<&'static str> {
    unimplemented!()
}
pub fn heap_mask(_pagedata: &mut [u8], _blkno: BlockNumber) {
    unimplemented!()
}
pub fn heap2_redo(_record: &mut XLogReaderState) {
    unimplemented!()
}
pub fn heap2_desc(_buf: &mut StringInfo, _record: &mut XLogReaderState) {
    unimplemented!()
}
pub fn heap2_identify(_info: u8) -> Option<&'static str> {
    unimplemented!()
}
pub fn heap_xlog_logical_rewrite(_r: &mut XLogReaderState) {
    unimplemented!()
}

pub fn log_heap_visible(
    _rel: Relation,
    _heap_buffer: Buffer,
    _vm_buffer: Buffer,
    _snapshotConflictHorizon: TransactionId,
    _vmflags: u8,
) -> XLogRecPtr {
    unimplemented!()
}

/// Deserializes prune-and-freeze sub-records. In heapdesc.c (shared FE/BE).
/// Returns the parsed plans/offsets arrays (C out-params folded into a struct).
pub fn heap_xlog_deserialize_prune_and_freeze(
    _cursor: &[u8],
    _flags: u8,
) -> PruneFreezeResult {
    unimplemented!()
}

/// Parsed result of heap_xlog_deserialize_prune_and_freeze (C out-params).
pub struct PruneFreezeResult {
    pub plans: Vec<xlhp_freeze_plan>,
    pub frz_offsets: Vec<OffsetNumber>,
    pub redirected: Vec<OffsetNumber>,
    pub nowdead: Vec<OffsetNumber>,
    pub nowunused: Vec<OffsetNumber>,
}
